#!/usr/bin/env python3
"""
Download ICOS Ecosystem L0 Eddy-flux raw data (all sites) from the Carbon Portal.

Requirements:
    pip install requests

Usage (recommended: put secrets in env vars):
    export ICOS_EMAIL="you@example.org"
    export ICOS_PASSWORD="your_password"
    # or, if you already have a fresh cpauthToken:
    # export ICOS_CPAUTH_TOKEN="....."

    python download_icos_l0_flux.py
"""

import builtins
import io
import os
import pathlib
import shutil
import time
import zipfile
from typing import List, Dict

import requests

# Safe print that ignores OS errors (e.g., broken pipe / invalid fd on long runs)
def safe_print(*args, **kwargs) -> None:
    try:
        builtins.print(*args, **kwargs)
    except OSError:
        pass

# Use safe_print throughout this module
print = safe_print

META_SPARQL_URL = "https://meta.icos-cp.eu/sparql"
DATA_OBJECT_URL = "https://data.icos-cp.eu/objects"
LOGIN_URL = "https://cpauth.icos-cp.eu/password/login"

# ----------------------------------------------------------------------
# User-configurable settings
# ----------------------------------------------------------------------

# Where to put the downloaded ZIPs
OUTPUT_DIR = pathlib.Path(r"D:/data/ec/raw/ICOS")
# Track processed object hashes to avoid re-downloading after extraction
DOWNLOAD_LOG = OUTPUT_DIR / ".downloaded_hashes.txt"

# Time window (based on acquisition start time)
# Use full network by setting START_DATE = None, END_DATE = None
START_DATE = "2023-01-01T00:00:00Z"   # inclusive, or None
END_DATE   = "2023-03-01T00:00:00Z"   # inclusive, or None

# Max number of stations per ecosystem type (IGBP class)
MAX_SITES_PER_ECOSYSTEM = 2

# Optional filter: only these ecosystem codes (local name from URI, e.g. "igbp_ENF", "igbp_GRA").
# Set to None to allow all ecosystem types.
ALLOWED_ECOSYSTEM_TYPES: list[str] | None = None
# Example:
# ALLOWED_ECOSYSTEM_TYPES = ["igbp_ENF", "igbp_GRA"]

# Which data object specifications to include
# ASCII raw daily archive + binary raw daily archive
EDDY_SPECS = [
    "http://meta.icos-cp.eu/resources/cpmeta/etcEddyFluxRawSeriesCsv",
    "http://meta.icos-cp.eu/resources/cpmeta/etcEddyFluxRawSeriesBin",
]

# If True, only ASCII; if True only binary. If both False -> both.
ONLY_ASCII = True
ONLY_BINARY = False

# Throttle between downloads (seconds) to be gentle to the service
DOWNLOAD_SLEEP = 0.5
# Throttle between SPARQL station queries (seconds) to avoid hammering the endpoint
SPARQL_SLEEP = 1.0

# ----------------------------------------------------------------------
# Authentication helpers
# ----------------------------------------------------------------------

def get_cpauth_token(session: requests.Session) -> str:
    """
    Get a cpauthToken either from env var ICOS_CPAUTH_TOKEN or
    by logging in with ICOS_EMAIL / ICOS_PASSWORD.
    """
    token = os.environ.get("ICOS_CPAUTH_TOKEN")
    if token:
        print("Using cpauthToken from ICOS_CPAUTH_TOKEN.")
        return token

    email = os.environ.get("ICOS_EMAIL")
    password = os.environ.get("ICOS_PASSWORD")
    if not email or not password:
        raise RuntimeError(
            "Set either ICOS_CPAUTH_TOKEN or ICOS_EMAIL + ICOS_PASSWORD "
            "as environment variables."
        )

    print("Logging in to CPauth to obtain cpauthToken...")
    resp = session.post(
        LOGIN_URL,
        data={"mail": email, "password": password},
        allow_redirects=False,  # token usually set on first response
    )
    resp.raise_for_status()

    # cpauthToken is set as a cookie
    token = resp.cookies.get("cpauthToken")
    if not token:
        # In some deployments it may be returned via Set-Cookie on redirect.
        # Fallback: look also in session cookies.
        token = session.cookies.get("cpauthToken")

    if not token:
        raise RuntimeError(
            "Login succeeded but no 'cpauthToken' cookie was found. "
            "Check credentials or inspect response headers."
        )

    print("Obtained cpauthToken.")
    return token

# ----------------------------------------------------------------------
# Metadata query (SPARQL)
# ----------------------------------------------------------------------
def post_sparql_with_retries(
    query: str,
    session: requests.Session | None = None,
    timeout: int = 120,
    retries: int = 5,
    backoff: float = 2.0,
) -> requests.Response:
    """
    Execute a SPARQL POST with simple retries for transient errors (5xx/429).
    """
    sess = session or requests.Session()
    retryable_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, retries + 1):
        try:
            resp = sess.post(
                META_SPARQL_URL,
                data={"query": query},
                headers={"Accept": "application/sparql-results+json"},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt >= retries:
                raise
            wait = backoff ** (attempt - 1)
            print(
                f"SPARQL request failed (attempt {attempt}/{retries}): "
                f"{exc}. Retrying in {wait}s..."
            )
            time.sleep(wait)
            continue

        if 200 <= resp.status_code < 300:
            return resp

        if resp.status_code in retryable_statuses and attempt < retries:
            wait = backoff ** (attempt - 1)
            print(
                f"SPARQL server returned {resp.status_code} "
                f"({resp.reason}); retrying in {wait}s..."
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()

    return resp  # pragma: no cover

def build_sparql_query(
    start_date: str | None,
    end_date: str | None,
    specs: List[str],
    station_ids: List[str] | None = None,
) -> str:
    """
    Build SPARQL query that returns all data objects that:
      * have one of the given object specifications (Eddy flux raw ASCII/binary)
      * (optionally) have acquisition start time inside [start_date, end_date]
      * (optionally) belong to one of the given station IDs
      * are linked to an ecosystem station with an ecosystem type (IGBP class)
    """
    specs_values = " ".join(f"<{s}>" for s in specs)

    date_filter = ""
    if start_date and end_date:
        date_filter = (
            f'FILTER (!BOUND(?start) || '
            f'(?start >= "{start_date}"^^xsd:dateTime && '
            f'?start <= "{end_date}"^^xsd:dateTime))'
        )
    elif start_date:
        date_filter = (
            f'FILTER (!BOUND(?start) || '
            f'?start >= "{start_date}"^^xsd:dateTime)'
        )
    elif end_date:
        date_filter = (
            f'FILTER (!BOUND(?start) || '
            f'?start <= "{end_date}"^^xsd:dateTime)'
        )

    station_filter = ""
    if station_ids:
        station_values = " ".join(f'"{sid}"' for sid in station_ids)
        station_filter = f"VALUES ?stationId {{ {station_values} }}"

    return f"""
PREFIX cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
PREFIX prov:   <http://www.w3.org/ns/prov#>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

SELECT ?dobj ?spec ?fileName ?stationId ?stationName ?eco ?start ?end
WHERE {{
  ?dobj cpmeta:hasObjectSpec ?spec .
  VALUES ?spec {{ {specs_values} }}

  OPTIONAL {{ ?dobj cpmeta:hasName ?fileName . }}

  OPTIONAL {{
    {{
      ?dobj cpmeta:wasAcquiredBy ?acq .
      ?acq prov:wasAssociatedWith ?station .
    }} UNION {{
      ?dobj cpmeta:wasAcquiredAtStation ?station .
    }}
    ?station cpmeta:hasStationId ?stationId .
    {station_filter}
    OPTIONAL {{ ?station rdfs:label ?stationName . }}
    OPTIONAL {{ ?station cpmeta:hasEcosystemType ?eco . }}
  }}

  OPTIONAL {{
    ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy/prov:startedAtTime) ?start .
    ?dobj cpmeta:hasEndTime   | (cpmeta:wasAcquiredBy/prov:endedAtTime)   ?end .
  }}

  {date_filter}
}}
ORDER BY ?stationId ?start
""".strip()


def query_eddy_flux_objects(
    start_date: str | None,
    end_date: str | None,
    specs: List[str],
    station_ids: List[str] | None = None,
    session: requests.Session | None = None,
) -> List[Dict[str, str]]:
    """
    Execute SPARQL query and return list of dicts with metadata
    for each data object. Optionally restrict results to specific station IDs.
    """
    query = build_sparql_query(start_date, end_date, specs, station_ids)

    resp = post_sparql_with_retries(
        query,
        session=session,
    )
    data = resp.json()

    rows: List[Dict[str, str]] = []
    for b in data["results"]["bindings"]:
        row = {}
        for key, val in b.items():
            row[key] = val.get("value")
        rows.append(row)

    print(f"SPARQL returned {len(rows)} data objects.")
    return rows

def query_all_ecosystem_stations(session: requests.Session | None = None) -> dict[str, str]:
    """
    Returns mapping: stationId -> ecosystem_type (igbp code).
    Works reliably for ICOS Ecosystem sites.
    """
    query = """
PREFIX cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?stationId ?eco
WHERE {
  ?s cpmeta:hasStationId ?stationId .
  ?s dcterms:isPartOf <http://meta.icos-cp.eu/resources/dcat/ICOS> .
  ?s cpmeta:hasEcosystemType ?eco .
}
"""

    resp = post_sparql_with_retries(query, session=session)
    data = resp.json()

    out = {}
    for b in data["results"]["bindings"]:
        sid = b["stationId"]["value"]
        eco_uri = b.get("eco", {}).get("value")
        eco = eco_uri.rsplit("/", 1)[-1] if eco_uri else "UNKNOWN"
        out[sid] = eco

    print(f"Found {len(out)} ecosystem stations with ecosystem types.")
    return out

# ----------------------------------------------------------------------
# Download helpers
# ----------------------------------------------------------------------

def hash_id_from_dobj_uri(dobj_uri: str) -> str:
    """
    Extract the hash-based object ID from a landing-page URL like
    https://meta.icos-cp.eu/objects/H8eBVr_5lFqu4C94xiRyN4OE
    """
    return dobj_uri.rstrip("/").rsplit("/", 1)[-1]


def choose_filename(row: Dict[str, str]) -> str:
    """
    Suggest a local filename from metadata.
    Falls back to the hash id if nothing else is available.
    """
    hash_id = hash_id_from_dobj_uri(row["dobj"])
    # Try station + date + original name
    station = row.get("stationId", "NA")
    start = row.get("start", "NA").replace(":", "").replace("-", "")
    orig = row.get("fileName")
    if orig:
        return f"{station}_{start}_{orig}"
    return f"{station}_{start}_{hash_id}.zip"


def sanitize_path_component(value: str) -> str:
    """
    Make a string safe for use as a directory name.
    """
    safe = value.strip().replace("\\", "_").replace("/", "_")
    return safe or "NA"

def load_downloaded_hashes(log_path: pathlib.Path) -> set[str]:
    """
    Read a newline-delimited list of processed object hash IDs.
    """
    if not log_path.exists():
        return set()
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except OSError:
        return set()

def record_downloaded_hash(log_path: pathlib.Path, hash_id: str) -> None:
    """
    Append a processed object hash ID to the log.
    """
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(hash_id + "\n")
    except OSError:
        pass

def discover_existing_station_ids(out_dir: pathlib.Path) -> list[str]:
    """
    Infer station IDs from an existing OUTPUT_DIR layout (ecosystem/type/station).
    """
    if not out_dir.exists():
        return []

    stations: set[str] = set()
    for eco_dir in out_dir.iterdir():
        if not eco_dir.is_dir():
            continue
        for station_dir in eco_dir.iterdir():
            if station_dir.is_dir():
                stations.add(station_dir.name)

    return sorted(stations)

def discover_station_to_eco(out_dir: pathlib.Path) -> dict[str, str]:
    """
    Infer station -> ecosystem key mapping from OUTPUT_DIR subdirectories.
    """
    mapping: dict[str, str] = {}
    if not out_dir.exists():
        return mapping

    for eco_dir in out_dir.iterdir():
        if not eco_dir.is_dir():
            continue
        eco_key = eco_dir.name
        for station_dir in eco_dir.iterdir():
            if station_dir.is_dir():
                mapping.setdefault(station_dir.name, eco_key)

    return mapping

def extract_nested_zip(
    zip_path: pathlib.Path,
    dest_dir: pathlib.Path,
) -> list[pathlib.Path]:
    """
    Extract a ZIP archive into `dest_dir`, recursively unpacking nested ZIPs and
    flattening any folder structure so CSVs end up directly in the site folder.
    Existing files are left untouched.
    """
    if not zipfile.is_zipfile(zip_path):
        raise RuntimeError(f"Not a ZIP file: {zip_path}")

    extracted: list[pathlib.Path] = []
    dest_dir.mkdir(parents=True, exist_ok=True)

    def _extract_from_zip(zf: zipfile.ZipFile) -> None:
        for info in zf.infolist():
            if info.is_dir():
                continue

            name_only = pathlib.Path(info.filename).name
            with zf.open(info) as src:
                if info.filename.lower().endswith(".zip"):
                    nested_bytes = src.read()
                    with zipfile.ZipFile(io.BytesIO(nested_bytes)) as nested:
                        _extract_from_zip(nested)
                    continue

                target_path = dest_dir / name_only
                if target_path.exists():
                    print(f"Already present, skipping: {target_path.name}")
                    continue

                with open(target_path, "wb") as out_file:
                    shutil.copyfileobj(src, out_file)
                extracted.append(target_path)

    with zipfile.ZipFile(zip_path) as zf:
        _extract_from_zip(zf)

    return extracted


def extract_all_downloaded_zips(out_dir: pathlib.Path) -> None:
    """
    Walk the OUTPUT_DIR and extract all ZIP files in place (flattened), then remove them.
    """
    if not out_dir.exists():
        print(f"Output directory does not exist: {out_dir}")
        return

    zips = sorted(out_dir.rglob("*.zip"))
    total = len(zips)
    if not zips:
        print("No ZIP files to extract.")
        return

    print(f"Extracting {total} ZIP file(s)...")
    for idx, zip_path in enumerate(zips, 1):
        dest_dir = zip_path.parent
        try:
            extracted = extract_nested_zip(zip_path, dest_dir)
            print(f"[{idx}/{total}] Extracted {len(extracted)} file(s): {zip_path.name}")
        except Exception as exc:
            print(f"[{idx}/{total}] Failed to extract {zip_path.name}: {exc}")
            continue

        try:
            zip_path.unlink()
            print(f"[{idx}/{total}] Removed ZIP: {zip_path.name}")
        except Exception as exc:
            print(f"[{idx}/{total}] Could not remove ZIP {zip_path.name}: {exc}")


def download_object(
    session: requests.Session,
    token: str,
    dobj_row: Dict[str, str],
    out_dir: pathlib.Path,
    downloaded_hashes: set[str],
    download_log: pathlib.Path,
) -> None:
    """
    Download one data object as a ZIP, if not already present.
    """
    hash_id = hash_id_from_dobj_uri(dobj_row["dobj"])
    filename = choose_filename(dobj_row)
    eco = ecosystem_key(dobj_row.get("eco"))
    station = dobj_row.get("stationId", "NA")
    dest_dir = out_dir / sanitize_path_component(eco) / sanitize_path_component(station)
    zip_path = dest_dir / filename
    if hash_id in downloaded_hashes:
        print(f"Already processed hash {hash_id}, skipping.")
        return

    url = f"{DATA_OBJECT_URL}/{hash_id}"
    if zip_path.exists():
        if zipfile.is_zipfile(zip_path):
            print(f"Using existing ZIP: {zip_path.name}")
        else:
            print(f"Existing ZIP is corrupt, removing: {zip_path.name}")
            try:
                zip_path.unlink()
            except OSError as exc:
                raise RuntimeError(
                    f"Cannot remove corrupt ZIP {zip_path}: {exc}"
                ) from exc

    if not zip_path.exists():
        print(f"Downloading {hash_id} -> {zip_path.name}")

        with session.get(
            url,
            headers={"Cookie": f"cpauthToken={token}"},
            stream=True,
            timeout=300,
        ) as r:
            if r.status_code == 403:
                raise RuntimeError(
                    "HTTP 403 while downloading. "
                    "Make sure your user has accepted the ICOS data licence "
                    "and is allowed to access L0 data."
                )
            r.raise_for_status()
            dest_dir.mkdir(parents=True, exist_ok=True)
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    print(f"Downloaded ZIP: {zip_path.name}")
    downloaded_hashes.add(hash_id)
    record_downloaded_hash(download_log, hash_id)

    time.sleep(DOWNLOAD_SLEEP)

def ecosystem_key(eco_uri: str | None) -> str:
    """
    Turn an ecosystem URI into a compact key, e.g.
    'http://meta.icos-cp.eu/ontologies/cpmeta/igbp_ENF' -> 'igbp_ENF'.
    """
    if not eco_uri:
        return "UNKNOWN"
    return eco_uri.rstrip("/").rsplit("/", 1)[-1]


def select_station_ids_by_ecosystem(
    rows: List[Dict[str, str]],
    max_sites_per_ecosystem: int,
    allowed_ecosystems: List[str] | None = None,
) -> set[str]:
    """
    From all rows, pick up to `max_sites_per_ecosystem` station IDs per ecosystem type.
    Returns the set of selected stationIds.
    """
    eco_to_sites: dict[str, list[str]] = {}
    station_labels: dict[str, str] = {}

    for r in rows:
        sid = r.get("stationId")
        if not sid:
            continue
        eco_uri = r.get("eco")
        eco = ecosystem_key(eco_uri)
        label = r.get("stationName") or sid
        if sid not in station_labels:
            station_labels[sid] = label

        if allowed_ecosystems is not None and eco not in allowed_ecosystems:
            continue

        sites = eco_to_sites.setdefault(eco, [])
        if sid not in sites:
            sites.append(sid)

    selected: set[str] = set()
    print("Ecosystem selection:")
    for eco, sites in sorted(eco_to_sites.items()):
        chosen = sites[:max_sites_per_ecosystem]
        selected.update(chosen)
        site_labels = []
        for sid in chosen:
            label = station_labels.get(sid, sid)
            site_labels.append(f"{sid} ({label})" if label != sid else sid)
        print(f"  Ecosystem Type: {eco}; Sites: {', '.join(site_labels)}")

    print(f"Total selected stations: {len(selected)}")
    return selected


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> None:
    session = requests.Session()

    downloaded_hashes = load_downloaded_hashes(DOWNLOAD_LOG)

    # If user wants only ASCII or only binary, adjust the spec list
    specs = list(EDDY_SPECS)
    if ONLY_ASCII and not ONLY_BINARY:
        specs = [EDDY_SPECS[0]]
    elif ONLY_BINARY and not ONLY_ASCII:
        specs = [EDDY_SPECS[1]]

    target_station_ids = discover_existing_station_ids(OUTPUT_DIR)
    if target_station_ids:
        print(
            "Restricting download to existing station folders: "
            + ", ".join(target_station_ids)
        )

    station_to_eco = discover_station_to_eco(OUTPUT_DIR) if target_station_ids else {}
    objs: List[Dict[str, str]] = []
    if target_station_ids:
        for sid in target_station_ids:
            try:
                station_rows = query_eddy_flux_objects(
                    START_DATE,
                    END_DATE,
                    specs,
                    station_ids=[sid],
                    session=session,
                )
                objs.extend(station_rows)
            except Exception as exc:
                print(f"Skipping station {sid} due to query error: {exc}")
            time.sleep(SPARQL_SLEEP)
    else:
        objs = query_eddy_flux_objects(
            START_DATE,
            END_DATE,
            specs,
            station_ids=None,
            session=session,
        )
    if not objs:
        print("No data objects found for the given filters.")
        return

    # Fetch ecosystem mapping if we are missing any station entries
    need_remote_eco = (not target_station_ids) or any(
        sid not in station_to_eco for sid in (target_station_ids or [])
    )
    if need_remote_eco:
        try:
            remote_mapping = query_all_ecosystem_stations(session=session)
            station_to_eco.update(remote_mapping)
        except Exception as exc:
            print(
                "Warning: could not refresh ecosystem types from SPARQL; "
                f"falling back to local folder names or metadata ({exc})"
            )

    # Annotate each L0 object row with an ecosystem type
    for r in objs:
        sid = r.get("stationId")
        eco_from_query = r.get("eco")
        if sid and sid in station_to_eco:
            r["eco"] = station_to_eco[sid]
        elif eco_from_query:
            r["eco"] = eco_from_query
            if sid and sid not in station_to_eco:
                station_to_eco[sid] = eco_from_query
        else:
            r["eco"] = "UNKNOWN"

    # NEW: select station IDs by ecosystem type
    if target_station_ids:
        selected_station_ids = {
            r.get("stationId")
            for r in objs
            if r.get("stationId") in set(target_station_ids)
        }
        if ALLOWED_ECOSYSTEM_TYPES is not None:
            selected_station_ids = {
                sid for sid in selected_station_ids
                if ecosystem_key(station_to_eco.get(sid)) in ALLOWED_ECOSYSTEM_TYPES
            }
        missing = set(target_station_ids) - selected_station_ids
        if missing:
            print(f"No data found for stations: {', '.join(sorted(missing))}")
        print(f"Using {len(selected_station_ids)} station(s) already present on disk.")
    else:
        selected_station_ids = select_station_ids_by_ecosystem(
            objs,
            max_sites_per_ecosystem=MAX_SITES_PER_ECOSYSTEM,
            allowed_ecosystems=ALLOWED_ECOSYSTEM_TYPES,
        )

    if not selected_station_ids:
        print("No stations selected for download after filtering.")
        return

    # Filter objects to only those from the selected stations
    filtered_objs = [
        r for r in objs
        if r.get("stationId") in selected_station_ids
    ]

    total_downloads = len(filtered_objs)
    print(f"Downloading {total_downloads} data objects "
          f"from {len(selected_station_ids)} stations.")

    token = get_cpauth_token(session)

    for idx, row in enumerate(filtered_objs, 1):
        station = row.get("stationId", "NA")
        eco = ecosystem_key(row.get("eco"))
        obj_label = row.get("fileName") or hash_id_from_dobj_uri(row["dobj"])
        print(f"[{idx}/{total_downloads}] Ecosystem Type: {eco}; "
              f"Site: {station}; Object: {obj_label}")
        try:
            download_object(
                session,
                token,
                row,
                OUTPUT_DIR,
                downloaded_hashes,
                DOWNLOAD_LOG,
            )
        except Exception as e:
            print(f"Failed to download {row.get('dobj')}: {e}")

    print("All downloads finished. Starting extraction of all ZIPs...")
    extract_all_downloaded_zips(OUTPUT_DIR)
    print("Done.")


if __name__ == "__main__":
    main()
