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

import os
import pathlib
import time
from typing import List, Dict

import requests

META_SPARQL_URL = "https://meta.icos-cp.eu/sparql"
DATA_OBJECT_URL = "https://data.icos-cp.eu/objects"
LOGIN_URL = "https://cpauth.icos-cp.eu/password/login"

# ----------------------------------------------------------------------
# User-configurable settings
# ----------------------------------------------------------------------

# Where to put the downloaded ZIPs
OUTPUT_DIR = pathlib.Path(r"D:/data/ec/raw/ICOS")

# Time window (based on acquisition start time)
# Use full network by setting START_DATE = None, END_DATE = None
START_DATE = "2023-03-01T00:00:00Z"   # inclusive, or None
END_DATE   = "2023-08-31T23:59:59Z"   # inclusive, or None

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
def build_sparql_query(
    start_date: str | None,
    end_date: str | None,
    specs: List[str],
) -> str:
    """
    Build SPARQL query that returns all data objects that:
      * have one of the given object specifications (Eddy flux raw ASCII/binary)
      * (optionally) have acquisition start time inside [start_date, end_date]
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
) -> List[Dict[str, str]]:
    """
    Execute SPARQL query and return list of dicts with metadata
    for each data object.
    """
    query = build_sparql_query(start_date, end_date, specs)

    resp = requests.post(
        META_SPARQL_URL,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    rows: List[Dict[str, str]] = []
    for b in data["results"]["bindings"]:
        row = {}
        for key, val in b.items():
            row[key] = val.get("value")
        rows.append(row)

    print(f"SPARQL returned {len(rows)} data objects.")
    return rows

def query_all_ecosystem_stations() -> dict[str, str]:
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

    resp = requests.post(
        META_SPARQL_URL,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    resp.raise_for_status()
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


def download_object(
    session: requests.Session,
    token: str,
    dobj_row: Dict[str, str],
    out_dir: pathlib.Path,
) -> None:
    """
    Download one data object, if not already present.
    """
    hash_id = hash_id_from_dobj_uri(dobj_row["dobj"])
    filename = choose_filename(dobj_row)
    eco = ecosystem_key(dobj_row.get("eco"))
    station = dobj_row.get("stationId", "NA")
    dest_dir = out_dir / sanitize_path_component(eco) / sanitize_path_component(station)
    out_path = dest_dir / filename

    if out_path.exists():
        print(f"Already downloaded: {out_path.name}")
        return

    url = f"{DATA_OBJECT_URL}/{hash_id}"
    print(f"Downloading {hash_id} -> {out_path.name}")

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
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

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

    # If user wants only ASCII or only binary, adjust the spec list
    specs = list(EDDY_SPECS)
    if ONLY_ASCII and not ONLY_BINARY:
        specs = [EDDY_SPECS[0]]
    elif ONLY_BINARY and not ONLY_ASCII:
        specs = [EDDY_SPECS[1]]

    objs = query_eddy_flux_objects(START_DATE, END_DATE, specs)
    if not objs:
        print("No data objects found for the given filters.")
        return

    # NEW: fetch ecosystem type for all ecosystem stations
    station_to_eco = query_all_ecosystem_stations()

    # NEW: annotate each L0 object row with an ecosystem type
    for r in objs:
        sid = r.get("stationId")
        if sid and sid in station_to_eco:
            r["eco"] = station_to_eco[sid]
        else:
            r["eco"] = "UNKNOWN"

    # NEW: select station IDs by ecosystem type
    selected_station_ids = select_station_ids_by_ecosystem(
        objs,
        max_sites_per_ecosystem=MAX_SITES_PER_ECOSYSTEM,
        allowed_ecosystems=ALLOWED_ECOSYSTEM_TYPES,
    )

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
            download_object(session, token, row, OUTPUT_DIR)
        except Exception as e:
            print(f"Failed to download {row.get('dobj')}: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
