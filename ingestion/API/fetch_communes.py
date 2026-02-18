"""Fetch commune data from a Geo API and upload the transformed dataset as JSON to Azure Data Lake Storage."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings

DEFAULT_API_URL = "https://geo.api.gouv.fr/communes"
DEFAULT_FIELDS = "nom,code,codesPostaux,population,surface,centre,contour,codeDepartement,codeRegion,departement,region"
DEFAULT_DEPARTEMENTS = ["02", "59", "60", "62", "80"]
DEFAULT_ADLS_PREFIX = "geo/communes"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch commune coordinates from a Geo API and upload the result as JSON to Azure Data Lake Storage.",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Geo API endpoint to call (default: {DEFAULT_API_URL}).",
    )
    parser.add_argument(
        "--departements",
        nargs="*",
        default=DEFAULT_DEPARTEMENTS,
        help="List of department codes to query (default: codes for Hauts-de-France). Pass an empty string to fetch all communes.",
    )
    parser.add_argument(
        "--fields",
        default=DEFAULT_FIELDS,
        help="Comma-separated list of fields to request from the API.",
    )
    parser.add_argument(
        "--geometry",
        default="contour",
        help="Geometry level to request (e.g., contour, centre). Default: contour.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("COMMUNE_API_KEY"),
        help="Optional API key. Defaults to COMMUNE_API_KEY environment variable.",
    )
    parser.add_argument(
        "--api-key-header",
        help="Header name used to send the API key (example: Authorization or X-API-Key).",
    )
    parser.add_argument(
        "--api-key-param",
        help="Query string parameter name used to send the API key (example: apikey).",
    )
    parser.add_argument(
        "--api-key-prefix",
        default="",
        help="Optional prefix when sending the API key (example: 'Bearer ').",
    )
    parser.add_argument(
        "--connection-string",
        default=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
        help="Azure Storage connection string. Defaults to AZURE_STORAGE_CONNECTION_STRING environment variable.",
    )
    parser.add_argument(
        "--container",
        default="raw",
        help="Filesystem cible dans Azure Data Lake Storage (defaut: raw).",
    )
    parser.add_argument(
        "--datalake-path",
        "--blob-path",
        dest="datalake_path",
        help="Chemin cible dans le Data Lake (defaut: geo/communes-<timestamp>.json).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds for API requests (default: 60).",
    )
    parser.add_argument(
        "--local-output",
        type=Path,
        help="Optional local path where the JSON payload will also be written.",
    )
    return parser.parse_args()


def build_auth_payload(
    api_key: str | None,
    api_key_header: str | None,
    api_key_param: str | None,
    api_key_prefix: str,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    if not api_key:
        return headers, params

    if api_key_header:
        headers[api_key_header] = f"{api_key_prefix}{api_key}"
    if api_key_param:
        params[api_key_param] = f"{api_key_prefix}{api_key}"
    if not api_key_header and not api_key_param:
        headers["Authorization"] = f"{api_key_prefix}{api_key}" if api_key_prefix else f"Bearer {api_key}"
    return headers, params


def fetch_communes(
    api_url: str,
    fields: str,
    geometry: str,
    timeout: float,
    departements: Iterable[str] | None,
    headers: Dict[str, str],
    key_query_params: Dict[str, str],
) -> List[dict]:
    session = requests.Session()
    base_params = {
        "fields": fields,
        "format": "json",
        "geometry": geometry,
    }
    base_params.update(key_query_params)

    payload: List[dict] = []

    if departements:
        for code in departements:
            if not code:
                continue
            params = dict(base_params, codeDepartement=code)
            response = session.get(api_url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected response format for department {code}: {data}")
            payload.extend(data)
    else:
        response = session.get(api_url, params=base_params, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected response format: {data}")
        payload.extend(data)

    return payload


def to_records(communes: List[dict]) -> List[dict]:
    df = pd.DataFrame(communes)
    if df.empty:
        return []

    def _extract_coord(value: dict | None, index: int) -> float | None:
        if isinstance(value, dict) and "coordinates" in value:
            coords = value["coordinates"]
            if isinstance(coords, (list, tuple)) and len(coords) > index:
                return coords[index]
        return None

    df["longitude"] = df.get("centre", pd.Series()).apply(lambda x: _extract_coord(x, 0))
    df["latitude"] = df.get("centre", pd.Series()).apply(lambda x: _extract_coord(x, 1))

    df["departement_nom"] = df.get("departement", pd.Series()).apply(
        lambda x: x.get("nom") if isinstance(x, dict) and "nom" in x else None
    )
    df["region_nom"] = df.get("region", pd.Series()).apply(
        lambda x: x.get("nom") if isinstance(x, dict) and "nom" in x else None
    )

    df["contour_geojson"] = df.get("contour", pd.Series()).apply(
        lambda x: x if isinstance(x, dict) else None
    )

    drop_cols = [col for col in ["centre", "departement", "region", "contour"] if col in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    ordered_cols = [
        "nom",
        "code",
        "codesPostaux",
        "codeDepartement",
        "departement_nom",
        "codeRegion",
        "region_nom",
        "population",
        "surface",
        "longitude",
        "latitude",
        "contour_geojson",
    ]
    existing_cols = [col for col in ordered_cols if col in df.columns]
    df = df[existing_cols + [c for c in df.columns if c not in existing_cols]]

    return df.to_dict(orient="records")


def upload_json_to_datalake(connection_string: str, filesystem: str, path: str, payload: dict) -> None:
    service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = service_client.get_container_client(filesystem)
    try:
        container_client.create_container()
    except ResourceExistsError:
        pass

    blob_client = container_client.get_blob_client(path)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    content_settings = ContentSettings(content_type="application/json", charset="utf-8")
    blob_client.upload_blob(body, overwrite=True, content_settings=content_settings)


def main() -> None:
    args = parse_args()

    if not args.connection_string:
        raise RuntimeError(
            "Azure Storage connection string is required. Provide --connection-string or set AZURE_STORAGE_CONNECTION_STRING."
        )

    departements = [code for code in args.departements if code] if args.departements else None

    headers, key_query_params = build_auth_payload(
        api_key=args.api_key,
        api_key_header=args.api_key_header,
        api_key_param=args.api_key_param,
        api_key_prefix=args.api_key_prefix,
    )

    communes = fetch_communes(
        api_url=args.api_url,
        fields=args.fields,
        geometry=args.geometry,
        timeout=args.timeout,
        departements=departements,
        headers=headers,
        key_query_params=key_query_params,
    )

    records = to_records(communes)

    if not records:
        print("No communes returned by the API. Nothing to upload.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    datalake_path = args.datalake_path or f"{DEFAULT_ADLS_PREFIX}-{timestamp}.json"

    payload = {
        "source": args.api_url,
        "fields": args.fields,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "departements": departements,
        "commune_count": len(records),
        "communes": records,
    }

    upload_json_to_datalake(args.connection_string, args.container, datalake_path, payload)
    print(f"JSON charge dans le Data Lake '{args.container}/{datalake_path}'.")

    if args.local_output:
        args.local_output.parent.mkdir(parents=True, exist_ok=True)
        args.local_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Local JSON written to {args.local_output}.")


if __name__ == "__main__":
    main()








