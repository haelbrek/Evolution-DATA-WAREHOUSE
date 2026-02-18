"""CLI pour explorer et telecharger les datasets stockes dans Azure Data Lake.

Fonctionnalites principales :
- lister les blobs CSV/JSON d'un filesystem ADLS;
- charger ces blobs dans des DataFrames pandas;
- sauvegarder localement les jeux telecharges (Parquet ou JSON brut).

Examples d'utilisation :

    python analytics/data_loader.py list --csv-prefix csv/
    python analytics/data_loader.py fetch --csv-prefix csv/ --json-prefix geo/ --save-local
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from azure.core.exceptions import ServiceRequestError
from azure.storage.blob import BlobServiceClient, ContainerClient

DEFAULT_FILESYSTEM = "raw"
ENV_CONNECTION_STRING = "ADLS_CONNECTION_STRING"
LEGACY_ENV_VARS = ["AZURE_STORAGE_CONNECTION_STRING", "AZURE_DATALAKE_CONNECTION_STRING"]


class MissingConnectionString(RuntimeError):
    """Aucune chaine de connexion fournie."""


def get_container_client(connection_string: str, filesystem: str, verbose: bool = False) -> ContainerClient:
    service = BlobServiceClient.from_connection_string(connection_string)
    if verbose:
        print(f"[INFO] Connexion au compte: {service.account_name} | filesystem: {filesystem}")
    return service.get_container_client(filesystem)


def list_blobs(container: ContainerClient, prefix: str = "") -> List[str]:
    return [blob.name for blob in container.list_blobs(name_starts_with=prefix)]


def load_csv(container: ContainerClient, blob_name: str) -> pd.DataFrame:
    payload = container.get_blob_client(blob_name).download_blob().readall()
    return pd.read_csv(BytesIO(payload))


def load_json(container: ContainerClient, blob_name: str) -> dict:
    payload = container.get_blob_client(blob_name).download_blob().readall()
    return json.loads(payload)


def export_dataframe(df: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(destination, index=False)


@dataclass
class FetchResult:
    csv_datasets: List[Tuple[str, pd.DataFrame]]
    json_payloads: List[Tuple[str, dict]]


def fetch_datasets(
    container: ContainerClient,
    csv_prefix: Optional[str],
    json_prefix: Optional[str],
    limit: Optional[int] = None,
) -> FetchResult:
    csv_results: List[Tuple[str, pd.DataFrame]] = []
    json_results: List[Tuple[str, dict]] = []

    if csv_prefix:
        csv_names = list_blobs(container, csv_prefix)
        if limit:
            csv_names = csv_names[:limit]
        for name in csv_names:
            csv_results.append((name, load_csv(container, name)))

    if json_prefix:
        json_names = list_blobs(container, json_prefix)
        if limit:
            json_names = json_names[:limit]
        for name in json_names:
            json_results.append((name, load_json(container, name)))

    return FetchResult(csv_results, json_results)


def save_results(results: FetchResult, output_dir: Path, convert_json: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for blob_name, df in results.csv_datasets:
        export_dataframe(df, output_dir / f"{blob_name.replace('/', '__')}.parquet")

    for blob_name, payload in results.json_payloads:
        if convert_json:
            df = pd.json_normalize(payload["communes"]) if "communes" in payload else pd.json_normalize(payload)
            export_dataframe(df, output_dir / f"{blob_name.replace('/', '__')}.parquet")
        else:
            path = output_dir / f"{blob_name.replace('/', '__')}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explore et telecharge les jeux CSV/JSON stockes dans ADLS.")
    parser.add_argument("command", choices=["list", "fetch"], help="Action a effectuer.")
    parser.add_argument(
        "--connection-string",
        help=f"Chaine de connexion Azure Storage (defaut: variable {ENV_CONNECTION_STRING}).",
    )
    parser.add_argument("--filesystem", default=DEFAULT_FILESYSTEM, help="Filesystem cible (defaut: raw).")
    parser.add_argument("--csv-prefix", help="Prefix pour les CSV (ex: csv/).")
    parser.add_argument("--json-prefix", help="Prefix pour les JSON (ex: geo/).")
    parser.add_argument("--limit", type=int, help="Nombre maximum de fichiers a traiter par type.")
    parser.add_argument("--output-dir", type=Path, default=Path("data") / "prepared", help="Dossier de sortie local.")
    parser.add_argument("--save-local", action="store_true", help="Sauvegarder les jeux telecharges en local.")
    parser.add_argument("--keep-json", action="store_true", help="Sauvegarder les payloads JSON bruts (pas de DF).")
    parser.add_argument("--verbose", action="store_true", help="Afficher des informations supplementaires.")
    return parser.parse_args()


def resolve_connection_string(args: argparse.Namespace) -> str:
    connection = args.connection_string or os.getenv(ENV_CONNECTION_STRING)
    if not connection:
        for legacy in LEGACY_ENV_VARS:
            connection = os.getenv(legacy)
            if connection:
                print(f"[WARN] Utilisation de la variable {legacy}. Pense a definir {ENV_CONNECTION_STRING} pour l'avenir.")
                break
    if not connection:
        raise MissingConnectionString(
            "Aucune chaine de connexion fournie. Passez --connection-string ou definissez "
            f"la variable {ENV_CONNECTION_STRING}."
        )
    return connection


def command_list(container: ContainerClient, csv_prefix: Optional[str], json_prefix: Optional[str]) -> None:
    if csv_prefix:
        csv_blobs = list_blobs(container, csv_prefix)
        print(f"[CSV] {len(csv_blobs)} fichiers trouves sous '{csv_prefix}':")
        for name in csv_blobs:
            print(f"  - {name}")
    if json_prefix:
        json_blobs = list_blobs(container, json_prefix)
        print(f"[JSON] {len(json_blobs)} fichiers trouves sous '{json_prefix}':")
        for name in json_blobs:
            print(f"  - {name}")


def command_fetch(args: argparse.Namespace, container: ContainerClient) -> None:
    results = fetch_datasets(container, args.csv_prefix, args.json_prefix, args.limit)

    for blob_name, df in results.csv_datasets:
        print(f"[CSV] {blob_name} -> {df.shape[0]} lignes, {df.shape[1]} colonnes")
    for blob_name, payload in results.json_payloads:
        keys = list(payload.keys())[:10]
        print(f"[JSON] {blob_name} -> clefs principales: {keys}")

    if args.save_local:
        save_results(results, args.output_dir, convert_json=not args.keep_json)
        print(f"Datasets sauvegardes dans {args.output_dir.resolve()}")


def main() -> None:
    args = parse_args()
    connection_string = resolve_connection_string(args)

    try:
        container = get_container_client(connection_string, args.filesystem, verbose=args.verbose)
    except ServiceRequestError as exc:
        hint = (
            "Connexion impossible: verifiez que la chaine pointe bien vers le Data Lake "
            "(ex: AccountName=adlselbrek) et que vous avez un acces reseau valide."
        )
        raise SystemExit(f"{hint}\nErreur detaillee: {exc}") from exc

    if args.verbose:
        print(
            f"[INFO] Prefix CSV: {args.csv_prefix or '(non specifie)'} | "
            f"Prefix JSON: {args.json_prefix or '(non specifie)'}"
        )

    if args.command == "list":
        command_list(container, args.csv_prefix, args.json_prefix)
    elif args.command == "fetch":
        command_fetch(args, container)


if __name__ == "__main__":
    main()
