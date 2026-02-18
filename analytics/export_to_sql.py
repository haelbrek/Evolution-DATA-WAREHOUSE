from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict

import pandas as pd
import sqlalchemy as sa

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.lib.data_prep import prepare_tables, tables_summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare les jeux locaux et les charge dans Azure SQL Database."
    )
    parser.add_argument("--project-root", type=Path, help="Racine du projet (defaut: detection automatique).")
    parser.add_argument(
        "--csv-dir",
        type=Path,
        help="Dossier contenant les CSV (defaut: <project>/uploads/landing/csv).",
    )
    parser.add_argument(
        "--communes-path",
        type=Path,
        help="Chemin vers le JSON des communes (defaut: <project>/data/communes.json).",
    )
    parser.add_argument("--schema", default=os.getenv("AZURE_SQL_SCHEMA", "dbo"), help="Schema SQL cible (defaut dbo).")
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="replace",
        help="Comportement en cas de table existante (defaut: replace).",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=int(os.getenv("AZURE_SQL_CHUNKSIZE", "100")),
        help="Nombre de lignes par batch lors de l'insertion (defaut: 100).",
    )

    parser.add_argument(
        "--server",
        default=os.getenv("AZURE_SQL_SERVER"),
        help="Serveur SQL (ex: sqlserver.database.windows.net) ou variable AZURE_SQL_SERVER.",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("AZURE_SQL_DATABASE", "projet_data_eng"),
        help="Nom de la base de donnees (defaut: projet_data_eng ou AZURE_SQL_DATABASE).",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("AZURE_SQL_USERNAME"),
        help="Utilisateur SQL (ou variable AZURE_SQL_USERNAME).",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("AZURE_SQL_PASSWORD"),
        help="Mot de passe SQL (ou variable AZURE_SQL_PASSWORD).",
    )
    parser.add_argument(
        "--driver",
        default=os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        help="Nom du driver ODBC (defaut: ODBC Driver 18 for SQL Server).",
    )
    parser.add_argument(
        "--port",
        default=os.getenv("AZURE_SQL_PORT", "1433"),
        help="Port SQL (defaut: 1433 ou AZURE_SQL_PORT).",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Affiche uniquement le resume des tables sans export.",
    )
    return parser


def load_sql_defaults_from_tfvars(project_root: Path) -> Dict[str, str]:
    tfvars_path = project_root / "Terraform" / "terraform.tfvars"
    if not tfvars_path.exists():
        return {}

    pattern = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]*)"')
    values: Dict[str, str] = {}

    for line in tfvars_path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line)
        if not match:
            continue
        key, value = match.groups()
        values[key] = value

    defaults: Dict[str, str] = {}
    server_name = values.get("sql_server_name")
    if server_name:
        defaults["server"] = f"{server_name}.database.windows.net"
    if "sql_admin_login" in values:
        defaults["username"] = values["sql_admin_login"]
    if "sql_admin_password" in values:
        defaults["password"] = values["sql_admin_password"]
    if "sql_database_name" in values:
        defaults["database"] = values["sql_database_name"]
    return defaults


def create_engine(server: str, database: str, username: str, password: str, driver: str, port: str) -> sa.Engine:
    driver_candidates = []
    if driver:
        driver_candidates.append(driver)
    driver_candidates.extend(
        [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
        ]
    )

    errors = []
    for candidate in dict.fromkeys(driver_candidates):  # preserve order, remove duplicates
        driver_token = candidate.replace(" ", "+")
        uri = f"mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver={driver_token}"
        engine = sa.create_engine(uri, fast_executemany=True)
        try:
            with engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
            print(
                f"Connexion initialisee vers {server} (base {database}) "
                f"avec le driver '{candidate}'."
            )
            return engine
        except Exception as exc:  # noqa: BLE001
            errors.append((candidate, exc))
            with contextlib.suppress(Exception):
                engine.dispose()

    message = [
        "Impossible de se connecter au serveur SQL : aucun des drivers ODBC testes n'est disponible ou valide.",
        "Drivers testes : " + ", ".join(candidate for candidate, _ in errors),
        "Installer 'ODBC Driver 18 for SQL Server' (https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server) "
        "ou renseigner --driver avec un driver present sur la machine.",
        "Detail des erreurs :",
    ]
    for candidate, exc in errors:
        message.append(f"- {candidate}: {exc}")
    raise RuntimeError("\n".join(message))


def export_tables(
    tables: Dict[str, pd.DataFrame],
    engine: sa.Engine,
    schema: str,
    if_exists: str,
    chunksize: int = 200,
) -> None:
    def _serialize_nested(value: object) -> object:
        # SQL Server via pyodbc ne sait pas décrire des listes/dicts -> on sérialise en JSON.
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        return value

    for table_name, df in tables.items():
        if df.empty:
            print(f"[WARN] Table {table_name} vide - skip.")
            continue

        # Normalise les colonnes contenant des listes/dicts (ex: contour_geojson_coordinates)
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(_serialize_nested)

        first_chunk = True
        for start in range(0, len(df), chunksize):
            chunk = df.iloc[start:start + chunksize]
            chunk_if_exists = if_exists if first_chunk else "append"
            try:
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists=chunk_if_exists,
                    index=False,
                    method="multi",
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Echec chargement table {table_name} (lignes {start}-{start + len(chunk) - 1}): {exc}"
                ) from exc
            first_chunk = False

        print(f"[OK] Table {table_name} chargee ({len(df)} lignes).")


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    tfvars_defaults = load_sql_defaults_from_tfvars(PROJECT_ROOT)

    if not args.server:
        args.server = tfvars_defaults.get("server")
    if args.database is None and tfvars_defaults.get("database"):
        args.database = tfvars_defaults["database"]
    if not args.username:
        args.username = tfvars_defaults.get("username")
    if not args.password:
        args.password = tfvars_defaults.get("password")

    missing = [key for key, value in {"server": args.server, "username": args.username, "password": args.password}.items() if not value]
    if missing:
        raise ValueError(
            "Parametres SQL manquants. Fournissez-les via les options CLI, les variables "
            f"d'environnement AZURE_SQL_* ou dans Terraform/terraform.tfvars ({', '.join(missing)})."
        )

    tables = prepare_tables(
        project_root=args.project_root,
        data_dir=args.csv_dir,
        communes_path=args.communes_path,
    )
    summary = tables_summary(tables)
    print("=== Tables preparees ===")
    print(summary.to_string(index=False))

    if args.preview:
        print("Mode preview: aucune table chargee.")
        return

    engine = create_engine(
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
        driver=args.driver,
        port=args.port,
    )
    try:
        export_tables(
            tables,
            engine,
            schema=args.schema,
            if_exists=args.if_exists,
            chunksize=args.chunksize,
        )
    finally:
        engine.dispose()
        print("Connexion SQL fermee.")


if __name__ == "__main__":
    main()
