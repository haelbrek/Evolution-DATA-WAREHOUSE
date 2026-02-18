from __future__ import annotations

import argparse
import io
import os
from typing import List, Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from azure.storage.blob import BlobServiceClient, ContentSettings

DEFAULT_TABLES: List[str] = [
    "dim_commune",
    "bridge_commune_code_postal",
    "stg_population",
    "stg_creation_entreprises",
    "stg_creation_entrepreneurs_individuels",
    "stg_deces",
    "stg_ds_filosofi",
    "stg_emploi_chomage",
    "stg_fecondite",
    "stg_filosofi_age_tp_nivvie",
    "stg_logement",
    "stg_menage",
    "stg_naissances",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporte des tables SQL (instance BIS) vers ADLS en Parquet."
    )
    parser.add_argument(
        "--server",
        default=os.getenv("AZURE_SQL_SERVER"),
        help="Serveur SQL (ex: sqlserver.database.windows.net) ou AZURE_SQL_SERVER.",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("AZURE_SQL_DATABASE_BIS") or os.getenv("AZURE_SQL_DATABASE", "projet_data_eng_bis"),
        help="Base cible (defaut: projet_data_eng_bis ou AZURE_SQL_DATABASE_BIS).",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("AZURE_SQL_USERNAME"),
        help="Utilisateur SQL (ou AZURE_SQL_USERNAME).",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("AZURE_SQL_PASSWORD"),
        help="Mot de passe SQL (ou AZURE_SQL_PASSWORD).",
    )
    parser.add_argument(
        "--driver",
        default=os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        help="Driver ODBC (defaut: ODBC Driver 18 for SQL Server).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("AZURE_SQL_SCHEMA", "dbo"),
        help="Schema SQL (defaut: dbo ou AZURE_SQL_SCHEMA).",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=DEFAULT_TABLES,
        help="Liste des tables a exporter (defaut: tables principales).",
    )
    parser.add_argument(
        "--adls-connection-string",
        default=os.getenv("ADLS_CONNECTION_STRING") or os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        help="Chaine de connexion ADLS (defaut: ADLS_CONNECTION_STRING ou AZURE_STORAGE_CONNECTION_STRING).",
    )
    parser.add_argument(
        "--container",
        default="raw",
        help="Conteneur ADLS cible (defaut: raw).",
    )
    parser.add_argument(
        "--prefix",
        default="sql-bis/",
        help="Prefix pour les blobs (defaut: sql-bis/).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limite de lignes par table (optionnel).",
    )
    return parser.parse_args()


def create_engine(server: str, database: str, username: str, password: str, driver: str, port: str = "1433") -> sa.Engine:
    import urllib
    # Chaîne de connexion sécurisée TLS pour Azure SQL
    params = urllib.parse.quote_plus(
        f"Driver={{{driver}}};"
        f"Server=tcp:{server},{port};"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    # Test de connexion
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def fetch_table(engine: sa.Engine, schema: str, table: str, limit: Optional[int]) -> pd.DataFrame:
    query = f"SELECT * FROM {schema}.{table}"
    if limit:
        query = f"SELECT TOP ({limit}) * FROM {schema}.{table}"
    return pd.read_sql_query(text(query), engine)


def upload_df_to_adls_parquet(
    df: pd.DataFrame,
    service: BlobServiceClient,
    container_name: str,
    blob_name: str,
) -> None:
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    blob_client = service.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(
        parquet_buffer.getvalue(),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/octet-stream"),
    )


def main() -> None:
    args = parse_args()

    # Vérification des paramètres SQL
    missing_sql = [
        k for k, v in {"server": args.server, "database": args.database, "username": args.username, "password": args.password}.items() if not v
    ]
    if missing_sql:
        raise SystemExit(f"Parametres SQL manquants: {missing_sql}")

    if not args.adls_connection_string:
        raise SystemExit("Chaine ADLS manquante. Renseigne --adls-connection-string ou ADLS_CONNECTION_STRING.")

    engine = create_engine(args.server, args.database, args.username, args.password, args.driver)
    service = BlobServiceClient.from_connection_string(args.adls_connection_string)

    for table in args.tables:
        df = fetch_table(engine, args.schema, table, args.limit)
        blob_name = f"{args.prefix.rstrip('/')}/{table}.parquet"
        upload_df_to_adls_parquet(df, service, args.container, blob_name)
        print(f"[OK] {table} -> {args.container}/{blob_name} ({len(df)} lignes).")

    engine.dispose()


if __name__ == "__main__":
    main()

