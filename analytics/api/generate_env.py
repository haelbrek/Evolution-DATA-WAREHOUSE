from __future__ import annotations

from pathlib import Path
from typing import Dict


def guess_sql_defaults() -> Dict[str, str]:
    """Lit Terraform/terraform.tfvars pour récupérer les paramètres SQL."""
    tfvars_path = Path("Terraform/terraform.tfvars")
    if not tfvars_path.exists():
        return {}

    values: Dict[str, str] = {}
    for raw_line in tfvars_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        values[key.strip()] = raw_value.strip().strip('"')

    defaults: Dict[str, str] = {}
    server = values.get("sql_server_name")
    if server:
        defaults["AZURE_SQL_SERVER"] = f"{server}.database.windows.net"
    if values.get("sql_database_name"):
        defaults["AZURE_SQL_DATABASE"] = values["sql_database_name"]
    if values.get("sql_admin_login"):
        defaults["AZURE_SQL_USERNAME"] = values["sql_admin_login"]
    if values.get("sql_admin_password"):
        defaults["AZURE_SQL_PASSWORD"] = values["sql_admin_password"]
    return defaults


def main() -> None:
    env_path = Path("analytics/api/.env")
    if env_path.exists():
        print(".env already exists, skipping creation")
        return

    defaults = guess_sql_defaults()
    env_lines = [
        "# fichier genere automatiquement, veuillez verifier les valeurs",
        f"AZURE_SQL_SERVER={defaults.get('AZURE_SQL_SERVER', 'sqlelbrek-prod.database.windows.net')}",
        f"AZURE_SQL_DATABASE={defaults.get('AZURE_SQL_DATABASE', 'projet_data_eng')}",
        f"AZURE_SQL_USERNAME={defaults.get('AZURE_SQL_USERNAME', 'sqladmin')}",
        f"AZURE_SQL_PASSWORD={defaults.get('AZURE_SQL_PASSWORD', 'ChangeM3!Please')}",
        "AZURE_SQL_SCHEMA=dbo",
        "AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server",
        "AZURE_SQL_CHUNKSIZE=100",
        (
            "ALLOWED_TABLES="
            "stg_population,stg_creation_entreprises,stg_creation_entrepreneurs_individuels,"
            "stg_deces,stg_ds_filosofi,stg_emploi_chomage,stg_fecondite,"
            "stg_filosofi_age_tp_nivvie,stg_logement,stg_menage,stg_naissances,"
            "dim_commune,bridge_commune_code_postal"
        ),
    ]

    env_path.write_text("\n".join(env_lines) + "\n", encoding="utf-8")
    print(".env generated with defaults (please review before use)")


if __name__ == "__main__":
    main()
