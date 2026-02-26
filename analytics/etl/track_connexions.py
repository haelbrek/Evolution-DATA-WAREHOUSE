#!/usr/bin/env python3
"""
E6 - Suivi des connexions a la base de donnees
Projet Data Engineering - Region Hauts-de-France

Ce script interroge Log Analytics pour recuperer l historique des connexions
a la base SQL et les stocke dans security.historique_connexions.

Usage:
    python track_connexions.py --days 7     # Derniers 7 jours (defaut)
    python track_connexions.py --days 30    # Dernier mois
"""

import os
import re
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pyodbc
from azure.identity import ClientSecretCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus

# ============================================================
# Configuration
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('track_connexions')

GREEN  = '\033[92m'
RED    = '\033[91m'
YELLOW = '\033[93m'
RESET  = '\033[0m'
BOLD   = '\033[1m'


def _load_tfvars() -> dict:
    tfvars_path = Path(__file__).parents[2] / 'Terraform' / 'terraform.tfvars'
    values = {}
    with open(tfvars_path, encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            values[key.strip()] = val.strip().strip('"')
    return values


_cfg = _load_tfvars()

SERVER   = _cfg.get('sql_server_name', '') + '.database.windows.net'
DATABASE = _cfg.get('sql_database_name', '')
USER     = _cfg.get('sql_admin_login', '')
PASSWORD = _cfg.get('sql_admin_password', '')

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER},1433;"
    f"DATABASE={DATABASE};"
    f"UID={USER};"
    f"PWD={PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=yes;"
)

# Nom du workspace Log Analytics (cree par Terraform)
LAW_NAME = "law-elbrek-prod"


# ============================================================
# Requete KQL - connexions reussies et echouees
# ============================================================
KQL_CONNEXIONS = """
AzureDiagnostics
| where ResourceType == "SERVERS/DATABASES"
| where Category == "SQLSecurityAuditEvents"
| where action_name_s in ("DATABASE AUTHENTICATION SUCCEEDED", "DATABASE AUTHENTICATION FAILED")
| project
    heure_connexion     = TimeGenerated,
    login_sql           = server_principal_name_s,
    ip_client           = client_ip_s,
    resultat            = action_name_s,
    application         = application_name_s,
    base_de_donnees     = database_name_s
| order by heure_connexion desc
"""


def get_law_workspace_id(resource_group: str, workspace_name: str) -> str:
    """
    Recupere l ID du workspace Log Analytics via Azure CLI.
    Necessite que 'az' soit installe et authentifie.
    """
    import subprocess, json
    cmd = [
        "az", "monitor", "log-analytics", "workspace", "show",
        "--resource-group", resource_group,
        "--workspace-name", workspace_name,
        "--query", "customerId",
        "--output", "tsv"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Impossible de recuperer l ID workspace : {result.stderr}")
    return result.stdout.strip()


def fetch_connexions_from_law(workspace_id: str, days: int) -> list[dict]:
    """
    Interroge Log Analytics et retourne les connexions sous forme de liste de dicts.
    Utilise DefaultAzureCredential (az login doit etre fait).
    """
    from azure.identity import DefaultAzureCredential
    credential = DefaultAzureCredential()
    client = LogsQueryClient(credential)

    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)

    logger.info(f"Interrogation Log Analytics ({days} derniers jours)...")
    response = client.query_workspace(
        workspace_id = workspace_id,
        query        = KQL_CONNEXIONS,
        timespan     = (start_time, end_time)
    )

    if response.status != LogsQueryStatus.SUCCESS:
        raise RuntimeError(f"Erreur Log Analytics : {response.partial_error}")

    rows = []
    for table in response.tables:
        cols = [c.name for c in table.columns]
        for row in table.rows:
            rows.append(dict(zip(cols, row)))

    logger.info(f"  {len(rows)} connexions recuperees depuis Log Analytics")
    return rows


def store_in_sql(rows: list[dict]) -> int:
    """
    Insere les connexions dans security.historique_connexions.
    Evite les doublons via la combinaison (login_sql, heure_connexion).
    """
    if not rows:
        return 0

    conn   = pyodbc.connect(CONN_STR, autocommit=False)
    cursor = conn.cursor()

    insert_sql = """
        IF NOT EXISTS (
            SELECT 1 FROM security.historique_connexions
            WHERE login_sql = ? AND heure_connexion = ?
        )
        INSERT INTO security.historique_connexions
            (login_sql, heure_connexion, statut_session, poste_client, application, snapshot_dt)
        VALUES (?, ?, ?, ?, ?, GETDATE())
    """

    inserted = 0
    for row in rows:
        login    = row.get('login_sql', '')
        heure    = row.get('heure_connexion')
        resultat = 'SUCCES' if 'SUCCEEDED' in row.get('resultat', '') else 'ECHEC'
        ip       = row.get('ip_client', '')
        appli    = row.get('application', '')

        cursor.execute(insert_sql, login, heure, login, heure, resultat, ip, appli)
        inserted += cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    return inserted


def show_current_sessions():
    """Affiche les sessions actives actuellement (sans Log Analytics)."""
    conn   = pyodbc.connect(CONN_STR, autocommit=True)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM security.v_connexions_actives ORDER BY heure_connexion DESC")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    print(f"\n{BOLD}CONNEXIONS ACTIVES EN CE MOMENT{RESET}")
    print("=" * 80)
    if not rows:
        print(f"  {YELLOW}Aucune connexion utilisateur active{RESET}")
    else:
        header = f"  {'Login':<20} {'Nom':<25} {'Niveau':<25} {'Departement':<15} {'Depuis (min)':<12}"
        print(header)
        print("  " + "-" * 78)
        for row in rows:
            r = dict(zip(cols, row))
            print(
                f"  {r.get('login_sql',''):<20} "
                f"{(r.get('nom_complet') or 'Inconnu'):<25} "
                f"{(r.get('niveau_hierarchique') or '-'):<25} "
                f"{(r.get('agence_departement') or '-'):<15} "
                f"{r.get('duree_connexion_min', 0):<12}"
            )
    print(f"\n  Total : {BOLD}{len(rows)}{RESET} session(s) active(s)")

    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Suivi connexions Azure SQL')
    parser.add_argument('--days',    type=int, default=7,
                        help='Nombre de jours d historique a recuperer (defaut: 7)')
    parser.add_argument('--current', action='store_true',
                        help='Afficher uniquement les connexions actives maintenant')
    args = parser.parse_args()

    print("=" * 60)
    print(f"{BOLD}  SUIVI CONNEXIONS - {DATABASE}{RESET}")
    print(f"  Serveur : {SERVER}")
    print("=" * 60)

    # --- Sessions actives en temps reel ---
    show_current_sessions()

    if args.current:
        return

    # --- Historique via Log Analytics ---
    print(f"\n{BOLD}HISTORIQUE DES CONNEXIONS (Log Analytics){RESET}")
    print("=" * 60)
    try:
        rg           = _cfg.get('resource_group_name', 'rg-elbrek-infra')
        workspace_id = get_law_workspace_id(rg, LAW_NAME)
        rows         = fetch_connexions_from_law(workspace_id, args.days)
        inserted     = store_in_sql(rows)

        print(f"  {GREEN}[OK]{RESET} {len(rows)} connexions trouvees")
        print(f"  {GREEN}[OK]{RESET} {inserted} nouvelles lignes inserees dans security.historique_connexions")
        print(f"\n  Pour consulter : SELECT * FROM security.historique_connexions ORDER BY heure_connexion DESC")

    except Exception as e:
        print(f"  {YELLOW}[INFO] Log Analytics non disponible : {e}{RESET}")
        print(f"  {YELLOW}       Verifiez que 'az login' est fait et que l auditing est active{RESET}")
        print(f"  {YELLOW}       Les sessions actives sont affichees ci-dessus (sans historique){RESET}")


if __name__ == '__main__':
    main()