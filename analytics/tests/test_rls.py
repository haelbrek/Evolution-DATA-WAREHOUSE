#!/usr/bin/env python3
"""
Test RLS (Row-Level Security) - dwh.dim_geographie
Projet Data Engineering - Region Hauts-de-France

Simule des utilisateurs avec EXECUTE AS USER pour verifier
que le filtrage par departement fonctionne correctement.

Utilisateurs de test (crees par 011_security_rls.sql) :
  - jean.dupont   : acces dept 59 (Nord) uniquement
  - sophie.martin : acces region entiere (NULL = tous departements)

Usage:
    python test_rls.py
"""

import os
import re
import sys
from pathlib import Path

import pyodbc


# ----------------------------------------------------------------
# Chargement de la configuration depuis terraform.tfvars
# ----------------------------------------------------------------
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

# Couleurs console
GREEN  = '\033[92m'
RED    = '\033[91m'
YELLOW = '\033[93m'
RESET  = '\033[0m'
BOLD   = '\033[1m'


def get_conn():
    return pyodbc.connect(CONN_STR, autocommit=True)


def query_as_user(cursor, login: str, sql: str) -> list:
    """Execute une requete en se placant dans le contexte d'un utilisateur.
    REVERT est toujours appele (try/finally) pour eviter de rester dans
    le contexte de l utilisateur en cas d exception."""
    cursor.execute(f"EXECUTE AS USER = '{login}'")
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    finally:
        cursor.execute("REVERT")
    return rows


def print_result(title: str, rows: list, col_names: list):
    print(f"\n  {BOLD}{title}{RESET}")
    print(f"  {'  '.join(col_names)}")
    print(f"  {'-' * 40}")
    for row in rows:
        print(f"  {'  '.join(str(v) for v in row)}")
    print(f"  {BOLD}=> {len(rows)} ligne(s){RESET}")


def ensure_test_users(cursor):
    """Cree les utilisateurs de test s ils n existent pas."""
    test_users = [
        ('jean.dupont',   'MotDePasseSecurise!2', 'role_consultant', '59'),
        ('sophie.martin', 'MotDePasseSecurise!1', 'role_consultant', None),
    ]
    for login, pwd, role, dept in test_users:
        cursor.execute(
            "SELECT COUNT(*) FROM sys.database_principals WHERE name = ? AND type = 'S'",
            login
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"CREATE USER [{login}] WITH PASSWORD = '{pwd}'")
            cursor.execute(f"ALTER ROLE {role} ADD MEMBER [{login}]")
            print(f"  [INFO] Utilisateur '{login}' cree et ajoute au {role}")
        # S assurer que la zone RLS est presente
        cursor.execute(
            "SELECT COUNT(*) FROM security.utilisateurs_zones WHERE login_sql = ?", login
        )
        if cursor.fetchone()[0] == 0:
            if dept:
                cursor.execute(
                    "INSERT INTO security.utilisateurs_zones (login_sql, departement_code) VALUES (?, ?)",
                    login, dept
                )
            else:
                cursor.execute(
                    "INSERT INTO security.utilisateurs_zones (login_sql, departement_code) VALUES (?, NULL)",
                    login
                )
            print(f"  [INFO] Zone RLS ajoutee : {login} -> dept={dept or 'NULL (region)'}")


def run_tests():
    print("=" * 60)
    print(f"{BOLD}  TEST RLS - dwh.dim_geographie{RESET}")
    print(f"  Serveur : {SERVER}")
    print(f"  Base    : {DATABASE}")
    print("=" * 60)

    passed = 0
    failed = 0

    try:
        conn   = get_conn()
        cursor = conn.cursor()
    except Exception as e:
        print(f"{RED}[ERREUR] Connexion impossible : {e}{RESET}")
        return 1

    # ----------------------------------------------------------------
    # Preparation : creer les utilisateurs de test si absents
    # ----------------------------------------------------------------
    print(f"\n{BOLD}[INIT] Verification des utilisateurs de test{RESET}")
    try:
        ensure_test_users(cursor)
        print(f"  {GREEN}Utilisateurs prets{RESET}")
    except Exception as e:
        print(f"  {RED}[ERREUR] {e}{RESET}")
        return 1

    # ----------------------------------------------------------------
    # Vue admin : tous les departements
    # ----------------------------------------------------------------
    print(f"\n{BOLD}[ADMIN] Vue sans restriction{RESET}")
    cursor.execute("""
        SELECT DISTINCT departement_code, departement_nom
        FROM dm.vm_demographie_departement
        ORDER BY departement_code
    """)
    admin_rows = cursor.fetchall()
    print_result("Departements visibles (admin via dm)", admin_rows,
                 ['departement_code', 'departement_nom'])
    admin_depts = {r[0] for r in admin_rows}

    # ----------------------------------------------------------------
    # TEST 1 : jean.dupont -> dept 59 uniquement
    # ----------------------------------------------------------------
    print(f"\n{BOLD}[TEST 1] jean.dupont (role_consultant, dept=59){RESET}")
    try:
        rows = query_as_user(cursor, 'jean.dupont', """
            SELECT DISTINCT departement_code, departement_nom
            FROM dm.vm_demographie_departement
            ORDER BY departement_code
        """)
        print_result("Departements visibles (jean.dupont)", rows,
                     ['departement_code', 'departement_nom'])

        depts = {r[0] for r in rows}
        if depts == {'59'}:
            print(f"  {GREEN}[PASS] RLS OK : jean.dupont voit uniquement le dept 59{RESET}")
            passed += 1
        elif len(depts) == 0:
            print(f"  {YELLOW}[WARN] Aucune donnee visible - verifiez que load_security.py a ete lance{RESET}")
            failed += 1
        else:
            print(f"  {RED}[FAIL] RLS KO : jean.dupont voit les depts {depts} (attendu: {{59}}){RESET}")
            failed += 1
    except Exception as e:
        print(f"  {RED}[ERREUR] {e}{RESET}")
        failed += 1

    # ----------------------------------------------------------------
    # TEST 2 : sophie.martin -> region entiere (NULL = tous)
    # ----------------------------------------------------------------
    print(f"\n{BOLD}[TEST 2] sophie.martin (role_consultant, dept=NULL = region){RESET}")
    try:
        rows = query_as_user(cursor, 'sophie.martin', """
            SELECT DISTINCT departement_code, departement_nom
            FROM dm.vm_demographie_departement
            ORDER BY departement_code
        """)
        print_result("Departements visibles (sophie.martin)", rows,
                     ['departement_code', 'departement_nom'])

        depts = {r[0] for r in rows}
        if depts == admin_depts:
            print(f"  {GREEN}[PASS] RLS OK : sophie.martin voit tous les departements (acces region){RESET}")
            passed += 1
        elif len(depts) == 0:
            print(f"  {YELLOW}[WARN] Aucune donnee visible - verifiez que load_security.py a ete lance{RESET}")
            failed += 1
        else:
            print(f"  {RED}[FAIL] RLS KO : sophie.martin voit {depts} (attendu: {admin_depts}){RESET}")
            failed += 1
    except Exception as e:
        print(f"  {RED}[ERREUR] {e}{RESET}")
        failed += 1

    # ----------------------------------------------------------------
    # TEST 3 : isolation croisee (jean ne voit pas les autres depts)
    # ----------------------------------------------------------------
    print(f"\n{BOLD}[TEST 3] Isolation croisee - jean.dupont ne voit pas le dept 62{RESET}")
    try:
        rows = query_as_user(cursor, 'jean.dupont', """
            SELECT COUNT(*) FROM dm.vm_demographie_departement
            WHERE departement_code = '62'
        """)
        count_62 = rows[0][0] if rows else 0
        if count_62 == 0:
            print(f"  {GREEN}[PASS] RLS OK : jean.dupont ne voit aucune commune du dept 62{RESET}")
            passed += 1
        else:
            print(f"  {RED}[FAIL] RLS KO : jean.dupont voit {count_62} communes du dept 62{RESET}")
            failed += 1
    except Exception as e:
        print(f"  {RED}[ERREUR] {e}{RESET}")
        failed += 1

    cursor.close()
    conn.close()

    # ----------------------------------------------------------------
    # Resume
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print(f"{BOLD}  RESUME{RESET}")
    print("=" * 60)
    print(f"  Tests passes  : {GREEN}{passed}{RESET}")
    print(f"  Tests echoues : {RED}{failed}{RESET}")
    print("=" * 60)

    if failed > 0:
        print(f"\n{YELLOW}  Conseil : si les tables sont vides, lancez d'abord :{RESET}")
        print(f"  python run_etl.py --security")

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(run_tests())
