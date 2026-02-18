#!/usr/bin/env python3
"""
E5 - Deploiement automatise du Data Warehouse
Projet Data Engineering - Region Hauts-de-France

Ce script execute les scripts SQL dans l'ordre pour creer
la structure complete de l'entrepot de donnees.

Usage:
    python deploy_dwh.py [--preview] [--tfvars path/to/terraform.tfvars]
"""

import os
import sys
import argparse
import re
from pathlib import Path


def parse_tfvars(tfvars_path: str) -> dict:
    """Parse un fichier terraform.tfvars pour extraire les variables."""
    config = {}
    tfvars_file = Path(tfvars_path)

    if not tfvars_file.exists():
        print(f"[WARN] Fichier tfvars non trouve: {tfvars_path}")
        return config

    with open(tfvars_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            match = re.match(r'^(\w+)\s*=\s*"?([^"]*)"?\s*$', line)
            if match:
                key, value = match.groups()
                config[key] = value.strip('"')

    return config


def get_sql_connection(config: dict) -> str:
    """Construit la chaine de connexion SQL Server."""
    server = config.get('sql_server_name', os.getenv('AZURE_SQL_SERVER', ''))
    database = config.get('sql_database_name', os.getenv('AZURE_SQL_DATABASE', ''))
    user = config.get('sql_admin_login', os.getenv('AZURE_SQL_USER', ''))
    password = config.get('sql_admin_password', os.getenv('AZURE_SQL_PASSWORD', ''))

    if not all([server, database, user, password]):
        raise ValueError("Configuration SQL incomplete. Verifiez les variables d'environnement ou le fichier tfvars.")

    # Ajout du suffixe Azure si necessaire
    if not server.endswith('.database.windows.net'):
        server = f"{server}.database.windows.net"

    return f"mssql+pyodbc://{user}:{password}@{server}:1433/{database}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"


def get_sql_scripts(sql_dir: Path) -> list:
    """Retourne la liste des scripts SQL dans l'ordre d'execution."""
    scripts = sorted(sql_dir.glob('*.sql'))
    return scripts


def execute_sql_file(connection_string: str, sql_file: Path, preview: bool = False) -> bool:
    """Execute un fichier SQL."""
    print(f"\n{'='*60}")
    print(f"[SQL] Execution: {sql_file.name}")
    print('='*60)

    with open(sql_file, 'r', encoding='utf-8-sig') as f:
        sql_content = f.read()

    if preview:
        print("[PREVIEW] Contenu du script:")
        print("-" * 40)
        # Affiche les 50 premieres lignes
        lines = sql_content.split('\n')[:50]
        for line in lines:
            print(f"  {line}")
        if len(sql_content.split('\n')) > 50:
            print(f"  ... ({len(sql_content.split(chr(10)))} lignes au total)")
        return True

    try:
        import pyodbc

        # Parse connection string pour pyodbc
        # Format: mssql+pyodbc://user:pass@server:port/db?driver=...
        match = re.match(r'mssql\+pyodbc://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)\?(.+)', connection_string)
        if not match:
            raise ValueError("Format de connection string invalide")

        user, password, server, port, database, params = match.groups()

        # Construire la connection string pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
        )

        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()

        # Separer par GO et executer chaque bloc
        blocks = re.split(r'\bGO\b', sql_content, flags=re.IGNORECASE)

        for i, block in enumerate(blocks, 1):
            block = block.strip()
            # Retirer les lignes de commentaires en debut de bloc
            lines = [l for l in block.split('\n') if l.strip() and not l.strip().startswith('--')]
            sql_code = '\n'.join(lines) if lines else ''
            if block and sql_code:
                try:
                    cursor.execute(block)
                    print(f"  [OK] Bloc {i}/{len(blocks)} execute")
                except pyodbc.Error as e:
                    print(f"  [WARN] Bloc {i}: {str(e)[:100]}")

        cursor.close()
        conn.close()
        print(f"[SUCCESS] {sql_file.name} execute avec succes")
        return True

    except ImportError:
        print("[ERROR] pyodbc non installe. Executez: pip install pyodbc")
        return False
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'execution: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Deploiement du Data Warehouse E5')
    parser.add_argument('--preview', action='store_true', help='Affiche les scripts sans les executer')
    parser.add_argument('--tfvars', default='terraform.tfvars', help='Chemin vers le fichier terraform.tfvars')
    parser.add_argument('--script', help='Execute un script specifique uniquement')
    args = parser.parse_args()

    print("="*60)
    print("E5 - DEPLOIEMENT DATA WAREHOUSE")
    print("Projet Data Engineering - Hauts-de-France")
    print("="*60)

    # Determiner le repertoire des scripts SQL
    script_dir = Path(__file__).parent
    sql_dir = script_dir

    print(f"\n[INFO] Repertoire SQL: {sql_dir}")

    # Charger la configuration
    tfvars_path = script_dir.parent / args.tfvars
    print(f"[INFO] Fichier tfvars: {tfvars_path}")

    config = parse_tfvars(str(tfvars_path))

    if not args.preview:
        try:
            connection_string = get_sql_connection(config)
            print(f"[INFO] Connexion SQL configuree")
        except ValueError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)
    else:
        connection_string = None
        print("[PREVIEW] Mode apercu - pas de connexion SQL")

    # Recuperer les scripts SQL
    if args.script:
        scripts = [sql_dir / args.script]
    else:
        scripts = get_sql_scripts(sql_dir)

    print(f"\n[INFO] {len(scripts)} script(s) a executer:")
    for s in scripts:
        print(f"  - {s.name}")

    # Executer les scripts
    success_count = 0
    for sql_file in scripts:
        if sql_file.exists():
            if execute_sql_file(connection_string, sql_file, args.preview):
                success_count += 1
        else:
            print(f"[WARN] Fichier non trouve: {sql_file}")

    print("\n" + "="*60)
    print(f"RESULTAT: {success_count}/{len(scripts)} scripts executes")
    print("="*60)

    return 0 if success_count == len(scripts) else 1


if __name__ == '__main__':
    sys.exit(main())
