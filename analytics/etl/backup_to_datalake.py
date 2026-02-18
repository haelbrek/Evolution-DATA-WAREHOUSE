#!/usr/bin/env python3
"""
E6 - ETL : Backup BACPAC vers Data Lake
Projet Data Engineering - Region Hauts-de-France

Exporte la base Azure SQL en fichier .bacpac via Azure CLI,
puis l'uploade vers le Data Lake ADLS Gen2 (container backups).

Usage:
    python backup_to_datalake.py --server sqlelbrek-prod --database projet_data_eng \
        --user sqladmin --password *** --storage-account dlelbrek --resource-group rg-projet-data

Prerequis:
    - Azure CLI installe et authentifie (az login)
    - Droits d'export sur la base Azure SQL
    - Droits d'ecriture sur le Storage Account
"""

import os
import sys
import re
import logging
import argparse
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime

# ============================================================
# Configuration du logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('etl_backup')


def log_etl_db(config: dict, etape: str, statut: str,
               nb_lignes: int = 0, duree_sec: float = 0, message: str = None):
    """Enregistre un log ETL dans dwh.log_etl."""
    try:
        import pyodbc

        server = config['server']
        if not server.endswith('.database.windows.net'):
            server = f"{server}.database.windows.net"

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server},1433;"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )

        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dwh.log_etl (etape, table_cible, statut, nb_lignes, duree_secondes, message)
            VALUES (?, 'DATABASE', ?, ?, ?, ?)
        """, etape, statut, nb_lignes, duree_sec, message)
        cursor.close()
        conn.close()
    except Exception:
        pass


def export_bacpac(config: dict, resource_group: str) -> str:
    """
    Exporte la base Azure SQL en .bacpac via az sql db export.
    Retourne l'URL du blob storage ou le fichier a ete exporte.
    """
    server = config['server']
    if not server.endswith('.database.windows.net'):
        server_fqdn = f"{server}.database.windows.net"
    else:
        server_fqdn = server

    storage_account = config['storage_account']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    blob_name = f"backups/{config['database']}_{timestamp}.bacpac"
    container = config.get('backup_container', 'raw')

    # Recuperer la cle du storage account
    logger.info(f"Recuperation de la cle du storage account '{storage_account}'...")
    result = subprocess.run(
        ['az', 'storage', 'account', 'keys', 'list',
         '--account-name', storage_account,
         '--resource-group', resource_group,
         '--query', '[0].value', '-o', 'tsv'],
        capture_output=True, text=True, shell=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Impossible de recuperer la cle du storage account: {result.stderr}")

    storage_key = result.stdout.strip()
    storage_uri = f"https://{storage_account}.blob.core.windows.net/{container}/{blob_name}"

    # Exporter la base en .bacpac directement vers le blob storage
    logger.info(f"Export BACPAC de '{config['database']}' vers '{storage_uri}'...")
    print(f"  [EXPORT] {config['database']} -> {blob_name}")

    export_cmd = [
        'az', 'sql', 'db', 'export',
        '--admin-user', config['user'],
        '--admin-password', config['password'],
        '--name', config['database'],
        '--server', server.replace('.database.windows.net', ''),
        '--resource-group', resource_group,
        '--storage-key', storage_key,
        '--storage-key-type', 'StorageAccessKey',
        '--storage-uri', storage_uri,
    ]

    result = subprocess.run(export_cmd, capture_output=True, text=True, timeout=1800, shell=True)

    if result.returncode != 0:
        raise RuntimeError(f"Echec export BACPAC: {result.stderr}")

    logger.info(f"Export BACPAC termine: {blob_name}")
    print(f"  [OK] Export BACPAC termine: {blob_name}")

    return blob_name


def cleanup_old_backups(config: dict, resource_group: str, retention_days: int = 30):
    """Supprime les fichiers .bacpac plus anciens que retention_days dans le Data Lake."""
    storage_account = config['storage_account']
    container = config.get('backup_container', 'raw')

    logger.info(f"Nettoyage des backups de plus de {retention_days} jours...")

    # Lister les blobs dans le dossier backups/
    result = subprocess.run(
        ['az', 'storage', 'blob', 'list',
         '--account-name', storage_account,
         '--container-name', container,
         '--prefix', 'backups/',
         '--query', '[].{name:name, lastModified:properties.lastModified}',
         '-o', 'tsv'],
        capture_output=True, text=True, shell=True
    )

    if result.returncode != 0:
        logger.warning(f"Impossible de lister les backups: {result.stderr}")
        return

    now = datetime.now()
    deleted = 0

    for line in result.stdout.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) < 2:
            continue

        blob_name = parts[0]
        try:
            # Extraire la date du nom du fichier (format: db_YYYYMMDD_HHMMSS.bacpac)
            date_str = blob_name.split('_')[-2] + '_' + blob_name.split('_')[-1].replace('.bacpac', '')
            blob_date = datetime.strptime(date_str, '%Y%m%d_%H%M%S')

            age_days = (now - blob_date).days
            if age_days > retention_days:
                del_result = subprocess.run(
                    ['az', 'storage', 'blob', 'delete',
                     '--account-name', storage_account,
                     '--container-name', container,
                     '--name', blob_name],
                    capture_output=True, text=True, shell=True
                )
                if del_result.returncode == 0:
                    deleted += 1
                    logger.info(f"Backup supprime: {blob_name} (age: {age_days} jours)")
        except (ValueError, IndexError):
            continue

    if deleted > 0:
        print(f"  [CLEAN] {deleted} ancien(s) backup(s) supprime(s)")
    else:
        print(f"  [CLEAN] Aucun backup a nettoyer")


def step_backup_datalake(config: dict) -> bool:
    """
    Etape 5 du pipeline ETL : Backup BACPAC vers Data Lake.
    Appelee depuis run_etl.py ou directement.
    """
    print("\n" + "=" * 60)
    print("ETAPE 5: BACKUP BACPAC VERS DATA LAKE")
    print("=" * 60)

    start_time = datetime.now()
    resource_group = config.get('resource_group', os.getenv('AZURE_RESOURCE_GROUP', ''))
    storage_account = config.get('storage_account', os.getenv('AZURE_STORAGE_ACCOUNT', ''))

    if not resource_group or not storage_account:
        print("  [ERROR] resource_group et storage_account sont requis")
        logger.error("Configuration manquante: resource_group ou storage_account")
        return False

    config['storage_account'] = storage_account

    log_etl_db(config, 'BACKUP_BACPAC', 'DEBUT',
               message='Demarrage export BACPAC vers Data Lake')

    try:
        # Export BACPAC vers Data Lake
        blob_name = export_bacpac(config, resource_group)

        duree = (datetime.now() - start_time).total_seconds()
        log_etl_db(config, 'BACKUP_BACPAC', 'SUCCES',
                   duree_sec=duree,
                   message=f'Export BACPAC termine: {blob_name} ({duree:.0f}s)')

        # Nettoyage des anciens backups
        retention = int(config.get('backup_retention_days', 30))
        cleanup_old_backups(config, resource_group, retention)

        print(f"\n  [OK] Backup BACPAC termine en {duree:.0f}s")
        return True

    except Exception as e:
        duree = (datetime.now() - start_time).total_seconds()
        logger.error(f"Echec backup BACPAC: {e}")
        log_etl_db(config, 'BACKUP_BACPAC', 'ERREUR',
                   duree_sec=duree,
                   message=f'Echec: {str(e)[:400]}')
        print(f"  [ERROR] {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='E6 - Backup BACPAC vers Data Lake')
    parser.add_argument('--server', help='Serveur Azure SQL')
    parser.add_argument('--database', help='Base de donnees')
    parser.add_argument('--user', help='Utilisateur SQL')
    parser.add_argument('--password', help='Mot de passe SQL')
    parser.add_argument('--storage-account', help='Nom du Storage Account ADLS Gen2')
    parser.add_argument('--resource-group', help='Resource Group Azure')
    parser.add_argument('--container', default='raw', help='Container cible (defaut: raw)')
    parser.add_argument('--retention-days', type=int, default=30,
                        help='Retention des backups en jours (defaut: 30)')
    args = parser.parse_args()

    print("=" * 60)
    print("E6 - BACKUP BACPAC VERS DATA LAKE")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 60)

    # Charger la config depuis terraform.tfvars
    from run_etl import load_config_from_tfvars
    tfvars_config = load_config_from_tfvars()

    config = {
        'server': args.server or tfvars_config.get('server', ''),
        'database': args.database or tfvars_config.get('database', ''),
        'user': args.user or tfvars_config.get('user', ''),
        'password': args.password or tfvars_config.get('password', ''),
        'storage_account': args.storage_account or tfvars_config.get('storage_account', ''),
        'resource_group': args.resource_group or tfvars_config.get('resource_group', ''),
        'backup_container': args.container,
        'backup_retention_days': args.retention_days,
    }

    success = step_backup_datalake(config)
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
