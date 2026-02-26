#!/usr/bin/env python3
"""
E6 - Tests de l'evolution du Data Warehouse (10 tests)
Projet Data Engineering - Region Hauts-de-France

Valide les evolutions cles du projet E6 :
  - Journalisation (log_etl, log_erreurs)
  - Sauvegarde (vue historique backups)
  - Nouvelles sources (fait_emploi, fait_menages)
  - Variations de dimensions SCD (Type 1, 2, 3)
  - Gestion des acces RBAC
"""

import os
import re
import sys
import unittest
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text


def _load_tfvars():
    """Lit les credentials depuis terraform.tfvars."""
    tfvars_path = Path(__file__).parents[2] / 'Terraform' / 'terraform.tfvars'
    values = {}
    with open(tfvars_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            values[key.strip()] = val.strip().strip('"')
    return values


_tfvars = _load_tfvars()


class TestConfiguration:
    SERVER   = os.getenv('AZURE_SQL_SERVER',
                         _tfvars.get('sql_server_name', 'sqlelbrek-prod2') + '.database.windows.net')
    DATABASE = os.getenv('AZURE_SQL_DATABASE',
                         _tfvars.get('sql_database_name', 'projet_data_eng'))
    USER     = os.getenv('AZURE_SQL_USER',
                         _tfvars.get('sql_admin_login', 'sqladmin'))
    PASSWORD = os.getenv('AZURE_SQL_PASSWORD',
                         _tfvars.get('sql_admin_password', ''))

    @classmethod
    def get_engine(cls):
        driver = 'ODBC+Driver+17+for+SQL+Server'
        conn_str = (
            f"mssql+pyodbc://{cls.USER}:{cls.PASSWORD}"
            f"@{cls.SERVER}:1433/{cls.DATABASE}"
            f"?driver={driver}&Encrypt=yes&TrustServerCertificate=yes"
        )
        return create_engine(conn_str)


class TestEvolutionE6(unittest.TestCase):
    """10 tests de validation des evolutions E6 du Data Warehouse."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    # ----------------------------------------------------------
    # TEST 1 : Journalisation - table log_etl
    # ----------------------------------------------------------
    def test_01_log_etl_existe(self):
        """[C16] Table dwh.log_etl cree pour la journalisation ETL."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dwh' AND TABLE_NAME = 'log_etl'
            """))
            self.assertEqual(result.scalar(), 1,
                "Table dwh.log_etl non trouvee")

    # ----------------------------------------------------------
    # TEST 2 : Gestion des erreurs - table log_erreurs
    # ----------------------------------------------------------
    def test_02_log_erreurs_existe(self):
        """[C16] Table dwh.log_erreurs cree pour le suivi des erreurs."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dwh' AND TABLE_NAME = 'log_erreurs'
            """))
            self.assertEqual(result.scalar(), 1,
                "Table dwh.log_erreurs non trouvee")

    # ----------------------------------------------------------
    # TEST 3 : Supervision - vue monitoring alertes
    # ----------------------------------------------------------
    def test_03_vue_monitoring_alertes(self):
        """[C16] Vue analytics.v_monitoring_alertes disponible pour la supervision."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = 'analytics'
                  AND TABLE_NAME   = 'v_monitoring_alertes'
            """))
            self.assertEqual(result.scalar(), 1,
                "Vue v_monitoring_alertes non trouvee")

    # ----------------------------------------------------------
    # TEST 4 : Sauvegarde - vue historique backups
    # ----------------------------------------------------------
    def test_04_vue_historique_backups(self):
        """[C16] Vue analytics.v_historique_backups trace les exports BACPAC."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = 'analytics'
                  AND TABLE_NAME   = 'v_historique_backups'
            """))
            self.assertEqual(result.scalar(), 1,
                "Vue v_historique_backups non trouvee")

    # ----------------------------------------------------------
    # TEST 5 : Nouvelle source - fait_emploi
    # ----------------------------------------------------------
    def test_05_fait_emploi_existe(self):
        """[C16] Table dwh.fait_emploi integree depuis EMPLOI_CHOMAGE.csv."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dwh' AND TABLE_NAME = 'fait_emploi'
            """))
            self.assertEqual(result.scalar(), 1,
                "Table dwh.fait_emploi non trouvee")

    # ----------------------------------------------------------
    # TEST 6 : Nouvelle source - fait_menages
    # ----------------------------------------------------------
    def test_06_fait_menages_existe(self):
        """[C16] Table dwh.fait_menages integree depuis Menage.csv."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dwh' AND TABLE_NAME = 'fait_menages'
            """))
            self.assertEqual(result.scalar(), 1,
                "Table dwh.fait_menages non trouvee")

    # ----------------------------------------------------------
    # TEST 7 : SCD Type 2 - colonnes d'historisation
    # ----------------------------------------------------------
    def test_07_scd_type2_colonnes(self):
        """[C17] Colonnes SCD Type 2 presentes dans dim_geographie."""
        scd2_cols = ['date_debut_validite', 'date_fin_validite',
                     'est_actif', 'version']
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dwh'
                  AND TABLE_NAME   = 'dim_geographie'
            """))
            columns = [r[0] for r in result.fetchall()]
            for col in scd2_cols:
                self.assertIn(col, columns,
                    f"Colonne SCD Type 2 '{col}' manquante dans dim_geographie")

    # ----------------------------------------------------------
    # TEST 8 : SCD Type 2 - integrite des valeurs est_actif
    # ----------------------------------------------------------
    def test_08_scd_type2_est_actif_valide(self):
        """[C17] Valeurs est_actif valides (0 ou 1) dans dim_geographie."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM dwh.dim_geographie
                WHERE est_actif IS NULL
                   OR est_actif NOT IN (0, 1)
            """))
            invalids = result.scalar()
            self.assertEqual(invalids, 0,
                f"{invalids} enregistrement(s) avec est_actif invalide")

    # ----------------------------------------------------------
    # TEST 9 : SCD Type 3 - colonnes dans dim_demographie
    # ----------------------------------------------------------
    def test_09_scd_type3_colonnes(self):
        """[C17] Colonnes SCD Type 3 presentes dans dim_demographie."""
        scd3_cols = ['ancien_pcs_libelle', 'date_changement_pcs']
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dwh'
                  AND TABLE_NAME   = 'dim_demographie'
            """))
            columns = [r[0] for r in result.fetchall()]
            for col in scd3_cols:
                self.assertIn(col, columns,
                    f"Colonne SCD Type 3 '{col}' manquante dans dim_demographie")

    # ----------------------------------------------------------
    # TEST 10 : RBAC - 4 roles de securite (refonte E6)
    # ----------------------------------------------------------
    def test_10_rbac_roles_existent(self):
        """[C16] Les 4 nouveaux roles RBAC sont configures dans l'entrepot."""
        expected_roles = ['role_admin', 'role_etl_process',
                          'role_analyst', 'role_consultant']
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT name FROM sys.database_principals
                WHERE type = 'R' AND name LIKE 'role_%'
            """))
            roles = [r[0] for r in result.fetchall()]
            for role in expected_roles:
                self.assertIn(role, roles,
                    f"Role RBAC '{role}' non trouve")


def run_tests():
    print("=" * 60)
    print("  E6 - TESTS EVOLUTION DU DATA WAREHOUSE")
    print(f"  Date : {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)

    loader = unittest.TestLoader()
    loader.sortTestMethodsUsing = None  # Garder l'ordre numerique
    suite = loader.loadTestsFromTestCase(TestEvolutionE6)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print("  RESUME")
    print("=" * 60)
    print(f"  Tests executes : {result.testsRun}")
    print(f"  Succes         : {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"  Echecs         : {len(result.failures)}")
    print(f"  Erreurs        : {len(result.errors)}")
    print("=" * 60)

    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(run_tests())
