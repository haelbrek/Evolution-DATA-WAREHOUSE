#!/usr/bin/env python3
"""
E6 - ETL : Chargement des donnees de securite (RLS)
Projet Data Engineering - Region Hauts-de-France

Ce script :
1. Identifie les 101 agences (communes > 10 000 hab dans la region)
2. Genere une hierarchie d employes :
     - 1  Directeur Regional (toute la region)
     - 5  Directeurs Departementaux (1 par departement)
     - 101 Directeurs d Agence (1 par ville-agence)
     - ~400 Collaborateurs (3 a 6 par agence selon taille)
3. Charge security.agences, security.employes, security.utilisateurs_zones

Usage:
    python load_security.py --check    # Verification sans chargement
    python load_security.py --load     # Chargement en base
    python load_security.py --reset    # Vider et recharger les tables security
"""

import os
import sys
import re
import json
import logging
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# Ajout du repertoire parent au path
# ============================================================
sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.data_prep import load_communes

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
logger = logging.getLogger('etl_security')

# ============================================================
# Constantes
# ============================================================

DEPARTEMENTS = {
    '02': 'Aisne',
    '59': 'Nord',
    '60': 'Oise',
    '62': 'Pas-de-Calais',
    '80': 'Somme',
}

SEUIL_AGENCE = 10_000       # Population min pour une agence
SEUIL_GRANDE = 50_000       # Ville grande (6 collaborateurs)
SEUIL_MOYENNE = 15_000      # Ville moyenne (4-5 collaborateurs)
# En dessous de SEUIL_MOYENNE -> PETITE (3 collaborateurs)

# Banque de prenoms
PRENOMS_F = [
    'Marie', 'Sophie', 'Claire', 'Anne', 'Isabelle', 'Catherine', 'Nathalie',
    'Christine', 'Laure', 'Celine', 'Emma', 'Lea', 'Alice', 'Camille', 'Julie',
    'Lucie', 'Marion', 'Pauline', 'Manon', 'Amelie', 'Charlotte', 'Laura',
    'Sarah', 'Chloe', 'Mathilde', 'Elise', 'Margot', 'Ines', 'Juliette', 'Axelle',
]
PRENOMS_M = [
    'Jean', 'Pierre', 'Paul', 'Jacques', 'Henri', 'Louis', 'Michel', 'Claude',
    'Andre', 'Philippe', 'Francois', 'Bernard', 'Patrick', 'Alain', 'Marc',
    'Daniel', 'Laurent', 'Thomas', 'Nicolas', 'Antoine', 'Julien', 'Maxime',
    'Baptiste', 'Hugo', 'Theo', 'Remi', 'Quentin', 'Kevin', 'Alexis', 'Florian',
]
NOMS = [
    'MARTIN', 'BERNARD', 'THOMAS', 'PETIT', 'ROBERT', 'RICHARD', 'DURAND',
    'DUBOIS', 'MOREAU', 'LAURENT', 'SIMON', 'MICHEL', 'LEFEBVRE', 'LEROY',
    'ROUX', 'DAVID', 'BERTRAND', 'MOREL', 'FOURNIER', 'GIRARD', 'BONNET',
    'DUPONT', 'LAMBERT', 'FONTAINE', 'ROUSSEAU', 'VINCENT', 'MULLER', 'LEFEVRE',
    'FAURE', 'ANDRE', 'MERCIER', 'BLANC', 'GUERIN', 'BOYER', 'GARNIER',
    'CHEVALIER', 'FRANCOIS', 'LEGRAND', 'GAUTHIER', 'GARCIA', 'PERRIN', 'ROBIN',
    'CLEMENT', 'MORIN', 'NICOLAS', 'HENRY', 'ROUSSEL', 'MATHIEU', 'GAUTIER',
]

# Prenoms reserves pour les directeurs (postes hierarchiques hauts)
DIRECTEUR_REGIONAL = ('Sophie',   'MARTIN')
DIRECTEURS_DEPT = {
    '02': ('Henri',    'LAMBERT'),
    '59': ('Jean',     'DUPONT'),
    '60': ('Isabelle', 'MOREL'),
    '62': ('Patrick',  'GARNIER'),
    '80': ('Laurent',  'ROUSSEAU'),
}


# ============================================================
# Utilitaires
# ============================================================

def to_ascii(text: str) -> str:
    """Supprime les accents et caracteres speciaux (pour login_sql)."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower()


def make_login(prenom: str, nom: str) -> str:
    """Construit un login SQL de la forme prenom.nom (ascii, minuscules)."""
    return f"{to_ascii(prenom)}.{to_ascii(nom).lower()}"


def get_taille_agence(population: int) -> tuple:
    """Retourne (taille_agence, nb_collaborateurs) selon la population."""
    if population >= SEUIL_GRANDE:
        return 'GRANDE', 6
    elif population >= SEUIL_MOYENNE:
        return 'MOYENNE', 5
    else:
        return 'PETITE', 3


# ============================================================
# Connexion SQL
# ============================================================

def parse_tfvars(path: str) -> dict:
    config = {}
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                m = re.match(r'^(\w+)\s*=\s*"([^"]*)"\s*$', line)
                if m:
                    config[m.group(1)] = m.group(2)
    except FileNotFoundError:
        pass
    return config


def get_engine():
    """Cree un moteur SQLAlchemy a partir de terraform.tfvars."""
    project_root = Path(__file__).resolve().parent.parent.parent
    tfvars = parse_tfvars(str(project_root / 'Terraform' / 'terraform.tfvars'))

    server   = tfvars.get('sql_server_name', os.getenv('AZURE_SQL_SERVER', ''))
    database = tfvars.get('sql_database_name', os.getenv('AZURE_SQL_DATABASE', ''))
    user     = tfvars.get('sql_admin_login', os.getenv('AZURE_SQL_USER', ''))
    password = tfvars.get('sql_admin_password', os.getenv('AZURE_SQL_PASSWORD', ''))

    if server and not server.endswith('.database.windows.net'):
        server = f"{server}.database.windows.net"

    for driver in ['ODBC Driver 17 for SQL Server', 'ODBC Driver 18 for SQL Server']:
        drv = driver.replace(' ', '+')
        conn_str = (
            f"mssql+pyodbc://{user}:{password}@{server}:1433/{database}"
            f"?driver={drv}&Encrypt=yes&TrustServerCertificate=yes"
        )
        try:
            engine = create_engine(conn_str, fast_executemany=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connexion SQL etablie ({driver})")
            return engine
        except Exception as e:
            logger.debug(f"Driver {driver} echec: {e}")

    raise ConnectionError("Impossible de se connecter a Azure SQL Server")


# ============================================================
# Generation des donnees
# ============================================================

def build_agences(df_communes: pd.DataFrame) -> pd.DataFrame:
    """
    Identifie les 101 agences (communes > 10 000 hab).
    Retourne un DataFrame compatible avec security.agences.
    """
    df = df_communes[df_communes['population'] >= SEUIL_AGENCE].copy()
    df = df.sort_values(['departement_code', 'population'], ascending=[True, False])

    taille_list, nb_collab_list = zip(*df['population'].apply(get_taille_agence))
    df['taille_agence']     = list(taille_list)
    df['nb_collaborateurs'] = list(nb_collab_list)
    df['region']            = 'Hauts-de-France'
    df['departement_nom']   = df['departement_code'].map(DEPARTEMENTS)

    agences = df[[
        'commune_code', 'commune_nom', 'departement_code',
        'departement_nom', 'region', 'population',
        'taille_agence', 'nb_collaborateurs'
    ]].rename(columns={'commune_nom': 'ville'})

    agences = agences.reset_index(drop=True)
    agences.index = agences.index + 1   # agence_id commence a 1

    logger.info(f"  {len(agences)} agences identifiees (population >= {SEUIL_AGENCE:,})")
    for taille in ['GRANDE', 'MOYENNE', 'PETITE']:
        n = (agences['taille_agence'] == taille).sum()
        logger.info(f"    -> {taille:8s}: {n} agences")

    return agences


def build_employes(df_agences: pd.DataFrame) -> pd.DataFrame:
    """
    Genere l ensemble de la hierarchie d employes.
    Retourne un DataFrame compatible avec security.employes.
    """
    employes = []
    nom_counter = {}   # Pour eviter les doublons de login

    def make_unique_login(prenom: str, nom: str) -> str:
        base = make_login(prenom, nom)
        key = base
        if key in nom_counter:
            nom_counter[key] += 1
            key = f"{base}{nom_counter[base]}"
        else:
            nom_counter[base] = 1
        return key

    # ---- Directeur Regional ----
    prenom_dr, nom_dr = DIRECTEUR_REGIONAL
    employes.append({
        'nom': nom_dr,
        'prenom': prenom_dr,
        'login_sql': make_unique_login(prenom_dr, nom_dr),
        'email': f"{make_login(prenom_dr, nom_dr)}@agence-hdf.fr",
        'poste': 'Directrice Regionale Hauts-de-France',
        'niveau_hierarchique': 'DIRECTEUR_REGIONAL',
        'agence_id': None,
        'departement_code': None,   # Acces region entiere
        'manager_id': None,
    })
    idx_dr = 1  # employe_id du directeur regional

    # ---- Directeurs Departementaux ----
    idx_dept_directors = {}
    for i_dept, (dept_code, dept_nom) in enumerate(DEPARTEMENTS.items()):
        prenom_dd, nom_dd = DIRECTEURS_DEPT[dept_code]
        employes.append({
            'nom': nom_dd,
            'prenom': prenom_dd,
            'login_sql': make_unique_login(prenom_dd, nom_dd),
            'email': f"{make_login(prenom_dd, nom_dd)}@agence-hdf.fr",
            'poste': f"Directeur(trice) Departemental(e) - {dept_nom} ({dept_code})",
            'niveau_hierarchique': 'DIRECTEUR_DEPARTEMENT',
            'agence_id': None,
            'departement_code': dept_code,
            'manager_id': idx_dr,
        })
        idx_dept_directors[dept_code] = len(employes)  # 1-based index

    # ---- Directeurs d Agence + Collaborateurs ----
    # Banques de noms (sans les noms deja utilises pour les directeurs)
    reserved_logins = {make_login(p, n) for p, n in [DIRECTEUR_REGIONAL] + list(DIRECTEURS_DEPT.values())}
    prenom_pool_m = [p for p in PRENOMS_M if to_ascii(p) not in reserved_logins]
    prenom_pool_f = [p for p in PRENOMS_F if to_ascii(p) not in reserved_logins]
    noms_pool = [n for n in NOMS]

    gen_counter = [0]  # compteur global pour iterer sur les noms

    def next_employe(is_director: bool = False) -> tuple:
        """Retourne (prenom, nom) en alternant H/F de facon deterministe."""
        c = gen_counter[0]
        gen_counter[0] += 1
        if c % 2 == 0:
            prenom = prenom_pool_m[c % len(prenom_pool_m)]
        else:
            prenom = prenom_pool_f[c % len(prenom_pool_f)]
        nom = noms_pool[c % len(noms_pool)]
        return prenom, nom

    for agence_id, row in df_agences.iterrows():
        dept_code   = row['departement_code']
        ville       = row['ville']
        manager_idx = idx_dept_directors.get(dept_code)

        # Directeur d agence
        prenom_da, nom_da = next_employe(is_director=True)
        da_login = make_unique_login(prenom_da, nom_da)
        employes.append({
            'nom': nom_da,
            'prenom': prenom_da,
            'login_sql': da_login,
            'email': f"{da_login}@agence-hdf.fr",
            'poste': f"Directeur(trice) Agence - {ville}",
            'niveau_hierarchique': 'DIRECTEUR_AGENCE',
            'agence_id': int(agence_id),
            'departement_code': dept_code,
            'manager_id': manager_idx,
        })
        da_employe_idx = len(employes)

        # Collaborateurs
        nb_collab = int(row['nb_collaborateurs'])
        for _ in range(nb_collab):
            prenom_c, nom_c = next_employe()
            c_login = make_unique_login(prenom_c, nom_c)
            employes.append({
                'nom': nom_c,
                'prenom': prenom_c,
                'login_sql': c_login,
                'email': f"{c_login}@agence-hdf.fr",
                'poste': f"Conseiller(ere) - {ville}",
                'niveau_hierarchique': 'COLLABORATEUR',
                'agence_id': int(agence_id),
                'departement_code': dept_code,
                'manager_id': da_employe_idx,
            })

    df_emp = pd.DataFrame(employes)
    df_emp.index = df_emp.index + 1   # employe_id commence a 1

    repartition = df_emp.groupby('niveau_hierarchique').size()
    logger.info(f"  {len(df_emp)} employes generes :")
    for niv, cnt in repartition.items():
        logger.info(f"    -> {niv:30s}: {cnt}")

    return df_emp


def build_zones(df_employes: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la table utilisateurs_zones (mapping login -> dept autorise).
    Logique :
      DIRECTEUR_REGIONAL    -> departement_code IS NULL (region entiere)
      DIRECTEUR_DEPARTEMENT -> leur departement_code
      DIRECTEUR_AGENCE      -> departement_code de leur agence
      COLLABORATEUR         -> departement_code de leur agence
    """
    zones = []
    for _, emp in df_employes.iterrows():
        if emp['niveau_hierarchique'] == 'DIRECTEUR_REGIONAL':
            zones.append({'login_sql': emp['login_sql'], 'departement_code': None})
        else:
            zones.append({'login_sql': emp['login_sql'], 'departement_code': emp['departement_code']})

    return pd.DataFrame(zones)


# ============================================================
# Chargement en base
# ============================================================

def reset_security_tables(engine):
    """Vide les tables security dans l ordre des cles etrangeres."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM security.utilisateurs_zones"))
        conn.execute(text("DELETE FROM security.employes"))
        conn.execute(text("DELETE FROM security.agences"))
        conn.execute(text("DBCC CHECKIDENT('security.agences',   RESEED, 0)"))
        conn.execute(text("DBCC CHECKIDENT('security.employes',  RESEED, 0)"))
        conn.execute(text("DBCC CHECKIDENT('security.utilisateurs_zones', RESEED, 0)"))
    logger.info("Tables security videes (RESET)")


def load_agences(engine, df_agences: pd.DataFrame) -> int:
    """Insere les agences dans security.agences."""
    df = df_agences[['commune_code', 'ville', 'departement_code', 'departement_nom',
                      'region', 'population', 'taille_agence', 'nb_collaborateurs']].copy()
    df.to_sql('agences', engine, schema='security', if_exists='append', index=False)
    logger.info(f"  {len(df)} agences chargees dans security.agences")
    return len(df)


def load_employes(engine, df_emp: pd.DataFrame) -> int:
    """
    Insere les employes dans security.employes.
    Les manager_id sont des references internes au DataFrame (1-based).
    La logique IDENTITY de SQL Server va auto-incrementer, donc on insere
    sans la colonne employe_id.
    """
    # Colonnes a inserer (sans employe_id qui est IDENTITY)
    cols = ['nom', 'prenom', 'login_sql', 'email', 'poste',
            'niveau_hierarchique', 'agence_id', 'departement_code', 'manager_id']

    df = df_emp[cols].copy()
    # manager_id = index interne (1-based) : correspond a l ordre d insertion
    # On convertit None -> None (SQL NULL) et int -> int
    df['manager_id'] = df['manager_id'].where(df['manager_id'].notna(), other=None)

    df.to_sql('employes', engine, schema='security', if_exists='append', index=False)
    logger.info(f"  {len(df)} employes charges dans security.employes")
    return len(df)


def load_zones(engine, df_zones: pd.DataFrame) -> int:
    """Insere le mapping login -> zone dans security.utilisateurs_zones."""
    df = df_zones.copy()
    df.to_sql('utilisateurs_zones', engine, schema='security', if_exists='append', index=False)
    logger.info(f"  {len(df)} zones chargees dans security.utilisateurs_zones")
    return len(df)


def make_password(login_sql: str) -> str:
    """
    Genere un mot de passe fort commun a tous les utilisateurs de l agence.
    Fixe pour eviter que le mot de passe contienne le nom d utilisateur
    (interdit par la politique Azure SQL).
    """
    return "AgenceHdF#2025!R"


def create_sql_users(engine, df_employes: pd.DataFrame) -> dict:
    """
    Cree les utilisateurs SQL et leur assigne role_consultant si
    ils n existent pas encore dans sys.database_principals.

    Retourne un dict avec les compteurs :
      {'created': int, 'skipped': int, 'errors': int}
    """
    counts = {'created': 0, 'skipped': 0, 'errors': 0}

    with engine.begin() as conn:
        # Recuperer les utilisateurs SQL existants en une seule requete
        result = conn.execute(text(
            "SELECT name FROM sys.database_principals WHERE type IN ('S','E','X')"
        ))
        existing_users = {row[0] for row in result}

    logger.info(f"  {len(existing_users)} utilisateurs SQL existants detectes")

    for _, emp in df_employes.iterrows():
        login = emp['login_sql']
        if login in existing_users:
            counts['skipped'] += 1
            continue

        pwd = make_password(login)
        try:
            # Chaque CREATE USER doit etre dans sa propre transaction
            with engine.begin() as conn:
                conn.execute(text(f"CREATE USER [{login}] WITH PASSWORD = '{pwd}'"))
                conn.execute(text(f"ALTER ROLE role_consultant ADD MEMBER [{login}]"))
            counts['created'] += 1
        except Exception as e:
            logger.warning(f"  Erreur creation user '{login}': {e}")
            counts['errors'] += 1

    return counts


# ============================================================
# Verification / rapport
# ============================================================

def print_summary(df_agences: pd.DataFrame, df_employes: pd.DataFrame, df_zones: pd.DataFrame):
    """Affiche un resume de ce qui sera charge."""
    print("\n" + "=" * 60)
    print("RESUME SECURITE RLS - Hauts-de-France")
    print("=" * 60)

    print(f"\nAGENCES ({len(df_agences)}) :")
    for dept_code, dept_nom in DEPARTEMENTS.items():
        sub = df_agences[df_agences['departement_code'] == dept_code]
        print(f"  Dept {dept_code} ({dept_nom:20s}) : {len(sub):3d} agences")

    print(f"\nEMPLOYES ({len(df_employes)}) :")
    for niv in ['DIRECTEUR_REGIONAL', 'DIRECTEUR_DEPARTEMENT', 'DIRECTEUR_AGENCE', 'COLLABORATEUR']:
        cnt = (df_employes['niveau_hierarchique'] == niv).sum()
        print(f"  {niv:30s} : {cnt:4d}")

    print(f"\nZONES RLS ({len(df_zones)}) :")
    null_count = df_zones['departement_code'].isna().sum()
    print(f"  Acces region entiere (dept IS NULL) : {null_count}")
    for dept_code in sorted(df_zones['departement_code'].dropna().unique()):
        cnt = (df_zones['departement_code'] == dept_code).sum()
        print(f"  Departement {dept_code}               : {cnt:4d} employes")

    print("\n" + "=" * 60)

    # Afficher les 10 premiers employes pour verification
    print("\nEchantillon employes (10 premiers) :")
    print(df_employes[['prenom', 'nom', 'login_sql', 'niveau_hierarchique',
                        'departement_code']].head(10).to_string(index=False))
    print("=" * 60)


# ============================================================
# Point d entree
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Chargement donnees securite RLS')
    parser.add_argument('--check',        action='store_true', help='Verification sans chargement en base')
    parser.add_argument('--load',         action='store_true', help='Chargement en base de donnees')
    parser.add_argument('--reset',        action='store_true', help='Vider et recharger les tables security')
    parser.add_argument('--create-users', action='store_true', help='Creer les utilisateurs SQL et assigner role_consultant')
    args = parser.parse_args()

    if not any([args.check, args.load, args.reset, args.create_users]):
        parser.print_help()
        sys.exit(0)

    print("\n" + "=" * 60)
    print("ETL SECURITE - Chargement donnees RLS")
    print("=" * 60)

    # --- Chargement des communes ---
    project_root = Path(__file__).resolve().parent.parent.parent
    communes_path = project_root / 'data' / 'communes.json'

    logger.info(f"Chargement communes depuis : {communes_path}")
    df_communes, _, _ = load_communes(str(communes_path))

    # --- Generation des donnees ---
    logger.info("Generation des agences...")
    df_agences = build_agences(df_communes)

    logger.info("Generation des employes...")
    df_employes = build_employes(df_agences)

    logger.info("Generation des zones RLS...")
    df_zones = build_zones(df_employes)

    # --- Rapport ---
    print_summary(df_agences, df_employes, df_zones)

    if args.check:
        print("\n[CHECK] Mode verification uniquement - aucun chargement en base.")
        return

    # --- Connexion ---
    logger.info("Connexion a Azure SQL Server...")
    engine = get_engine()

    # --- Reset + Chargement des tables security ---
    if args.reset or args.load:
        if args.reset:
            logger.info("RESET : suppression des donnees existantes...")
            reset_security_tables(engine)

        t0 = datetime.now()
        logger.info("Chargement en base...")

        n_agences  = load_agences(engine, df_agences)
        n_employes = load_employes(engine, df_employes)
        n_zones    = load_zones(engine, df_zones)

        duree = (datetime.now() - t0).total_seconds()

        print(f"\n[OK] Chargement termine en {duree:.1f}s")
        print(f"     {n_agences}  agences  -> security.agences")
        print(f"     {n_employes} employes -> security.employes")
        print(f"     {n_zones}   zones    -> security.utilisateurs_zones")
        print("\n[INFO] Predicate RLS actif sur dwh.dim_geographie")
        print("       -> Filtre propage aux vues dm.* et analytics.*")

    # --- Creation optionnelle des utilisateurs SQL ---
    if args.create_users:
        print("\n" + "=" * 60)
        print("CREATION DES UTILISATEURS SQL")
        print("=" * 60)
        logger.info("Creation des utilisateurs SQL et assignation role_consultant...")
        counts = create_sql_users(engine, df_employes)
        print(f"\n[OK] Utilisateurs SQL traites :")
        print(f"     {counts['created']:4d} crees  (role_consultant assigne)")
        print(f"     {counts['skipped']:4d} ignores (existaient deja)")
        print(f"     {counts['errors']:4d} erreurs")
        print(f"\n[INFO] Mot de passe commun : AgenceHdF#2025!R")
        print(f"[INFO] Pour voir les acces : SELECT * FROM security.v_acces_employes")


if __name__ == '__main__':
    main()
