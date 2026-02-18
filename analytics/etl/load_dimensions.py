#!/usr/bin/env python3
"""
E6 - ETL : Chargement des Dimensions (avec SCD)
Projet Data Engineering - Region Hauts-de-France

Ce script charge les tables de dimensions du Data Warehouse
a partir des donnees de staging et des referentiels.

Evolution E6 :
- Ajout du logging structure (module logging)
- Ajout de la journalisation en base (dwh.log_etl)
- Ajout de la logique SCD Type 1, 2 et 3
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# Ajouter le repertoire parent au path
sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.data_prep import load_communes

# ============================================================
# E6 - Configuration du logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('etl_dimensions')


def get_connection_string(config: dict) -> str:
    """Construit la chaine de connexion SQL Server."""
    server = config.get('server', os.getenv('AZURE_SQL_SERVER', ''))
    database = config.get('database', os.getenv('AZURE_SQL_DATABASE', ''))
    user = config.get('user', os.getenv('AZURE_SQL_USER', ''))
    password = config.get('password', os.getenv('AZURE_SQL_PASSWORD', ''))

    if not server.endswith('.database.windows.net'):
        server = f"{server}.database.windows.net"

    # Tester les drivers ODBC disponibles
    for driver in ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server']:
        try:
            driver_encoded = driver.replace(' ', '+')
            return f"mssql+pyodbc://{user}:{password}@{server}:1433/{database}?driver={driver_encoded}&Encrypt=yes&TrustServerCertificate=yes"
        except:
            continue

    raise ValueError("Aucun driver ODBC disponible")


# ============================================================
# E6 - Journalisation en base de donnees
# ============================================================
def log_etl_db(engine, etape: str, table_cible: str, statut: str,
               nb_lignes: int = 0, duree_sec: float = 0, message: str = None):
    """Enregistre un log ETL dans dwh.log_etl (si la table existe)."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO dwh.log_etl (etape, table_cible, statut, nb_lignes, duree_secondes, message)
                VALUES (:etape, :table_cible, :statut, :nb_lignes, :duree_sec, :message)
            """), {
                'etape': etape, 'table_cible': table_cible, 'statut': statut,
                'nb_lignes': nb_lignes, 'duree_sec': duree_sec, 'message': message
            })
            conn.commit()
    except Exception:
        pass  # La table n'existe peut-etre pas encore


# ============================================================
# E6 - Logique SCD (Slowly Changing Dimensions)
# ============================================================
def apply_scd_type1(engine, table: str, business_key: str,
                    update_column: str, df_new: pd.DataFrame) -> int:
    """
    SCD Type 1 : Ecrasement de la valeur sans historique.
    Detecte les changements et ecrase les anciennes valeurs.
    L'ancienne valeur est tracee dans les logs.

    Returns: nombre de lignes modifiees
    """
    start = datetime.now()
    nb_changes = 0

    # Charger les enregistrements actuels
    with engine.connect() as conn:
        df_current = pd.read_sql(f"SELECT * FROM {table}", conn)

    if df_current.empty:
        return 0

    # Detecter les changements
    merged = df_new.merge(df_current[[business_key, update_column]],
                          on=business_key, suffixes=('_new', '_old'))

    for _, row in merged.iterrows():
        new_val = row.get(f'{update_column}_new', row.get(update_column))
        old_val = row.get(f'{update_column}_old')

        if pd.notna(new_val) and pd.notna(old_val) and str(new_val) != str(old_val):
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {table}
                    SET {update_column} = :new_val, date_modification = GETDATE()
                    WHERE {business_key} = :bk
                """), {'new_val': str(new_val), 'bk': row[business_key]})
                conn.commit()

            logger.info(f"SCD Type 1 - {table}: {row[business_key]} "
                        f"'{old_val}' -> '{new_val}'")
            nb_changes += 1

    duree = (datetime.now() - start).total_seconds()
    if nb_changes > 0:
        log_etl_db(engine, 'SCD_TYPE1', table, 'SUCCES', nb_changes, duree,
                   f'{nb_changes} ecrasements effectues')
    return nb_changes


def apply_scd_type2(engine, table: str, business_key: str,
                    tracked_columns: list, df_new: pd.DataFrame) -> int:
    """
    SCD Type 2 : Historisation complete.
    Ferme l'ancien enregistrement (est_actif=0, date_fin)
    et insere une nouvelle version (est_actif=1, version+1).

    Returns: nombre de changements detectes
    """
    start = datetime.now()
    nb_changes = 0

    # Charger les enregistrements actifs actuels
    with engine.connect() as conn:
        df_current = pd.read_sql(
            f"SELECT * FROM {table} WHERE est_actif = 1", conn
        )

    if df_current.empty:
        return 0

    # Detecter les changements sur les colonnes suivies
    merged = df_new.merge(df_current, on=business_key, suffixes=('_new', '_old'))

    for _, row in merged.iterrows():
        changed = False
        for col in tracked_columns:
            new_val = row.get(f'{col}_new', row.get(col))
            old_val = row.get(f'{col}_old')
            if pd.notna(new_val) and pd.notna(old_val) and str(new_val) != str(old_val):
                changed = True
                break

        if changed:
            bk_value = row[business_key]
            old_id = row.get('geo_id_old', row.get('geo_id'))
            old_version = row.get('version_old', row.get('version', 1))

            with engine.connect() as conn:
                # Fermer l'ancien enregistrement
                conn.execute(text(f"""
                    UPDATE {table}
                    SET date_fin_validite = GETDATE(), est_actif = 0,
                        date_modification = GETDATE()
                    WHERE {business_key} = :bk AND est_actif = 1
                """), {'bk': bk_value})

                # Recuperer l'ancien enregistrement pour copier les colonnes
                old_record = pd.read_sql(text(f"""
                    SELECT * FROM {table}
                    WHERE {business_key} = :bk AND est_actif = 0
                    ORDER BY version DESC
                """), conn, params={'bk': bk_value}).iloc[0]

                # Construire la nouvelle version
                new_record = old_record.to_dict()
                # Mettre a jour les colonnes trackees avec les nouvelles valeurs
                for col in tracked_columns:
                    new_val = row.get(f'{col}_new', row.get(col))
                    if pd.notna(new_val):
                        new_record[col] = new_val

                new_record['date_debut_validite'] = datetime.now()
                new_record['date_fin_validite'] = None
                new_record['est_actif'] = 1
                new_record['version'] = int(old_version) + 1
                new_record['date_creation'] = datetime.now()
                new_record['date_modification'] = datetime.now()

                # Retirer l'ID auto-incremente
                new_record.pop('geo_id', None)

                df_insert = pd.DataFrame([new_record])
                df_insert.to_sql(table.split('.')[-1], engine,
                                 schema=table.split('.')[0],
                                 if_exists='append', index=False)
                conn.commit()

            logger.info(f"SCD Type 2 - {table}: nouvelle version (v{int(old_version)+1}) "
                        f"pour {business_key}={bk_value}")
            nb_changes += 1

    duree = (datetime.now() - start).total_seconds()
    if nb_changes > 0:
        log_etl_db(engine, 'SCD_TYPE2', table, 'SUCCES', nb_changes, duree,
                   f'{nb_changes} nouvelles versions creees')
    return nb_changes


def apply_scd_type3(engine, table: str, business_key: str,
                    current_column: str, previous_column: str,
                    date_column: str, df_new: pd.DataFrame) -> int:
    """
    SCD Type 3 : Colonne precedente.
    Sauvegarde l'ancienne valeur dans une colonne dediee
    et met a jour la valeur courante.

    Returns: nombre de lignes modifiees
    """
    start = datetime.now()
    nb_changes = 0

    with engine.connect() as conn:
        df_current = pd.read_sql(f"SELECT * FROM {table}", conn)

    if df_current.empty:
        return 0

    merged = df_new.merge(df_current[[business_key, current_column]],
                          on=business_key, suffixes=('_new', '_old'))

    for _, row in merged.iterrows():
        new_val = row.get(f'{current_column}_new', row.get(current_column))
        old_val = row.get(f'{current_column}_old')

        if pd.notna(new_val) and pd.notna(old_val) and str(new_val) != str(old_val):
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {table}
                    SET {previous_column} = {current_column},
                        {current_column} = :new_val,
                        {date_column} = GETDATE(),
                        date_modification = GETDATE()
                    WHERE {business_key} = :bk
                """), {'new_val': str(new_val), 'bk': row[business_key]})
                conn.commit()

            logger.info(f"SCD Type 3 - {table}: {row[business_key]} "
                        f"'{old_val}' -> '{new_val}' (ancien conserve)")
            nb_changes += 1

    duree = (datetime.now() - start).total_seconds()
    if nb_changes > 0:
        log_etl_db(engine, 'SCD_TYPE3', table, 'SUCCES', nb_changes, duree,
                   f'{nb_changes} mises a jour avec colonne precedente')
    return nb_changes


def load_dim_temps(engine) -> int:
    """Charge la dimension temps avec les annees de reference."""
    print("\n[DIM_TEMPS] Chargement...")

    # Annees de reference du projet
    annees = [
        (2010, 'Annee 2010', True),
        (2012, 'Annee 2012', False),
        (2013, 'Annee 2013', False),
        (2014, 'Annee 2014', False),
        (2015, 'Annee 2015', True),
        (2016, 'Annee 2016', False),
        (2017, 'Annee 2017', False),
        (2018, 'Annee 2018', False),
        (2019, 'Annee 2019', False),
        (2020, 'Annee 2020', False),
        (2021, 'Annee 2021', True),
        (2022, 'Annee 2022', False),
        (2023, 'Annee 2023', False),
        (2024, 'Annee 2024', False),
    ]

    df = pd.DataFrame(annees, columns=['annee', 'libelle_periode', 'est_annee_recensement'])

    with engine.connect() as conn:
        # Verifier si la table existe et est vide
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_temps"))
        count = result.scalar()

        if count == 0:
            df.to_sql('dim_temps', engine, schema='dwh', if_exists='append', index=False)
            print(f"  [OK] {len(df)} annees inserees")
            return len(df)
        else:
            print(f"  [SKIP] Table deja alimentee ({count} lignes)")
            return 0


def load_dim_geographie(engine, communes_path: str = None) -> int:
    """Charge la dimension geographie avec les departements et communes."""
    print("\n[DIM_GEOGRAPHIE] Chargement...")

    # Departements Hauts-de-France
    departements = [
        ('02', 'Aisne', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-02'),
        ('59', 'Nord', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-59'),
        ('60', 'Oise', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-60'),
        ('62', 'Pas-de-Calais', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-62'),
        ('80', 'Somme', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-80'),
    ]

    df_dept = pd.DataFrame(departements, columns=[
        'departement_code', 'departement_nom', 'region_code', 'region_nom',
        'niveau_geo', 'geo_code_source'
    ])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_geographie"))
        count = result.scalar()

        if count == 0:
            df_dept.to_sql('dim_geographie', engine, schema='dwh', if_exists='append', index=False)
            print(f"  [OK] {len(df_dept)} departements inseres")

            # Charger les communes si le fichier existe
            if communes_path and Path(communes_path).exists():
                try:
                    dim_commune, _, _ = load_communes(communes_path)
                    # Adapter pour la structure de dim_geographie
                    df_communes = dim_commune.rename(columns={
                        'commune_code': 'commune_code',
                        'commune_nom': 'commune_nom',
                        'departement_code': 'departement_code',
                        'codes_postaux': 'codes_postaux',
                        'population': 'population_reference',
                        'surface_km2': 'surface_km2',
                        'longitude': 'longitude',
                        'latitude': 'latitude'
                    })
                    df_communes['niveau_geo'] = 'COMMUNE'
                    df_communes['region_code'] = '32'
                    df_communes['region_nom'] = 'Hauts-de-France'

                    # Ajouter le nom du departement
                    dept_mapping = {d[0]: d[1] for d in departements}
                    df_communes['departement_nom'] = df_communes['departement_code'].map(dept_mapping)

                    df_communes.to_sql('dim_geographie', engine, schema='dwh', if_exists='append', index=False)
                    print(f"  [OK] {len(df_communes)} communes inserees")
                    return len(df_dept) + len(df_communes)
                except Exception as e:
                    print(f"  [WARN] Erreur chargement communes: {e}")

            return len(df_dept)
        else:
            print(f"  [SKIP] Table deja alimentee ({count} lignes)")
            return 0


def load_dim_demographie(engine) -> int:
    """Charge la dimension demographie avec sexe, age, PCS."""
    print("\n[DIM_DEMOGRAPHIE] Chargement...")

    data = []

    # Sexe
    sexes = [('M', 'Masculin'), ('F', 'Feminin'), ('_T', 'Total')]
    for code, libelle in sexes:
        data.append({
            'sexe_code': code, 'sexe_libelle': libelle,
            'age_code': '_T', 'pcs_code': '_T'
        })

    # PCS
    pcs = [
        ('1', 'Agriculteurs exploitants', 1),
        ('2', 'Artisans, commercants, chefs d\'entreprise', 1),
        ('3', 'Cadres et professions intellectuelles superieures', 1),
        ('4', 'Professions intermediaires', 1),
        ('5', 'Employes', 1),
        ('6', 'Ouvriers', 1),
        ('7', 'Retraites', 1),
        ('9', 'Autres personnes sans activite professionnelle', 1),
        ('_T', 'Total toutes categories', 1),
    ]
    for code, libelle, niveau in pcs:
        data.append({
            'sexe_code': '_T', 'pcs_code': code, 'pcs_libelle': libelle,
            'pcs_niveau': niveau, 'age_code': '_T'
        })

    # Tranches d'age
    ages = [
        ('Y15T24', '15-24 ans', 15, 24),
        ('Y25T54', '25-54 ans', 25, 54),
        ('Y_GE55', '55 ans et plus', 55, 999),
        ('Y_GE15', '15 ans et plus', 15, 999),
        ('Y15T64', '15-64 ans', 15, 64),
        ('Y_LT30', 'Moins de 30 ans', 0, 29),
        ('Y30T39', '30-39 ans', 30, 39),
        ('Y40T49', '40-49 ans', 40, 49),
        ('Y50T59', '50-59 ans', 50, 59),
        ('Y_GE60', '60 ans et plus', 60, 999),
        ('Y60T74', '60-74 ans', 60, 74),
        ('Y_GE75', '75 ans et plus', 75, 999),
        ('_T', 'Tous ages', 0, 999),
    ]
    for code, libelle, age_min, age_max in ages:
        data.append({
            'sexe_code': '_T', 'age_code': code, 'age_libelle': libelle,
            'age_min': age_min, 'age_max': age_max, 'pcs_code': '_T'
        })

    df = pd.DataFrame(data)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_demographie"))
        count = result.scalar()

        if count == 0:
            df.to_sql('dim_demographie', engine, schema='dwh', if_exists='append', index=False)
            print(f"  [OK] {len(df)} lignes inserees")
            return len(df)
        else:
            print(f"  [SKIP] Table deja alimentee ({count} lignes)")
            return 0


def load_dim_activite(engine) -> int:
    """Charge la dimension activite avec NAF et formes juridiques."""
    print("\n[DIM_ACTIVITE] Chargement...")

    data = []

    # Sections NAF
    naf_sections = [
        ('A', 'Agriculture, sylviculture et peche', 'Primaire'),
        ('B', 'Industries extractives', 'Secondaire'),
        ('C', 'Industrie manufacturiere', 'Secondaire'),
        ('D', 'Electricite, gaz', 'Secondaire'),
        ('E', 'Eau, assainissement, dechets', 'Secondaire'),
        ('F', 'Construction', 'Secondaire'),
        ('G', 'Commerce, reparation auto', 'Tertiaire'),
        ('H', 'Transports et entreposage', 'Tertiaire'),
        ('I', 'Hebergement et restauration', 'Tertiaire'),
        ('J', 'Information et communication', 'Tertiaire'),
        ('K', 'Activites financieres', 'Tertiaire'),
        ('L', 'Activites immobilieres', 'Tertiaire'),
        ('M', 'Activites scientifiques', 'Tertiaire'),
        ('N', 'Services administratifs', 'Tertiaire'),
        ('O', 'Administration publique', 'Tertiaire'),
        ('P', 'Enseignement', 'Tertiaire'),
        ('Q', 'Sante et action sociale', 'Tertiaire'),
        ('R', 'Arts et spectacles', 'Tertiaire'),
        ('S', 'Autres services', 'Tertiaire'),
        ('_T', 'Toutes activites', 'Total'),
    ]

    for code, libelle, secteur in naf_sections:
        data.append({
            'naf_section_code': code,
            'naf_section_libelle': libelle,
            'secteur_activite': secteur,
            'forme_juridique_code': '_T'
        })

    # Formes juridiques
    formes = [
        ('10', 'Entrepreneur individuel', 'Micro'),
        ('54', 'SARL', 'PME'),
        ('57', 'SAS', 'PME'),
        ('MICRO', 'Micro-entrepreneur', 'Micro'),
        ('ENTIND_X_MICRO', 'EI hors micro', 'TPE'),
        ('OTH_SIDE', 'Autres formes', 'Autres'),
        ('_T', 'Toutes formes', 'Total'),
    ]

    for code, libelle, type_ent in formes:
        data.append({
            'naf_section_code': '_T',
            'forme_juridique_code': code,
            'forme_juridique_libelle': libelle,
            'type_entreprise': type_ent
        })

    df = pd.DataFrame(data)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_activite"))
        count = result.scalar()

        if count == 0:
            df.to_sql('dim_activite', engine, schema='dwh', if_exists='append', index=False)
            print(f"  [OK] {len(df)} lignes inserees")
            return len(df)
        else:
            print(f"  [SKIP] Table deja alimentee ({count} lignes)")
            return 0


def load_dim_indicateur(engine) -> int:
    """Charge la dimension indicateur avec les codes FILOSOFI et autres."""
    print("\n[DIM_INDICATEUR] Chargement...")

    indicateurs = [
        # Revenus FILOSOFI
        ('MED_SL', 'Niveau de vie median', 'EUR/an', 'REVENUS', 'FILOSOFI'),
        ('D1_SL', 'Premier decile', 'EUR/an', 'REVENUS', 'FILOSOFI'),
        ('D9_SL', 'Neuvieme decile', 'EUR/an', 'REVENUS', 'FILOSOFI'),
        ('IR_D9_D1_SL', 'Rapport interdecile', 'Ratio', 'REVENUS', 'FILOSOFI'),
        ('PR_MD60', 'Taux de pauvrete 60%', '%', 'PAUVRETE', 'FILOSOFI'),
        ('S_EI_DI', 'Part revenus activite', '%', 'REVENUS', 'FILOSOFI'),
        ('S_RET_PEN_DI', 'Part pensions retraites', '%', 'REVENUS', 'FILOSOFI'),
        ('S_SOC_BEN_DI', 'Part prestations sociales', '%', 'REVENUS', 'FILOSOFI'),
        ('NUM_HH', 'Nombre de menages', 'Nombre', 'POPULATION', 'FILOSOFI'),
        ('NUM_PER', 'Nombre de personnes', 'Nombre', 'POPULATION', 'FILOSOFI'),
        # Demographie
        ('LVB', 'Naissances vivantes', 'Nombre', 'DEMOGRAPHIE', 'INSEE'),
        ('DTH', 'Deces', 'Nombre', 'DEMOGRAPHIE', 'INSEE'),
        ('POP', 'Population', 'Nombre', 'POPULATION', 'INSEE RP'),
        ('DWELLINGS', 'Logements', 'Nombre', 'LOGEMENT', 'INSEE RP'),
    ]

    df = pd.DataFrame(indicateurs, columns=[
        'indicateur_code', 'indicateur_libelle', 'unite_mesure', 'domaine', 'source_donnees'
    ])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_indicateur"))
        count = result.scalar()

        if count == 0:
            df.to_sql('dim_indicateur', engine, schema='dwh', if_exists='append', index=False)
            print(f"  [OK] {len(df)} indicateurs inseres")
            return len(df)
        else:
            print(f"  [SKIP] Table deja alimentee ({count} lignes)")
            return 0


def load_dim_logement(engine) -> int:
    """Charge la dimension logement."""
    print("\n[DIM_LOGEMENT] Chargement...")

    data = [
        {'occupation_code': 'DW_MAIN', 'occupation_libelle': 'Residence principale',
         'surpeuplement_code': '0', 'surpeuplement_libelle': 'Non surpeuple'},
        {'occupation_code': 'DW_MAIN', 'occupation_libelle': 'Residence principale',
         'surpeuplement_code': '1', 'surpeuplement_libelle': 'Surpeuple'},
        {'occupation_code': 'DW_MAIN', 'occupation_libelle': 'Residence principale',
         'surpeuplement_code': '_T', 'surpeuplement_libelle': 'Total'},
    ]

    # Types de menages
    types_menages = [
        ('110', 'Personne seule homme', '1', '1 personne'),
        ('111', 'Personne seule femme', '1', '1 personne'),
        ('11', 'Personne seule', '1', '1 personne'),
        ('MF21', 'Couple sans enfant', '2', '2 personnes'),
        ('MF221', 'Couple avec 1 enfant', '3', '3 personnes'),
        ('MF222', 'Couple avec 2+ enfants', '4+', '4+ personnes'),
        ('_T', 'Total', '_T', 'Total'),
    ]

    for code, libelle, taille_code, taille_libelle in types_menages:
        data.append({
            'type_menage_code': code,
            'type_menage_libelle': libelle,
            'taille_menage_code': taille_code,
            'taille_menage_libelle': taille_libelle
        })

    df = pd.DataFrame(data)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_logement"))
        count = result.scalar()

        if count == 0:
            df.to_sql('dim_logement', engine, schema='dwh', if_exists='append', index=False)
            print(f"  [OK] {len(df)} lignes inserees")
            return len(df)
        else:
            print(f"  [SKIP] Table deja alimentee ({count} lignes)")
            return 0


def main():
    parser = argparse.ArgumentParser(description='ETL - Chargement des dimensions')
    parser.add_argument('--server', help='Serveur SQL')
    parser.add_argument('--database', help='Base de donnees')
    parser.add_argument('--user', help='Utilisateur SQL')
    parser.add_argument('--password', help='Mot de passe SQL')
    parser.add_argument('--communes', help='Chemin vers communes.json')
    parser.add_argument('--preview', action='store_true', help='Mode apercu')
    args = parser.parse_args()

    print("=" * 60)
    print("E6 - ETL : CHARGEMENT DES DIMENSIONS (avec SCD)")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 60)

    config = {
        'server': args.server or os.getenv('AZURE_SQL_SERVER'),
        'database': args.database or os.getenv('AZURE_SQL_DATABASE'),
        'user': args.user or os.getenv('AZURE_SQL_USER'),
        'password': args.password or os.getenv('AZURE_SQL_PASSWORD'),
    }

    if args.preview:
        print("[PREVIEW] Mode apercu - pas de connexion SQL")
        return 0

    try:
        connection_string = get_connection_string(config)
        engine = create_engine(connection_string)

        start_time = datetime.now()
        log_etl_db(engine, 'load_dimensions', 'ALL', 'DEBUT',
                   message='Demarrage du chargement des dimensions')

        total = 0
        total += load_dim_temps(engine)
        total += load_dim_geographie(engine, args.communes)
        total += load_dim_demographie(engine)
        total += load_dim_activite(engine)
        total += load_dim_indicateur(engine)
        total += load_dim_logement(engine)

        duree = (datetime.now() - start_time).total_seconds()
        log_etl_db(engine, 'load_dimensions', 'ALL', 'SUCCES',
                   nb_lignes=total, duree_sec=duree,
                   message=f'{total} lignes inserees en {duree:.1f}s')

        logger.info(f"TOTAL: {total} lignes inserees dans les dimensions")
        print("\n" + "=" * 60)
        print(f"TOTAL: {total} lignes inserees dans les dimensions")
        print("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Erreur chargement dimensions: {e}")
        try:
            log_etl_db(engine, 'load_dimensions', 'ALL', 'ERREUR',
                       message=str(e)[:500])
        except:
            pass
        print(f"[ERROR] {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
