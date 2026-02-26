#!/usr/bin/env python3
"""
E6 - ETL : Chargement des Tables de Faits
Projet Data Engineering - Region Hauts-de-France

Ce script charge les tables de faits du Data Warehouse
a partir des tables de staging.

Evolution E6 :
- Ajout du logging structure (module logging)
- Ajout de la journalisation en base (dwh.log_etl)
- Ajout de load_fait_emploi (source: EMPLOI_CHOMAGE_hauts_de_france.csv)
- Ajout de load_fait_menages (source: Menage_hauts_de_france.csv)
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# Ajouter le repertoire parent au path
sys.path.insert(0, str(Path(__file__).parent.parent))

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
logger = logging.getLogger('etl_facts')


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
        pass


def get_connection_string(config: dict) -> str:
    """Construit la chaine de connexion SQL Server."""
    server = config.get('server', os.getenv('AZURE_SQL_SERVER', ''))
    database = config.get('database', os.getenv('AZURE_SQL_DATABASE', ''))
    user = config.get('user', os.getenv('AZURE_SQL_USER', ''))
    password = config.get('password', os.getenv('AZURE_SQL_PASSWORD', ''))

    if not server.endswith('.database.windows.net'):
        server = f"{server}.database.windows.net"

    driver = 'ODBC+Driver+17+for+SQL+Server'
    return f"mssql+pyodbc://{user}:{password}@{server}:1433/{database}?driver={driver}&Encrypt=yes&TrustServerCertificate=yes"


def get_dim_mapping(engine, dim_table: str, key_col: str, lookup_cols: list) -> dict:
    """Recupere le mapping entre codes sources et IDs de dimension."""
    cols = ', '.join([key_col] + lookup_cols)
    query = f"SELECT {cols} FROM dwh.{dim_table}"

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    # Creer un mapping multi-colonnes si necessaire
    if len(lookup_cols) == 1:
        return dict(zip(df[lookup_cols[0]], df[key_col]))
    else:
        # Pour les dimensions avec cles composites
        return df.set_index(lookup_cols)[key_col].to_dict()


def load_fait_population(engine) -> int:
    """Charge la table de faits population."""
    print("\n[FAIT_POPULATION] Chargement...")

    # Verifier si les donnees de staging existent
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'stg_population'
        """))
        if result.scalar() == 0:
            print("  [SKIP] Table stg_population non trouvee")
            return 0

        # Lire les donnees de staging
        df_stg = pd.read_sql("SELECT * FROM dbo.stg_population", conn)

    if df_stg.empty:
        print("  [SKIP] Pas de donnees dans stg_population")
        return 0

    print(f"  [INFO] {len(df_stg)} lignes dans staging")

    # Recuperer les mappings de dimensions
    temps_map = get_dim_mapping(engine, 'dim_temps', 'temps_id', ['annee'])
    geo_map = get_dim_mapping(engine, 'dim_geographie', 'geo_id', ['departement_code'])

    # Transformer les donnees
    df_fact = df_stg.copy()

    # Detection des colonnes (insensible a la casse)
    col_map = {c.upper(): c for c in df_fact.columns}

    # Extraire l'annee du champ year ou time_period
    if 'YEAR' in col_map:
        df_fact['annee'] = df_fact[col_map['YEAR']].astype(int)
    elif 'TIME_PERIOD' in col_map:
        df_fact['annee'] = df_fact[col_map['TIME_PERIOD']].astype(int)

    # Mapper les IDs de dimensions
    df_fact['temps_id'] = df_fact['annee'].map(temps_map)

    # Extraire le code departement
    if 'GEO_CODE' in col_map:
        df_fact['dept_code'] = df_fact[col_map['GEO_CODE']].str.zfill(2)
    elif 'DEPARTEMENT' in col_map:
        df_fact['dept_code'] = df_fact[col_map['DEPARTEMENT']].astype(str).str.zfill(2)

    df_fact['geo_id'] = df_fact['dept_code'].map(geo_map)

    # Pour demo_id, utiliser une valeur par defaut (a ameliorer)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MIN(demo_id) FROM dwh.dim_demographie"))
        default_demo_id = result.scalar() or 1
    df_fact['demo_id'] = default_demo_id

    # Selectionner les colonnes pour la table de faits
    if 'POPULATION_VALUE' in col_map:
        df_fact['population'] = df_fact[col_map['POPULATION_VALUE']]
    elif 'OBS_VALUE' in col_map:
        df_fact['population'] = df_fact[col_map['OBS_VALUE']]

    df_fact['source_fichier'] = 'stg_population'

    # Filtrer les lignes valides
    df_insert = df_fact[['temps_id', 'geo_id', 'demo_id', 'population', 'source_fichier']].dropna(subset=['temps_id', 'geo_id'])

    if df_insert.empty:
        print("  [WARN] Aucune ligne valide apres transformation")
        return 0

    # Inserer dans la table de faits
    with engine.connect() as conn:
        # Verifier si deja charge
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.fait_population"))
        if result.scalar() > 0:
            print("  [SKIP] Table deja alimentee")
            return 0

    df_insert.to_sql('fait_population', engine, schema='dwh', if_exists='append', index=False)
    print(f"  [OK] {len(df_insert)} lignes inserees")
    return len(df_insert)


def load_fait_evenements_demo(engine) -> int:
    """Charge la table de faits evenements demographiques (naissances, deces)."""
    print("\n[FAIT_EVENEMENTS_DEMO] Chargement...")

    with engine.connect() as conn:
        # Verifier les tables de staging
        tables_exist = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ('stg_naissances', 'stg_deces')
        """)).scalar()

        if tables_exist < 2:
            print("  [SKIP] Tables stg_naissances ou stg_deces non trouvees")
            return 0

        # Lire naissances et deces
        df_naiss = pd.read_sql("SELECT * FROM dbo.stg_naissances", conn)
        df_deces = pd.read_sql("SELECT * FROM dbo.stg_deces", conn)

    if df_naiss.empty and df_deces.empty:
        print("  [SKIP] Pas de donnees")
        return 0

    # Recuperer les mappings
    temps_map = get_dim_mapping(engine, 'dim_temps', 'temps_id', ['annee'])
    geo_map = get_dim_mapping(engine, 'dim_geographie', 'geo_id', ['departement_code'])

    # Preparer les donnees
    records = []

    # Normaliser les noms de colonnes en minuscules pour la compatibilite
    df_naiss.columns = df_naiss.columns.str.lower()
    df_deces.columns = df_deces.columns.str.lower()

    # Traiter les naissances
    for _, row in df_naiss.iterrows():
        annee = int(row.get('year', row.get('time_period', 0)))
        dept = str(row.get('geo_code', row.get('departement', ''))).zfill(2)
        naissances = row.get('birth_count', row.get('obs_value', 0))

        temps_id = temps_map.get(annee)
        geo_id = geo_map.get(dept)

        if temps_id and geo_id:
            records.append({
                'temps_id': temps_id,
                'geo_id': geo_id,
                'naissances': naissances,
                'deces': None
            })

    # Traiter les deces
    for _, row in df_deces.iterrows():
        annee = int(row.get('year', row.get('time_period', 0)))
        dept = str(row.get('geo_code', row.get('departement', ''))).zfill(2)
        deces = row.get('death_count', row.get('obs_value', 0))

        temps_id = temps_map.get(annee)
        geo_id = geo_map.get(dept)

        if temps_id and geo_id:
            # Chercher si un enregistrement existe deja
            found = False
            for r in records:
                if r['temps_id'] == temps_id and r['geo_id'] == geo_id:
                    r['deces'] = deces
                    found = True
                    break
            if not found:
                records.append({
                    'temps_id': temps_id,
                    'geo_id': geo_id,
                    'naissances': None,
                    'deces': deces
                })

    if not records:
        print("  [WARN] Aucun enregistrement valide")
        return 0

    df_fact = pd.DataFrame(records)
    df_fact['source_fichier'] = 'stg_naissances_deces'

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.fait_evenements_demo"))
        if result.scalar() > 0:
            print("  [SKIP] Table deja alimentee")
            return 0

    df_fact.to_sql('fait_evenements_demo', engine, schema='dwh', if_exists='append', index=False)
    print(f"  [OK] {len(df_fact)} lignes inserees")
    return len(df_fact)


def load_fait_entreprises(engine) -> int:
    """Charge la table de faits entreprises."""
    print("\n[FAIT_ENTREPRISES] Chargement...")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'stg_creation_entreprises'
        """))
        if result.scalar() == 0:
            print("  [SKIP] Table stg_creation_entreprises non trouvee")
            return 0

        df_stg = pd.read_sql("SELECT * FROM dbo.stg_creation_entreprises", conn)

    if df_stg.empty:
        print("  [SKIP] Pas de donnees")
        return 0

    print(f"  [INFO] {len(df_stg)} lignes dans staging")

    # Mappings
    temps_map = get_dim_mapping(engine, 'dim_temps', 'temps_id', ['annee'])
    geo_map = get_dim_mapping(engine, 'dim_geographie', 'geo_id', ['departement_code'])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT MIN(activite_id) FROM dwh.dim_activite"))
        default_activite_id = result.scalar() or 1

    # Agreger par annee/departement (insensible a la casse)
    col_map = {c.upper(): c for c in df_stg.columns}
    annee_col = col_map.get('YEAR', col_map.get('TIME_PERIOD'))
    dept_col  = col_map.get('GEO_CODE', col_map.get('DEPARTEMENT'))
    value_col = col_map.get('CREATION_COUNT', col_map.get('OBS_VALUE'))
    df_stg['annee']     = df_stg[annee_col].astype(int)
    df_stg['dept_code'] = df_stg[dept_col].astype(str).str.zfill(2)

    df_agg = df_stg.groupby(['annee', 'dept_code']).agg({
        value_col: 'sum'
    }).reset_index()
    df_agg.columns = ['annee', 'dept_code', 'nb_creations_entreprises']

    df_agg['temps_id'] = df_agg['annee'].map(temps_map)
    df_agg['geo_id'] = df_agg['dept_code'].map(geo_map)
    df_agg['activite_id'] = default_activite_id
    df_agg['source_fichier'] = 'stg_creation_entreprises'

    df_insert = df_agg[['temps_id', 'geo_id', 'activite_id', 'nb_creations_entreprises', 'source_fichier']].dropna(subset=['temps_id', 'geo_id'])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.fait_entreprises"))
        if result.scalar() > 0:
            print("  [SKIP] Table deja alimentee")
            return 0

    df_insert.to_sql('fait_entreprises', engine, schema='dwh', if_exists='append', index=False)
    print(f"  [OK] {len(df_insert)} lignes inserees")
    return len(df_insert)


def load_fait_revenus(engine) -> int:
    """Charge la table de faits revenus (FILOSOFI)."""
    print("\n[FAIT_REVENUS] Chargement...")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'stg_ds_filosofi'
        """))
        if result.scalar() == 0:
            print("  [SKIP] Table stg_ds_filosofi non trouvee")
            return 0

        df_stg = pd.read_sql("SELECT * FROM dbo.stg_ds_filosofi", conn)

    if df_stg.empty:
        print("  [SKIP] Pas de donnees")
        return 0

    print(f"  [INFO] {len(df_stg)} lignes dans staging")

    # Mappings
    temps_map = get_dim_mapping(engine, 'dim_temps', 'temps_id', ['annee'])
    geo_map = get_dim_mapping(engine, 'dim_geographie', 'geo_id', ['departement_code'])

    # Pivoter les indicateurs FILOSOFI
    df_stg['annee'] = df_stg['year'].astype(int) if 'year' in df_stg.columns else 2021
    df_stg['dept_code'] = df_stg['geo_code'].str.zfill(2) if 'geo_code' in df_stg.columns else df_stg['departement'].str.zfill(2)

    # Pivoter par indicateur
    indicator_col = 'indicator_code' if 'indicator_code' in df_stg.columns else 'filosofi_measure'
    value_col = 'indicator_value' if 'indicator_value' in df_stg.columns else 'obs_value'

    df_pivot = df_stg.pivot_table(
        index=['annee', 'dept_code'],
        columns=indicator_col,
        values=value_col,
        aggfunc='first'
    ).reset_index()

    # Mapper les colonnes aux noms de la table de faits
    col_mapping = {
        'MED_SL': 'revenu_median',
        'D1_SL': 'revenu_d1',
        'D9_SL': 'revenu_d9',
        'IR_D9_D1_SL': 'rapport_interdecile',
        'PR_MD60': 'taux_pauvrete',
        'S_EI_DI': 'part_revenus_activite',
        'S_RET_PEN_DI': 'part_pensions_retraites',
        'S_SOC_BEN_DI': 'part_prestations_sociales',
        'NUM_HH': 'nb_menages',
        'NUM_PER': 'nb_personnes',
    }

    for old_col, new_col in col_mapping.items():
        if old_col in df_pivot.columns:
            df_pivot[new_col] = df_pivot[old_col]

    df_pivot['temps_id'] = df_pivot['annee'].map(temps_map)
    df_pivot['geo_id'] = df_pivot['dept_code'].map(geo_map)
    df_pivot['source_fichier'] = 'stg_ds_filosofi'

    # Colonnes a inserer
    fact_cols = ['temps_id', 'geo_id', 'revenu_median', 'revenu_d1', 'revenu_d9',
                 'rapport_interdecile', 'taux_pauvrete', 'part_revenus_activite',
                 'part_pensions_retraites', 'part_prestations_sociales',
                 'nb_menages', 'nb_personnes', 'source_fichier']

    existing_cols = [c for c in fact_cols if c in df_pivot.columns]
    df_insert = df_pivot[existing_cols].dropna(subset=['temps_id', 'geo_id'])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.fait_revenus"))
        if result.scalar() > 0:
            print("  [SKIP] Table deja alimentee")
            return 0

    df_insert.to_sql('fait_revenus', engine, schema='dwh', if_exists='append', index=False)
    print(f"  [OK] {len(df_insert)} lignes inserees")
    return len(df_insert)


# ============================================================
# E6 - Nouvelles sources de donnees
# ============================================================

def load_fait_emploi(engine) -> int:
    """
    E6 - Charge la table de faits emploi et chomage.
    Source: EMPLOI_CHOMAGE_hauts_de_france.csv
    Colonnes source: GEO, PCS, FREQ, EMPSTA_ENQ, TIME_PERIOD,
                     RP_MEASURE, AGE, OBS_VALUE, DEPARTEMENT
    """
    start = datetime.now()
    logger.info("Chargement de fait_emploi...")
    print("\n[FAIT_EMPLOI] Chargement...")

    # Verifier si les donnees de staging existent
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'stg_emploi_chomage'
        """))
        if result.scalar() == 0:
            logger.warning("Table stg_emploi_chomage non trouvee")
            print("  [SKIP] Table stg_emploi_chomage non trouvee")
            return 0

        df_stg = pd.read_sql("SELECT * FROM dbo.stg_emploi_chomage", conn)

    if df_stg.empty:
        logger.warning("Pas de donnees dans stg_emploi_chomage")
        print("  [SKIP] Pas de donnees dans stg_emploi_chomage")
        return 0

    print(f"  [INFO] {len(df_stg)} lignes dans staging")
    logger.info(f"Colonnes staging emploi: {list(df_stg.columns)}")
    print(f"  [INFO] Colonnes: {list(df_stg.columns)}")

    # Mappings de dimensions
    temps_map = get_dim_mapping(engine, 'dim_temps', 'temps_id', ['annee'])
    geo_map = get_dim_mapping(engine, 'dim_geographie', 'geo_id', ['departement_code'])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT MIN(demo_id) FROM dwh.dim_demographie"))
        default_demo_id = result.scalar() or 1

    # Extraire annee et departement (insensible a la casse)
    col_map = {c.upper(): c for c in df_stg.columns}

    if 'TIME_PERIOD' in col_map:
        df_stg['annee'] = df_stg[col_map['TIME_PERIOD']].astype(int)
    elif 'YEAR' in col_map:
        df_stg['annee'] = df_stg[col_map['YEAR']].astype(int)
    elif 'ANNEE' in col_map:
        df_stg['annee'] = df_stg[col_map['ANNEE']].astype(int)
    else:
        logger.error(f"Colonne annee/TIME_PERIOD/year introuvable dans: {list(df_stg.columns)}")
        return 0

    if 'DEPARTEMENT' in col_map:
        df_stg['dept_code'] = df_stg[col_map['DEPARTEMENT']].astype(str).str.zfill(2)
    elif 'DEPARTEMENT_CODE' in col_map:
        df_stg['dept_code'] = df_stg[col_map['DEPARTEMENT_CODE']].astype(str).str.zfill(2)
    elif 'DEPT_CODE' in col_map:
        df_stg['dept_code'] = df_stg[col_map['DEPT_CODE']].astype(str).str.zfill(2)
    else:
        logger.error(f"Colonne departement introuvable dans: {list(df_stg.columns)}")
        return 0

    obs_col = col_map.get('OBS_VALUE', col_map.get('POPULATION_VALUE', 'obs_value'))
    empsta_col = col_map.get('EMPSTA_ENQ', col_map.get('EMPLOYMENT_STATUS', 'empsta_enq'))

    # Agreger par annee / departement
    # EMPSTA_ENQ: 1=emploi, 2=chomage, 1T2=actifs
    records = []
    for (annee, dept), group in df_stg.groupby(['annee', 'dept_code']):
        temps_id = temps_map.get(int(annee))
        geo_id = geo_map.get(str(dept))

        if not temps_id or not geo_id:
            continue

        pop_active = group[group[empsta_col] == '1T2'][obs_col].sum() if '1T2' in group[empsta_col].values else None
        pop_emploi = group[group[empsta_col] == '1'][obs_col].sum() if '1' in group[empsta_col].values else None
        pop_chomeurs = group[group[empsta_col] == '2'][obs_col].sum() if '2' in group[empsta_col].values else None

        taux_chomage = None
        if pop_active and pop_active > 0 and pop_chomeurs:
            taux_chomage = (pop_chomeurs / pop_active) * 100

        records.append({
            'temps_id': int(temps_id),
            'geo_id': int(geo_id),
            'demo_id': int(default_demo_id),
            'population_active': pop_active,
            'population_en_emploi': pop_emploi,
            'population_chomeurs': pop_chomeurs,
            'taux_chomage': taux_chomage,
            'source_fichier': 'stg_emploi_chomage'
        })

    if not records:
        logger.warning("Aucun enregistrement valide pour fait_emploi")
        print("  [WARN] Aucun enregistrement valide")
        return 0

    df_fact = pd.DataFrame(records)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.fait_emploi"))
        if result.scalar() > 0:
            print("  [SKIP] Table deja alimentee")
            return 0

    df_fact.to_sql('fait_emploi', engine, schema='dwh', if_exists='append', index=False)

    duree = (datetime.now() - start).total_seconds()
    log_etl_db(engine, 'load_facts', 'dwh.fait_emploi', 'SUCCES',
               nb_lignes=len(df_fact), duree_sec=duree,
               message=f'{len(df_fact)} lignes inserees')
    logger.info(f"fait_emploi: {len(df_fact)} lignes inserees en {duree:.1f}s")
    print(f"  [OK] {len(df_fact)} lignes inserees")
    return len(df_fact)


def load_fait_menages(engine) -> int:
    """
    E6 - Charge la table de faits menages.
    Source: Menage_hauts_de_france.csv
    Colonnes source: GEO, PCS, FREQ, TIME_PERIOD, RP_MEASURE,
                     PREFPH, TPH, OCS, OBS_VALUE, DEPARTEMENT
    """
    start = datetime.now()
    logger.info("Chargement de fait_menages...")
    print("\n[FAIT_MENAGES] Chargement...")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ('stg_menages', 'stg_menage')
        """))
        if result.scalar() == 0:
            logger.warning("Table stg_menages/stg_menage non trouvee")
            print("  [SKIP] Table stg_menages/stg_menage non trouvee")
            return 0

        # Detecter le nom exact de la table
        table_result = conn.execute(text("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ('stg_menages', 'stg_menage')
        """))
        table_name = table_result.scalar()
        df_stg = pd.read_sql(f"SELECT * FROM dbo.{table_name}", conn)

    if df_stg.empty:
        logger.warning("Pas de donnees dans stg_menages")
        print("  [SKIP] Pas de donnees dans stg_menages")
        return 0

    print(f"  [INFO] {len(df_stg)} lignes dans staging")
    logger.info(f"Colonnes staging menages: {list(df_stg.columns)}")
    print(f"  [INFO] Colonnes: {list(df_stg.columns)}")

    # Mappings de dimensions
    temps_map = get_dim_mapping(engine, 'dim_temps', 'temps_id', ['annee'])
    geo_map = get_dim_mapping(engine, 'dim_geographie', 'geo_id', ['departement_code'])

    # Extraire annee et departement (insensible a la casse)
    col_map = {c.upper(): c for c in df_stg.columns}

    if 'TIME_PERIOD' in col_map:
        df_stg['annee'] = df_stg[col_map['TIME_PERIOD']].astype(int)
    elif 'YEAR' in col_map:
        df_stg['annee'] = df_stg[col_map['YEAR']].astype(int)
    elif 'ANNEE' in col_map:
        df_stg['annee'] = df_stg[col_map['ANNEE']].astype(int)
    else:
        logger.error(f"Colonne annee/TIME_PERIOD/year introuvable dans: {list(df_stg.columns)}")
        return 0

    if 'DEPARTEMENT' in col_map:
        df_stg['dept_code'] = df_stg[col_map['DEPARTEMENT']].astype(str).str.zfill(2)
    elif 'DEPARTEMENT_CODE' in col_map:
        df_stg['dept_code'] = df_stg[col_map['DEPARTEMENT_CODE']].astype(str).str.zfill(2)
    elif 'DEPT_CODE' in col_map:
        df_stg['dept_code'] = df_stg[col_map['DEPT_CODE']].astype(str).str.zfill(2)
    else:
        logger.error(f"Colonne departement introuvable dans: {list(df_stg.columns)}")
        return 0

    obs_col = col_map.get('OBS_VALUE', col_map.get('MEASURE_VALUE', 'obs_value'))
    measure_col = col_map.get('RP_MEASURE', 'rp_measure')

    # Agreger par annee / departement
    # RP_MEASURE: DWELLINGS = nb logements/menages, DWELLINGS_POPSIZE = population des menages
    records = []
    for (annee, dept), group in df_stg.groupby(['annee', 'dept_code']):
        temps_id = temps_map.get(int(annee))
        geo_id = geo_map.get(str(dept))

        if not temps_id or not geo_id:
            continue

        nb_menages = group[group[measure_col] == 'DWELLINGS'][obs_col].sum() or None
        nb_personnes = group[group[measure_col] == 'DWELLINGS_POPSIZE'][obs_col].sum() or None

        taille_moyenne = None
        if nb_menages and nb_menages > 0 and nb_personnes:
            taille_moyenne = nb_personnes / nb_menages

        records.append({
            'temps_id': int(temps_id),
            'geo_id': int(geo_id),
            'nb_menages': nb_menages,
            'nb_personnes': nb_personnes,
            'taille_moyenne_menage': taille_moyenne,
            'source_fichier': 'stg_menages'
        })

    if not records:
        logger.warning("Aucun enregistrement valide pour fait_menages")
        print("  [WARN] Aucun enregistrement valide")
        return 0

    df_fact = pd.DataFrame(records)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dwh.fait_menages"))
        if result.scalar() > 0:
            print("  [SKIP] Table deja alimentee")
            return 0

    df_fact.to_sql('fait_menages', engine, schema='dwh', if_exists='append', index=False)

    duree = (datetime.now() - start).total_seconds()
    log_etl_db(engine, 'load_facts', 'dwh.fait_menages', 'SUCCES',
               nb_lignes=len(df_fact), duree_sec=duree,
               message=f'{len(df_fact)} lignes inserees')
    logger.info(f"fait_menages: {len(df_fact)} lignes inserees en {duree:.1f}s")
    print(f"  [OK] {len(df_fact)} lignes inserees")
    return len(df_fact)


def _staging_exists(engine, table_name: str) -> bool:
    """Verifie si une table staging existe dans la base."""
    with engine.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = :tbl
        """), {'tbl': table_name}).scalar()
    return count > 0


def main():
    parser = argparse.ArgumentParser(description='ETL - Chargement des tables de faits')
    parser.add_argument('--server',         help='Serveur SQL')
    parser.add_argument('--database',       help='Base de donnees')
    parser.add_argument('--user',           help='Utilisateur SQL')
    parser.add_argument('--password',       help='Mot de passe SQL')
    parser.add_argument('--preview',        action='store_true', help='Mode apercu')
    parser.add_argument('--staging-failed', action='store_true',
                        help='Indique que le staging a echoue globalement (cascade)')
    parser.add_argument('--report',         help='Chemin JSON pour ecrire le rapport par table')
    args = parser.parse_args()

    print("=" * 60)
    print("E6 - ETL : CHARGEMENT DES TABLES DE FAITS")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 60)

    config = {
        'server':   args.server   or os.getenv('AZURE_SQL_SERVER'),
        'database': args.database or os.getenv('AZURE_SQL_DATABASE'),
        'user':     args.user     or os.getenv('AZURE_SQL_USER'),
        'password': args.password or os.getenv('AZURE_SQL_PASSWORD'),
    }

    if args.preview:
        print("[PREVIEW] Mode apercu")
        return 0

    # ----------------------------------------------------------------
    # Connexion
    # ----------------------------------------------------------------
    try:
        connection_string = get_connection_string(config)
        engine = create_engine(connection_string)
    except Exception as e:
        logger.error(f"Impossible de se connecter a la base : {e}")
        return 1

    start_time    = datetime.now()
    staging_failed = getattr(args, 'staging_failed', False)

    log_etl_db(engine, 'load_facts', 'ALL', 'DEBUT',
               message='Demarrage du chargement des faits'
               + (' (staging en echec)' if staging_failed else ''))

    # ----------------------------------------------------------------
    # Dependances staging par table de faits
    # ----------------------------------------------------------------
    TABLES = [
        ('dwh.fait_population',      load_fait_population,      ['stg_population']),
        ('dwh.fait_evenements_demo', load_fait_evenements_demo, ['stg_naissances', 'stg_deces']),
        ('dwh.fait_entreprises',     load_fait_entreprises,     ['stg_creation_entreprises']),
        ('dwh.fait_revenus',         load_fait_revenus,         ['stg_ds_filosofi']),
        ('dwh.fait_emploi',          load_fait_emploi,          ['stg_emploi_chomage']),
        ('dwh.fait_menages',         load_fait_menages,         ['stg_menage']),
    ]

    rapport   = {}
    total     = 0
    has_error = False

    for nom, fn, stg_requises in TABLES:
        heure = datetime.now().strftime('%H:%M:%S')

        # Cas 1 : staging global en echec — on ignore toutes les tables de faits
        if staging_failed:
            rapport[nom] = {
                'statut': 'IGNORE', 'nb_lignes': 0,
                'heure': heure, 'duree_sec': 0,
                'erreur': 'Staging global en echec — etape ignoree en cascade',
            }
            logger.warning(f"{nom} ignore : staging global en echec")
            continue

        # Cas 2 : verifier que les tables staging sources existent
        manquantes = [t for t in stg_requises if not _staging_exists(engine, t)]
        if manquantes:
            rapport[nom] = {
                'statut': 'IGNORE', 'nb_lignes': 0,
                'heure': heure, 'duree_sec': 0,
                'erreur': f"Tables staging manquantes : {', '.join(manquantes)}",
            }
            logger.warning(f"{nom} ignore : tables staging manquantes {manquantes}")
            log_etl_db(engine, 'load_facts', nom, 'IGNORE',
                       message=f"Staging manquant: {manquantes}")
            continue

        # Cas 3 : chargement normal avec try/except individuel
        t0 = datetime.now()
        try:
            nb    = fn(engine)
            duree = (datetime.now() - t0).total_seconds()
            statut = 'OK' if nb > 0 else 'SKIP'
            total += nb
            rapport[nom] = {
                'statut': statut, 'nb_lignes': nb,
                'heure': heure, 'duree_sec': duree,
            }
            log_etl_db(engine, 'load_facts', nom, statut,
                       nb_lignes=nb, duree_sec=duree)
        except Exception as e:
            duree     = (datetime.now() - t0).total_seconds()
            has_error = True
            rapport[nom] = {
                'statut': 'ERREUR', 'nb_lignes': 0,
                'heure': heure, 'duree_sec': duree,
                'erreur': str(e),
            }
            logger.error(f"Erreur chargement {nom}: {e}")
            log_etl_db(engine, 'load_facts', nom, 'ERREUR',
                       duree_sec=duree, message=str(e)[:500])

    # ----------------------------------------------------------------
    # Rapport final
    # ----------------------------------------------------------------
    duree_totale = (datetime.now() - start_time).total_seconds()
    statut_final = 'ERREUR_PARTIELLE' if has_error else 'SUCCES'
    log_etl_db(engine, 'load_facts', 'ALL', statut_final,
               nb_lignes=total, duree_sec=duree_totale,
               message=f'{total} lignes inserees en {duree_totale:.1f}s')

    logger.info(f"TOTAL: {total} lignes inserees dans les tables de faits ({statut_final})")
    print("\n" + "=" * 60)
    print(f"TOTAL    : {total} lignes inserees dans les tables de faits")
    print(f"STATUT   : {statut_final}")
    print("=" * 60)

    # Ecrire le rapport JSON si demande
    if args.report:
        import json
        from pathlib import Path
        Path(args.report).write_text(json.dumps(rapport, ensure_ascii=False, indent=2))
        logger.info(f"Rapport faits ecrit dans {args.report}")

    return 1 if has_error else 0


if __name__ == '__main__':
    sys.exit(main())
