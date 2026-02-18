# E5 - Guide du Data Warehouse
## Projet Data Engineering - Region Hauts-de-France

---

## Table des matieres

1. [Qu'est-ce qu'un Data Warehouse ?](#1-quest-ce-quun-data-warehouse)
2. [Architecture du projet](#2-architecture-du-projet)
3. [Description des scripts SQL](#3-description-des-scripts-sql)
4. [Description des scripts ETL Python](#4-description-des-scripts-etl-python)
5. [Avant et apres l'ETL](#5-avant-et-apres-letl)
6. [Commandes d'execution](#6-commandes-dexecution)
7. [Schema de la base de donnees](#7-schema-de-la-base-de-donnees)

---

## 1. Qu'est-ce qu'un Data Warehouse ?

### Definition

Un **Data Warehouse** (entrepot de donnees) est une base de donnees specialisee concu pour l'analyse et le reporting. Contrairement aux bases de donnees transactionnelles (OLTP) qui gerent les operations quotidiennes, le Data Warehouse est optimise pour les requetes analytiques complexes (OLAP).

### Pourquoi utiliser un Data Warehouse ?

| Probleme AVANT | Solution APRES (Data Warehouse) |
|----------------|--------------------------------|
| Donnees dispersees dans plusieurs sources (CSV, API, bases) | Donnees centralisees en un seul endroit |
| Formats differents, difficiles a combiner | Format uniforme et standardise |
| Requetes lentes sur les bases de production | Optimise pour les analyses rapides |
| Pas d'historique des donnees | Conservation de l'historique complet |
| Difficulte a croiser les informations | Jointures facilitees par le schema en etoile |

### Schema en Etoile (Star Schema)

Notre Data Warehouse utilise un **schema en etoile** :

```
                    +----------------+
                    |  dim_temps     |
                    | (annee, mois)  |
                    +-------+--------+
                            |
+----------------+  +-------+--------+  +------------------+
| dim_geographie |--| FAIT_POPULATION|--| dim_demographie  |
| (departement)  |  | (mesures)      |  | (sexe, age, PCS) |
+----------------+  +----------------+  +------------------+
```

- **Tables de DIMENSIONS** : Contiennent les attributs descriptifs (qui, quoi, ou, quand)
- **Tables de FAITS** : Contiennent les mesures numeriques (combien, quantite)

### Objectifs du Data Warehouse dans ce projet

1. **Centraliser** les donnees de la region Hauts-de-France (population, entreprises, revenus)
2. **Historiser** les donnees sur plusieurs annees (2010-2024)
3. **Faciliter** les analyses croisees (population par departement et par annee)
4. **Alimenter** les rapports et tableaux de bord Power BI
5. **Optimiser** les performances des requetes analytiques

---

## 2. Architecture du projet

### Vue d'ensemble

```
+------------------+     +------------------+     +------------------+
|   SOURCES        |     |   DATA LAKE      |     |  DATA WAREHOUSE  |
|------------------|     |------------------|     |------------------|
| - API INSEE      | --> | - Zone RAW       | --> | - Dimensions     |
| - Fichiers CSV   |     | - Zone STAGING   |     | - Faits          |
| - Fichiers Excel |     | - Zone CURATED   |     | - Datamarts      |
+------------------+     +------------------+     +------------------+
                                                           |
                                                           v
                                                  +------------------+
                                                  |   REPORTING      |
                                                  |------------------|
                                                  | - Power BI       |
                                                  | - Requetes SQL   |
                                                  +------------------+
```

### Flux de donnees

1. **Ingestion** : Les donnees brutes arrivent dans le Data Lake (zone `raw`)
2. **Transformation** : Les donnees sont nettoyees et preparees (zone `staging`)
3. **Chargement DWH** : Les donnees sont chargees dans le Data Warehouse
4. **Analyse** : Les datamarts fournissent des vues pre-agregees pour le reporting

---

## 3. Description des scripts SQL

Les scripts SQL sont dans le dossier `terraform/sql/` et s'executent dans l'ordre numerique.

### 001_create_schemas.sql

**Role** : Creer les schemas (namespaces) pour organiser les tables.

| Schema | Description |
|--------|-------------|
| `stg` | Tables de staging (donnees temporaires) |
| `dwh` | Tables du Data Warehouse (dimensions + faits) |
| `dm` | Datamarts (vues agregees pour le reporting) |
| `analytics` | Vues analytiques supplementaires |

```sql
-- Exemple de ce que fait le script
CREATE SCHEMA stg;      -- Pour les donnees de staging
CREATE SCHEMA dwh;      -- Pour les dimensions et faits
CREATE SCHEMA dm;       -- Pour les datamarts
CREATE SCHEMA analytics; -- Pour les vues analytiques
```

---

### 002_create_dimensions.sql

**Role** : Creer les tables de dimensions (attributs descriptifs).

| Table | Description | Colonnes principales |
|-------|-------------|---------------------|
| `dwh.dim_temps` | Dimension temporelle | annee, trimestre, mois |
| `dwh.dim_geographie` | Dimension geographique | departement_code, departement_nom, region |
| `dwh.dim_demographie` | Dimension demographique | sexe, tranche_age, PCS |
| `dwh.dim_activite` | Dimension activite economique | code_NAF, secteur, forme_juridique |
| `dwh.dim_indicateur` | Dimension indicateurs | code_indicateur, unite, source |
| `dwh.dim_logement` | Dimension logement | type_occupation, surpeuplement |

```sql
-- Exemple : Table dim_temps
CREATE TABLE dwh.dim_temps (
    temps_id INT IDENTITY(1,1) PRIMARY KEY,  -- Cle primaire auto-incrementee
    annee INT NOT NULL,                       -- 2010, 2015, 2021...
    trimestre INT NULL,                       -- 1, 2, 3, 4
    mois INT NULL,                            -- 1-12
    libelle_periode NVARCHAR(50),             -- "Annee 2021"
    est_annee_recensement BIT                 -- 1 si annee de recensement
);
```

---

### 003_create_facts.sql

**Role** : Creer les tables de faits (mesures numeriques).

| Table | Description | Mesures |
|-------|-------------|---------|
| `dwh.fait_population` | Population par territoire | population, population_hommes, population_femmes |
| `dwh.fait_evenements_demo` | Naissances et deces | naissances, deces, solde_naturel |
| `dwh.fait_entreprises` | Creations d'entreprises | nb_creations, nb_micro, nb_ei |
| `dwh.fait_revenus` | Revenus et pauvrete | revenu_median, taux_pauvrete |
| `dwh.fait_emploi` | Emploi et chomage | nb_actifs, taux_chomage |
| `dwh.fait_logement` | Logements | nb_logements, taux_surpeuplement |
| `dwh.fait_menages` | Menages | nb_menages, taille_moyenne |

```sql
-- Exemple : Table fait_population
CREATE TABLE dwh.fait_population (
    population_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres vers les dimensions
    temps_id INT NOT NULL REFERENCES dwh.dim_temps(temps_id),
    geo_id INT NOT NULL REFERENCES dwh.dim_geographie(geo_id),
    demo_id INT NOT NULL REFERENCES dwh.dim_demographie(demo_id),

    -- Mesures (les valeurs numeriques)
    population FLOAT,
    population_hommes FLOAT,
    population_femmes FLOAT,

    -- Metadata
    source_fichier NVARCHAR(255),
    date_chargement DATETIME2 DEFAULT GETDATE()
);
```

---

### 004_populate_dimensions.sql

**Role** : Inserer les donnees de reference dans les dimensions.

Ce script insere les valeurs "statiques" qui ne changent pas souvent :

- **dim_temps** : Les annees de reference (2010-2024)
- **dim_geographie** : Les 5 departements des Hauts-de-France
- **dim_demographie** : Les codes sexe (M, F), tranches d'age, PCS
- **dim_activite** : Les sections NAF (Agriculture, Industrie, Services...)
- **dim_indicateur** : Les codes des indicateurs INSEE (revenus, pauvrete...)

```sql
-- Exemple : Insertion des departements
INSERT INTO dwh.dim_geographie (departement_code, departement_nom, region_nom)
VALUES
    ('02', 'Aisne', 'Hauts-de-France'),
    ('59', 'Nord', 'Hauts-de-France'),
    ('60', 'Oise', 'Hauts-de-France'),
    ('62', 'Pas-de-Calais', 'Hauts-de-France'),
    ('80', 'Somme', 'Hauts-de-France');
```

---

### 005_create_datamarts.sql

**Role** : Creer les vues agregees pour faciliter le reporting.

Les **datamarts** sont des vues SQL qui pre-calculent les agregations courantes :

| Vue | Description | Utilisation |
|-----|-------------|-------------|
| `dm.vm_demographie_departement` | Population et evenements par departement | Tableaux demographiques |
| `dm.vm_entreprises_departement` | Creations d'entreprises par departement | Analyses economiques |
| `dm.vm_revenus_departement` | Revenus et pauvrete par departement | Analyses sociales |
| `dm.vm_synthese_territoriale` | Vue globale multi-indicateurs | Tableaux de bord |

```sql
-- Exemple : Vue datamart demographie
CREATE VIEW dm.vm_demographie_departement AS
SELECT
    t.annee,
    g.departement_nom,
    SUM(p.population) AS population_totale,
    SUM(e.naissances) AS naissances,
    SUM(e.deces) AS deces,
    -- Taux de natalite pour 1000 habitants
    SUM(e.naissances) / SUM(p.population) * 1000 AS taux_natalite
FROM dwh.fait_population p
JOIN dwh.dim_temps t ON p.temps_id = t.temps_id
JOIN dwh.dim_geographie g ON p.geo_id = g.geo_id
LEFT JOIN dwh.fait_evenements_demo e ON ...
GROUP BY t.annee, g.departement_nom;
```

---

### 006_configure_security.sql

**Role** : Configurer les roles et permissions d'acces.

- Creation de roles (lecteur, analyste, admin)
- Attribution des permissions sur les schemas
- Configuration des acces pour Power BI

---

### 007_configure_performance.sql

**Role** : Optimiser les performances des requetes.

- Creation d'index supplementaires
- Configuration des statistiques
- Optimisation pour les requetes frequentes

---

## 4. Description des scripts ETL Python

Les scripts ETL sont dans le dossier `analytics/etl/`.

### run_etl.py (Orchestrateur principal)

**Role** : Orchestrer l'execution complete du pipeline ETL.

```
run_etl.py
    |
    +-- Etape 1: STAGING (export_to_sql.py)
    |       Charge les fichiers CSV/Parquet du Data Lake vers les tables stg.*
    |
    +-- Etape 2: DIMENSIONS (load_dimensions.py)
    |       Alimente les tables dwh.dim_* avec les donnees de reference
    |
    +-- Etape 3: FAITS (load_facts.py)
    |       Transforme et charge les donnees dans les tables dwh.fait_*
    |
    +-- Etape 4: REFRESH (sp_updatestats)
            Met a jour les statistiques pour optimiser les requetes
```

---

### load_dimensions.py

**Role** : Charger les dimensions avec les donnees dynamiques.

Ce script complete les dimensions avec les donnees qui viennent des sources :
- Charge les communes depuis le fichier `communes.json` (API geo.api.gouv.fr)
- Cree les mappings entre codes et libelles

---

### load_facts.py

**Role** : Transformer et charger les tables de faits.

Pour chaque table de faits, le script :

1. **Lit** les donnees de staging (`stg.*`)
2. **Mappe** les cles etrangeres vers les dimensions
3. **Transforme** les donnees (calculs, agregations)
4. **Insere** dans les tables de faits (`dwh.fait_*`)

```python
# Exemple simplifie du processus
def load_fait_population(engine):
    # 1. Lire les donnees de staging
    df = pd.read_sql("SELECT * FROM stg.stg_population", engine)

    # 2. Mapper vers les dimensions
    df['temps_id'] = df['annee'].map(temps_mapping)      # annee -> temps_id
    df['geo_id'] = df['departement'].map(geo_mapping)    # dept -> geo_id
    df['demo_id'] = df['sexe'].map(demo_mapping)         # sexe -> demo_id

    # 3. Inserer dans la table de faits
    df.to_sql('fait_population', engine, schema='dwh', if_exists='append')
```

---

## 5. Avant et apres l'ETL

### AVANT l'ETL : Etat initial

```
DATA LAKE (Azure Storage)                 SQL DATABASE
+---------------------------+             +---------------------------+
| raw/                      |             | (tables vides)            |
|   - communes.json         |             |                           |
|   - population_2021.csv   |             | stg.stg_population: 0     |
|   - entreprises.parquet   |             | dwh.fait_population: 0    |
|   - filosofi.csv          |             | dwh.dim_temps: 0          |
+---------------------------+             +---------------------------+

Problemes :
- Donnees brutes non exploitables directement
- Pas de liens entre les fichiers
- Formats heterogenes (CSV, JSON, Parquet)
- Impossible de faire des analyses croisees
```

### Execution de l'ETL

```
ETAPE 1: STAGING
================
Data Lake (CSV/Parquet) ----> stg.stg_population (1578 lignes)
                         ----> stg.stg_entreprises (10756 lignes)
                         ----> stg.stg_filosofi (100 lignes)

ETAPE 2: DIMENSIONS
===================
Donnees de reference ----> dwh.dim_temps (14 lignes)
Fichier communes     ----> dwh.dim_geographie (5 departements + communes)
Codes INSEE          ----> dwh.dim_demographie (18 lignes)
Codes NAF            ----> dwh.dim_activite (13 lignes)

ETAPE 3: FAITS
==============
stg.stg_population + dimensions ----> dwh.fait_population (1578 lignes)
stg.stg_entreprises + dimensions ---> dwh.fait_entreprises (65 lignes)
stg.stg_filosofi + dimensions ------> dwh.fait_revenus (5 lignes)

ETAPE 4: DATAMARTS
==================
Vues pre-calculees ----> dm.vm_demographie_departement
                    ----> dm.vm_entreprises_departement
                    ----> dm.vm_revenus_departement
```

### APRES l'ETL : Data Warehouse operationnel

```
SQL DATABASE - DATA WAREHOUSE
+----------------------------------------------------------+
| DIMENSIONS (attributs)          | FAITS (mesures)        |
|---------------------------------|------------------------|
| dwh.dim_temps: 14 lignes        | dwh.fait_population    |
|   - 2010, 2015, 2021...         |   - 1578 lignes        |
|                                 |   - population par     |
| dwh.dim_geographie: 5 depts     |     dept/annee/sexe    |
|   - Aisne, Nord, Oise...        |                        |
|                                 | dwh.fait_entreprises   |
| dwh.dim_demographie: 18 lignes  |   - 65 lignes          |
|   - Masculin, Feminin           |   - creations par      |
|   - Tranches d'age              |     secteur/annee      |
|   - PCS                         |                        |
|                                 | dwh.fait_revenus       |
| dwh.dim_activite: 13 lignes     |   - 5 lignes           |
|   - Agriculture, Industrie...   |   - revenus par dept   |
+----------------------------------------------------------+

DATAMARTS (vues pre-calculees)
+----------------------------------------------------------+
| dm.vm_demographie_departement                            |
|   -> Population + naissances + deces par departement     |
|                                                          |
| dm.vm_entreprises_departement                            |
|   -> Creations d'entreprises par secteur et departement  |
|                                                          |
| dm.vm_revenus_departement                                |
|   -> Revenus et taux de pauvrete par departement         |
+----------------------------------------------------------+

Avantages :
+ Donnees centralisees et normalisees
+ Requetes analytiques rapides
+ Historique conserve
+ Jointures facilitees
+ Pret pour Power BI
```

### Exemple de requete AVANT vs APRES

**AVANT** (donnees brutes) - Impossible ou tres complexe :
```
Comment obtenir la population du Nord en 2021 avec le taux de natalite ?
-> Ouvrir le CSV population
-> Ouvrir le CSV naissances
-> Faire correspondre manuellement les departements
-> Calculer a la main...
```

**APRES** (Data Warehouse) - Simple et rapide :
```sql
SELECT
    departement_nom,
    population_totale,
    naissances,
    taux_natalite
FROM dm.vm_demographie_departement
WHERE annee = 2021 AND departement_nom = 'Nord';
```

---

## 6. Commandes d'execution

### Prerequis

```bash
# Installer les dependances Python
pip install pyodbc pandas sqlalchemy azure-storage-blob

# Verifier que ODBC Driver 18 est installe
# Telecharger depuis : https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server
```

### Etape 1 : Deployer la structure du Data Warehouse

```bash
# Se placer dans le dossier terraform/sql
cd "D:\data eng\Projet-Data-ENG - v0\terraform\sql"

# Deployer les schemas et tables
python deploy_dwh.py --tfvars "D:\data eng\Projet-Data-ENG - v0\terraform\terraform.tfvars"
```

Cette commande execute les 7 scripts SQL dans l'ordre :
1. Cree les schemas (stg, dwh, dm, analytics)
2. Cree les tables de dimensions
3. Cree les tables de faits
4. Peuple les dimensions avec les donnees de reference
5. Cree les datamarts (vues)
6. Configure la securite
7. Optimise les performances

### Etape 2 : Executer le pipeline ETL complet

```bash
# Se placer dans le dossier analytics/etl
cd "D:\data eng\Projet-Data-ENG - v0\analytics\etl"

# Executer le pipeline complet
python run_etl.py --full \
    --server sqlelbrek-prod2.database.windows.net \
    --database projet_data_eng \
    --user sqladmin \
    --password "VotreMotDePasse"
```

### Options du pipeline ETL

```bash
# Pipeline complet (staging + dimensions + faits + refresh)
python run_etl.py --full

# Seulement le staging (charger les donnees brutes)
python run_etl.py --staging

# Seulement les dimensions
python run_etl.py --dimensions

# Seulement les faits
python run_etl.py --facts

# Seulement rafraichir les statistiques
python run_etl.py --refresh
```

### Etape 3 : Verifier les donnees

```bash
# Verifier le contenu des tables
python -c "
import pyodbc
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=sqlelbrek-prod2.database.windows.net;'
    'DATABASE=projet_data_eng;'
    'UID=sqladmin;PWD=VotreMotDePasse;'
    'Encrypt=yes;TrustServerCertificate=yes;'
)
cursor = conn.cursor()

# Compter les lignes dans chaque table
tables = [
    'dwh.dim_temps', 'dwh.dim_geographie', 'dwh.dim_demographie',
    'dwh.fait_population', 'dwh.fait_entreprises', 'dwh.fait_revenus'
]
for table in tables:
    cursor.execute(f'SELECT COUNT(*) FROM {table}')
    print(f'{table}: {cursor.fetchone()[0]} lignes')

conn.close()
"
```

### Commande complete (tout en une fois)

```bash
# 1. Deployer la structure
cd "D:\data eng\Projet-Data-ENG - v0\terraform\sql"
python deploy_dwh.py --tfvars "D:\data eng\Projet-Data-ENG - v0\terraform\terraform.tfvars"

# 2. Executer l'ETL
cd "D:\data eng\Projet-Data-ENG - v0\analytics\etl"
python run_etl.py --full --server sqlelbrek-prod2.database.windows.net --database projet_data_eng --user sqladmin --password "ChangeM3!Please"
```

---

## 7. Schema de la base de donnees

### Diagramme des tables

```
+-------------------+       +--------------------+       +-------------------+
|   dwh.dim_temps   |       | dwh.fait_population|       | dwh.dim_geographie|
|-------------------|       |--------------------|       |-------------------|
| temps_id (PK)     |<------| temps_id (FK)      |------>| geo_id (PK)       |
| annee             |       | geo_id (FK)        |       | departement_code  |
| trimestre         |       | demo_id (FK)       |       | departement_nom   |
| mois              |       | population         |       | region_code       |
| libelle_periode   |       | population_hommes  |       | commune_code      |
+-------------------+       | population_femmes  |       | commune_nom       |
                            +--------------------+       +-------------------+
                                    |
                                    v
                            +-------------------+
                            |dwh.dim_demographie|
                            |-------------------|
                            | demo_id (PK)      |
                            | sexe_code         |
                            | sexe_libelle      |
                            | age_code          |
                            | pcs_code          |
                            +-------------------+
```

### Liste complete des tables

#### Schema STG (Staging)
| Table | Description |
|-------|-------------|
| stg.stg_population | Donnees brutes population |
| stg.stg_creation_entreprises | Donnees brutes entreprises |
| stg.stg_filosofi | Donnees brutes revenus |
| stg.stg_naissances | Donnees brutes naissances |
| stg.stg_deces | Donnees brutes deces |

#### Schema DWH (Data Warehouse)
| Table | Type | Description |
|-------|------|-------------|
| dwh.dim_temps | Dimension | Periodes temporelles |
| dwh.dim_geographie | Dimension | Territoires |
| dwh.dim_demographie | Dimension | Caracteristiques demographiques |
| dwh.dim_activite | Dimension | Secteurs d'activite |
| dwh.dim_indicateur | Dimension | Indicateurs statistiques |
| dwh.fait_population | Fait | Mesures de population |
| dwh.fait_evenements_demo | Fait | Naissances et deces |
| dwh.fait_entreprises | Fait | Creations d'entreprises |
| dwh.fait_revenus | Fait | Revenus et pauvrete |

#### Schema DM (Datamarts)
| Vue | Description |
|-----|-------------|
| dm.vm_demographie_departement | Synthese demographique par departement |
| dm.vm_entreprises_departement | Synthese economique par departement |
| dm.vm_revenus_departement | Synthese revenus par departement |

---

## Resume

Le Data Warehouse transforme des donnees brutes dispersees en une base analytique structuree :

1. **Structure** : Schema en etoile (dimensions + faits)
2. **Scripts SQL** : Creent la structure et les donnees de reference
3. **Scripts ETL** : Chargent et transforment les donnees
4. **Datamarts** : Fournissent des vues pre-calculees pour le reporting

Le resultat est une base de donnees optimisee pour l'analyse, prete a etre connectee a Power BI ou a tout autre outil de Business Intelligence.
