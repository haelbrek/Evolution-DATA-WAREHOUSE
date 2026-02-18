# Architecture & Flux de données

Ce document complète le README. Il décrit la topologie Azure, le cheminement des jeux de données et les composants applicatifs utilisés pour passer d’un fichier brut (CSV/API/scraping) à une table prête pour l’analyse dans Azure SQL Database.

---

## 1. Sources et formats

| Source | Localisation | Format | Commentaire |
|--------|--------------|--------|-------------|
| Jeux CSV (statistiques INSEE, démographie, emploi, logement, etc.) | `uploads/landing/csv/` | CSV | Fichiers récupérés manuellement ou par scraping |
| Données API communes | `data/communes.json` (local) et `raw/geo/communes.json` (ADLS) | JSON | Généré via `ingestion/fetch_communes.py` |
| Exports additionnels | `uploads/landing/excel/` | XLSX | Optionnel, non exploité dans la préparation actuelle |

Toutes les sources convergent vers le Data Lake (filesystem `raw`). Terraform peut téléverser automatiquement le contenu du dossier `uploads/landing/`.

---

## 2. Composants Azure provisionnés

| Ressource Azure | Terraform | Rôle |
|-----------------|-----------|------|
| Resource Group | `azurerm_resource_group.rg` | Regroupe l'ensemble des ressources |
| Storage Account ADLS Gen2 | `azurerm_storage_account.datalake` | Stockage hiérarchique des zones `raw/staging/curated` |
| Filesystems ADLS | `azurerm_storage_data_lake_gen2_filesystem.zones` | `raw`, `staging`, `curated` (créés automatiquement) |
| Upload local | `azurerm_storage_blob.uploaded` | Synchronise `uploads/landing/` vers ADLS si activé |
| Script API | `null_resource.run_fetch_communes` | Exécute `ingestion/fetch_communes.py` lors de `terraform apply` (optionnel) |
| Key Vault | `azurerm_key_vault.kv` | Stockage de secrets, purge protection activée |
| Data Factory | `azurerm_data_factory.adf` | Point d’entrée pour orchestrations futures |
| Azure SQL Server | `azurerm_mssql_server.sql` | Conteneur logique pour la base relationnelle |
| Azure SQL Database | `azurerm_mssql_database.sql` | Base `projet_data_eng` accueillant les tables préparées |
| Règles firewall SQL | `azurerm_mssql_firewall_rule.sql` | Autorise l’adresse IP du poste de travail ou des services |
| API FastAPI (optionnel) | Déployée manuellement (App Service / conteneur) | Expose les tables SQL en REST (`analytics/api/`) |

Sorties Terraform utilisées par les scripts :

```text
- datalake_primary_connection_string
- datalake_dfs_endpoint
- sql_server_fqdn / sql_server_name / sql_database_name
```

---

## 3. Flux de données détaillé

Le diagramme complet du flux de données est disponible en **[Annexe A](#annexe-a--diagramme-du-flux-de-données)**.

**Résumé** : Les sources (CSV, API) transitent par Terraform vers ADLS (`raw`), puis sont transformées par `data_prep.py` avant d'être chargées dans Azure SQL via `export_to_sql.py`. L'API FastAPI expose ensuite les données.

Étapes :

1. **Collecte** : dépôt des CSV dans `uploads/landing/csv/` et exécution du script API pour `communes.json`.
2. **Atterrissage** : `terraform apply` synchronise `uploads/landing/` et peut exécuter l’ingestion API (via `run_fetch_communes=true`). Les fichiers se retrouvent sous `abfss://raw/<chemin local>`.
3. **Préparation** :
   - Notebook `analytics/notebooks/data_preparation.ipynb` pour l’exploration interactive.
   - Module `analytics.lib.data_prep` pour des traitements automatisés (utilisé par `export_to_sql.py`).
   - Normalisations réalisées : renommage de colonnes (`TableSpec`), parsing des identifiants GEO, conversion en numérique, zfill sur codes, extraction des codes postaux.
4. **Publication** :
   - `analytics/export_to_sql.py` lit les paramètres SQL (tfvars, env, CLI), teste le driver ODBC (`ODBC 18`, `ODBC 17`, `SQL Native Client 11.0`) et charge les tables par lots (`chunksize` configurable, `replace` + `append`).
   - Les tables cibles sont créées dans le schéma `dbo` (modifiable).

---

## 4. Tables préparées

| Table | Description | Particularités |
|-------|-------------|----------------|
| `stg_population` | Population par PCS, sexe, tranche d’âge | Enrichie des colonnes `geo_reference_year`, `geo_level_code`, `geo_code` |
| `stg_creation_entreprises` | Créations d’entreprises (activité, forme juridique) | `creation_count` numérique |
| `stg_creation_entrepreneurs_individuels` | Créations EI selon sexe/activité | Colonnes `age_group`, `creation_count` |
| `stg_deces` / `stg_naissances` | Statistiques vitales annuelles | Colonnes `event_code`, `year`, `departement_code` |
| `stg_ds_filosofi`, `stg_filosofi_age_tp_nivvie` | Indicateurs socio-économiques | Codes indicateurs normalisés |
| `stg_emploi_chomage`, `stg_logement`, `stg_menage`, `stg_fecondite` | Indicateurs thématiques | Conversion numérique automatique |
| `dim_commune` | Dimension commune (population, surface, codes INSEE) | Nettoyage des codes postaux en chaîne `comma-separated` |
| `dim_commune_geojson` | Contours géographiques GeoJSON | Vide si le JSON ne contient pas de géométrie |
| `bridge_commune_code_postal` | Table de correspondance (commune ↔ code postal) | Générée via `explode` sur `codes_postaux` |

Toutes les tables sont chargées dans Azure SQL via `to_sql`. Les colonnes textuelles restent en `NVARCHAR`, les mesures en `FLOAT` (nullable).

---

## 5. Orchestration et scripts

| Script / notebook | Rôle | Commande principale |
|-------------------|------|---------------------|
| `ingestion/fetch_communes.py` | Récupère et charge le JSON des communes (API geo.api.gouv.fr) | `python ingestion/fetch_communes.py --connection-string ... --container raw` |
| `analytics/data_loader.py` | Liste/télécharge les blobs ADLS en local | `python analytics/data_loader.py list --csv-prefix csv/` |
| `analytics/notebooks/data_preparation.ipynb` | Préparation interactive des tables | Exécution dans VS Code/Jupyter |
| `analytics/lib/data_prep.py` | Fonctions de transformation réutilisables | `from analytics.lib.data_prep import prepare_tables` |
| `analytics/export_to_sql.py` | Charge les tables préparées dans Azure SQL | `python analytics/export_to_sql.py --preview` / `python analytics/export_to_sql.py` |

---

## 6. Sécurité & bonnes pratiques

- **Secrets** : ne jamais commiter `terraform.tfvars` ni les sorties contenant des credentials. Utiliser Key Vault pour stocker les chaînes sensibles.
- **ODBC** : installer le driver 18 (ou 17). Sur Windows, on peut vérifier via `Get-OdbcDriver`.
- **Réseau** : définir des règles firewall précises (`sql_firewall_rules`). Désactiver `sql_allow_azure_services` si non nécessaire.
- **Data Lake** : l’upload Terraform ne traite que les extensions `.csv`/`.xlsx`. Ajouter des fichiers JSON si besoin via `azcopy` ou `data_loader.py`.
- **Nettoyage** : `terraform destroy` supprime l’infrastructure, mais pas les données locales (`uploads/`, `data/`).

---

## 7. Dépannage

| Problème | Cause probable | Solution |
|----------|----------------|----------|
| `ContainerNotFound` lors de l’upload | Filesystem `raw` non créé | Vérifier que l’account ADLS est provisionné (`terraform apply`) ou créer le container via Azure Portal |
| `IM002` driver ODBC introuvable | Driver non installé ou PATH invalide | Installer `ODBC Driver 18 for SQL Server`, ouvrir un nouveau terminal |
| `07002 Champ COUNT incorrect` | Trop de paramètres dans un insert multi | Utiliser la version actuelle de `export_to_sql.py` (insertion par lots maîtrisée) |
| `ImportError: analytics` | Conflit avec un paquet pip nommé `analytics` | Le script force maintenant l’insertion du dossier projet dans `sys.path` |
| `KeyVault VaultAlreadyExists` | Nom de coffre déjà pris | Choisir un nom unique (`kvelbrek-prod-kv` par exemple) ou purger l’ancien coffre |
| API : `HYT00` timeout SQL | Temps de réponse trop long | Augmenter `chunksize`, optimiser les requêtes, vérifier les indexes |

---

## 8. Évolutions possibles

- Ajouter une couche `staging` <-> `curated` dans ADLS avec des jobs Data Factory / Synapse.  
- Déployer un jeu de vues SQL (`dbo.v_population`, etc.) pour faciliter la consommation BI.  
- Mettre en place une pipeline CI/CD Terraform + tests des scripts.  
- Ajouter des validations de schémas (ex : `pandera`) avant export.
- Industrialiser l’API (authentification JWT, caching, monitoring Application Insights).

---

_Dernière mise à jour : 2025-10-29_ (adapter manuellement lors des prochaines modifications).

---

# Annexes

## Annexe A : Diagramme du flux de données

```
Sources locales/API  -->  uploads/landing/* et data/communes.json
      |                              |
      +--> Terraform (upload_files_enabled=true) -----------------------------+
                                                                              |
                                                       +--> analytics/data_loader.py (lecture/sauvegarde locale)
                                                       |
                             Azure Data Lake Storage (filesystem raw)
                                                       |
                                                       +--> analytics/lib/data_prep.py
                                                            (normalisation, enrichissements géographiques,
                                                             création des tables stg_* et dim_*)
                                                       |
                                                       +--> analytics/export_to_sql.py
                                                            (chargement par lots vers Azure SQL Database)
                                                                              |
                                                Azure SQL Database (dbo.stg_*, dbo.dim_*)
                                                       |
                                                       +--> API FastAPI (`analytics/api/`)
                                                             - Endpoints `/tables/...`
                                                             - Connexion SQL avec utilisateur dédié
```
