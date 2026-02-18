# Procedure de creation et chargement des donnees

## 1. Preparation
- Prerequis installes : Terraform 1.5+, Python 3.10+, Azure CLI (optionnel pour verifier).
- Fichier `Terraform/terraform.tfvars` renseigne (resource_group_name, datalake_storage_account_name, sql_*, etc.).
- Fichiers a uploader places dans `uploads/landing/` (CSV/XLSX).

## 2. Provisionner l'infrastructure avec Terraform
Depuis le dossier `Terraform/` :
```powershell
cd Terraform
terraform init
terraform plan
terraform apply --auto-approve
```
Parametres d'upload (dans `terraform.tfvars`) pour pousser automatiquement `uploads/landing/` :
```hcl
upload_files_enabled       = true
upload_source_dir          = "../uploads/landing"
upload_datalake_filesystem = "raw"
```
Apres `apply`, les fichiers de `uploads/landing/` sont copies dans ADLS Gen2 (`raw`).

## 3. Recuperer les communes via le script fetch_communes.py

1) Configurer la variable d'environnement `AZURE_STORAGE_CONNECTION_STRING` (voir **[Annexe A](#annexe-a--configuration-des-variables-denvironnement)**).
   - Recuperer la chaine : Azure Portal > Storage account `adlselbrek` > Access keys > Connection string.

2) Lancer le script depuis `Terraform/` :
   - `python ../ingestion/API/fetch_communes.py --container raw`

Le script recupere les communes depuis l'API geo et deverse un JSON dans `raw`.

**Options** : `--departements 59 62` (filtrer), `--datalake-path` (chemin cible).

## 4. Charger les donnees dans Azure SQL avant d'exposer l'API

1) Lancer l'export SQL : `python analytics/export_to_sql.py`
   - Prerequis : ODBC Driver 18/17, firewall SQL ouvert.

2) Demarrer l'API : `python -m uvicorn analytics.api.app.main:app --reload --port 8000`
   - Endpoints disponibles : `/tables/dim_commune?limit=100`, etc.

## 5. Creer et alimenter la base SQL secondaire (_bis)

### 5.1 Provisionner la base secondaire
Appliquer Terraform pour creer `${sql_database_name}_bis` : `cd Terraform && terraform apply --auto-approve`

### 5.2 Charger les donnees dans la base _bis
Configurer les variables d'environnement SQL (voir **[Annexe B](#annexe-b--commandes-pour-la-base-_bis)**), puis :
- `python analytics/export_to_sql_bis.py`
- Mode preview : `--preview`

### 5.3 Exporter la base _bis vers ADLS en Parquet
- `python analytics/export_to_adls_bis.py --container raw --prefix sql-bis-parquet/`
- Options : `--tables`, `--limit`, `--adls-connection-string`

---

# Annexes

## Annexe A : Configuration des variables d'environnement

**PowerShell** :
```powershell
$env:AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=adlselbrek;AccountKey=...;EndpointSuffix=core.windows.net"
```

**Bash** :
```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=adlselbrek;AccountKey=...;EndpointSuffix=core.windows.net"
```

## Annexe B : Commandes pour la base _bis

**Configuration des variables SQL (PowerShell)** :
```powershell
$env:AZURE_SQL_SERVER = "sqlelbrek-prod.database.windows.net"
$env:AZURE_SQL_DATABASE_BIS = "<nom_base>_bis"   # ex: projet_data_eng_bis
$env:AZURE_SQL_USERNAME = "sqladmin"
$env:AZURE_SQL_PASSWORD = "<motdepasse>"
```

**Configuration des variables SQL (Bash)** :
```bash
export AZURE_SQL_SERVER="sqlelbrek-prod.database.windows.net"
export AZURE_SQL_DATABASE_BIS="<nom_base>_bis"
export AZURE_SQL_USERNAME="sqladmin"
export AZURE_SQL_PASSWORD="<motdepasse>"
```

**Commande d'export** :
```powershell
python analytics/export_to_sql_bis.py
```

**Export vers ADLS en Parquet** :
```powershell
python analytics/export_to_adls_bis.py --container raw --prefix sql-bis-parquet/
```
