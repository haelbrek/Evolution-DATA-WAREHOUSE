# Projet-Data-ENG

Suite d'outils et d'infrastructure pour collecter, deposer, preparer puis publier des donnees territoriales sur Azure (Data Lake + Azure SQL).

## 1. Vue d'ensemble
- Collecte: CSV dans `uploads/landing/`, API `ingestion/API/fetch_communes.py`, scraping.
- Atterrissage: ADLS Gen2 (`raw`, `staging`, `curated`), upload optionnel via Terraform.
- Preparation: notebooks/analytics (`analytics/notebooks`, `analytics/lib/data_prep.py`).
- Publication: export vers Azure SQL (`analytics/export_to_sql.py`).

## 2. Prerequis
- Terraform 1.5+
- Azure CLI 2.52+
- Python 3.10+
- ODBC Driver 18/17 (pour export SQL)

Installation Python:
```
python -m pip install -r requirements.txt
```

## 3. Provisionner l'infrastructure
```
cd Terraform
terraform init
terraform plan
terraform apply
```
Variables clefs dans `Terraform/terraform.tfvars` (exemple):
```
resource_group_name              = "rg-exemple"
resource_group_location          = "francecentral"
datalake_storage_account_name    = "nomstockage"
key_vault_name                   = "kv-exemple"
data_factory_name                = "adf-exemple"
sql_server_name                  = "sqlexemple"
sql_admin_login                  = "sqladmin"
sql_admin_password               = "MotDePasse!2024"
sql_database_name                = "projet_data_eng"
sql_firewall_rules = [
  { name = "Home", start_ip = "X.X.X.X", end_ip = "X.X.X.X" }
]
```

Sorties utiles:
```
terraform output datalake_primary_connection_string
terraform output sql_server_fqdn
terraform output sql_database_name
```

## 4. Collecter et deposer les donnees
### 4.1 Chargement local -> Data Lake
- Placer les CSV/XLSX dans `uploads/landing/`.
- Activer l'upload auto dans `terraform.tfvars`:
```
upload_files_enabled       = true
upload_source_dir          = "../uploads/landing"
upload_datalake_filesystem = "raw"
```
- `terraform apply` poussera dans `abfss://raw@<storage>.dfs.core.windows.net/`.

### 4.2 Ingestion API communes
Commande generique:
```
python ingestion/API/fetch_communes.py \
  --connection-string "<chaine ADLS>" \
  --departements 02 59 60 62 80 \
  --container raw \
  --local-output data/communes.json
```
Options: `--departements ""` pour tout recuperer, `--datalake-path` pour changer le chemin.

Execution rapide depuis le dossier `Terraform/` (apres recreation du compte, utiliser la nouvelle chaine de connexion) :
```
$env:AZURE_STORAGE_CONNECTION_STRING = "<connection-string-ADLS> (Portal > Storage account adlselbrek > Access keys > Connection string)"
python ..\ingestion\API\fetch_communes.py --container raw
```
En Bash :
```
export AZURE_STORAGE_CONNECTION_STRING="<connection-string-ADLS>"
python ../ingestion/API/fetch_communes.py --container raw
```

### 4.3 Exploration depuis le Data Lake
```
python analytics/data_loader.py list --csv-prefix csv/
python analytics/data_loader.py fetch --csv-prefix csv/ --json-prefix geo/ --save-local
```
`--connection-string` ou variable `AZURE_STORAGE_CONNECTION_STRING`, `--filesystem` (raw/staging/curated).

## 5. Preparation des tables analytiques
- Notebook: `analytics/notebooks/data_preparation.ipynb`
- Module: `analytics/lib/data_prep.py` (fonction `prepare_tables()`)

## 6. Export vers Azure SQL
```
python analytics/export_to_sql.py
```
Lit les creds depuis `terraform.tfvars` ou variables env (`AZURE_SQL_*`).

## 7. API FastAPI (optionnel)
```
$env:PYTHONPATH = "D:\data eng\Projet-Data-ENG"
python -m uvicorn analytics.api.app.main:app --reload --port 8000
```
