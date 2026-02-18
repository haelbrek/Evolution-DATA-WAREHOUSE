# Deploiement E6 - Commandes

## Prerequis

    - Python 3.x avec pandas, sqlalchemy, pyodbc
    - ODBC Driver 18 for SQL Server
    - Terraform (pour l'infrastructure)
    - Azure CLI (az) authentifie (`az login`)
    - XeLaTeX (pour le rapport)

Remplacer `<mot_de_passe>` par le mot de passe SQL Azure,
ou definir les variables d'environnement :

```bash
export AZURE_SQL_PASSWORD="<mot_de_passe>"
export AZURE_RESOURCE_GROUP="<resource_group>"
export AZURE_STORAGE_ACCOUNT="<storage_account>"
```

---

## Ordre de deploiement

```
Etape 1 : Terraform       Cree le serveur SQL, la base, le firewall, le Data Lake
    |
    v
Etape 2 : Scripts SQL      Cree les schemas, tables, vues, procedures dans la base
    |
    v
Etape 3 : Pipeline ETL     Remplit les tables (dimensions, faits, backup BACPAC)
    |
    v
Etape 4 : Tests             Verifie que tout est deploye correctement
    |
    v
Etape 5 : Rapport LaTeX     Compile le rapport PDF
```

Chaque etape depend de la precedente. Ne pas changer l'ordre.

---

## Etape 1 : Deployer l'infrastructure Terraform

Terraform cree le serveur SQL, la base de donnees, le Data Lake ADLS Gen2, les regles firewall et la retention des backups.
Cette etape doit etre faite EN PREMIER, sinon les scripts SQL et ETL n'auront pas de serveur.

```bash
cd "D:\data eng\Projet-Data-ENG - E6\Terraform"
terraform init
terraform plan
terraform apply
```

Si le firewall bloque votre IP, ajoutez-la manuellement :

```bash
az sql server firewall-rule create \
    --resource-group <resource_group> \
    --server sqlelbrek-prod2 \
    --name "MonIP" \
    --start-ip-address 80.215.227.45 \
    --end-ip-address 80.215.227.45
```

---

## Etape 2 : Deployer les scripts SQL

Les scripts creent les schemas, tables, procedures stockees et vues dans la base.
A executer dans l'ordre numerique (001 a 010) :

```bash
cd "D:\data eng\Projet-Data-ENG - E6\Terraform\sql"
python deploy_dwh.py
```

Ou manuellement via sqlcmd (dans l'ordre) :

```bash
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 001_create_schemas.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 002_create_dimensions.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 003_create_facts.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 004_populate_dimensions.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 005_create_datamarts.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 006_configure_security.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 007_configure_performance.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 008_configure_logging.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 009_configure_backup.sql
sqlcmd -S sqlelbrek-prod2.database.windows.net -d projet_data_eng -U sqladmin -P <mot_de_passe> -i 010_scd_dimensions.sql
```

---

## Etape 3 : Lancer le pipeline ETL

L'ETL charge les donnees dans les tables creees a l'etape 2, puis exporte un backup BACPAC vers le Data Lake.

```bash
cd "D:\data eng\Projet-Data-ENG - E6\analytics\etl"

# Pipeline complet (etapes 1 a 5 : staging, dimensions, faits, refresh, backup)
python run_etl.py --full \
    --server sqlelbrek-prod2.database.windows.net \
    --database projet_data_eng \
    --user sqladmin \
    --password "<mot_de_passe>" \
    --storage-account <storage_account> \
    --resource-group <resource_group>
```

Ou etape par etape :

```bash
python run_etl.py --staging       # 1. Charger les sources vers staging
python run_etl.py --dimensions    # 2. Dimensions avec detection SCD
python run_etl.py --facts         # 3. Faits (emploi, menages)
python run_etl.py --refresh       # 4. Rafraichir statistiques et vues
python run_etl.py --backup \
    --storage-account <storage_account> \
    --resource-group <resource_group>   # 5. Backup BACPAC vers Data Lake
```

Le backup genere un fichier horodate dans le Data Lake :
`raw/backups/projet_data_eng_YYYYMMDD_HHMMSS.bacpac`

---

## Etape 4 : Lancer les tests

Verifie que tout a ete deploye correctement.

```bash
cd "D:\data eng\Projet-Data-ENG - E6\analytics\tests"
python test_dwh.py
```

Resultat attendu : 49 tests (17 E5 + 32 E6), 0 echecs.

---

## Etape 5 : Compiler le rapport LaTeX

```bash
cd "D:\data eng\Projet-Data-ENG - E6\rapports\E6_Evolution_Entrepot"
xelatex rapport_E6.tex
xelatex rapport_E6.tex
```

La 2eme passe genere la table des matieres.
