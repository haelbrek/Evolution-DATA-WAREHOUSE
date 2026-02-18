# API Projet-Data-ENG

API FastAPI qui expose les tables Azure SQL générées par la préparation de données du projet.

## Installation locale

`powershell
cd analytics/api
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python generate_env.py  # crée analytics/api/.env avec les valeurs Terraform si disponibles
uvicorn analytics.api.app.main:app --reload --port 8000
`

Endpoints principaux :
- GET /health : statut simple
- GET /tables/summary : description des tables préparées
- GET /tables/{table_name}?limit=100 : extrait les données d’une table autorisée

Les paramètres SQL sont lus selon la priorité suivante : .env > variables d’environnement > defaults.

## Déploiement Azure App Service (exemple)

1. Crée un App Service plan Linux :
   `powershell
   az group create --name rg-elbrek-infra --location francecentral
   az appservice plan create --name plan-projet-data --resource-group rg-elbrek-infra --sku B1 --is-linux
   `
2. Crée le webapp :
   `powershell
   az webapp create --name proj-data-api --resource-group rg-elbrek-infra \
     --plan plan-projet-data --runtime "PYTHON|3.10" --deployment-local-git
   `
3. Définis les variables d’environnement SQL :
   `powershell
   az webapp config appsettings set --name proj-data-api --resource-group rg-elbrek-infra --settings \
     AZURE_SQL_SERVER="sqlelbrek-prod.database.windows.net" \
     AZURE_SQL_DATABASE="projet_data_eng" \
     AZURE_SQL_USERNAME="sqladmin" \
     AZURE_SQL_PASSWORD="<motdepasse>" \
     ALLOWED_TABLES="stg_population,dim_commune,..."
   `
4. Pour les drivers ODBC, ajoute un script de startup (startup.sh) :
   `ash
   #!/usr/bin/env bash
   apt-get update && apt-get install -y curl apt-transport-https
   curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
   curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
   apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
   uvicorn analytics.api.app.main:app --host 0.0.0.0 --port 
   `
   Puis :
   `powershell
   az webapp config set --resource-group rg-elbrek-infra --name proj-data-api \
     --startup-file "bash startup.sh"
   `
5. Déploie le code (git push, zip deploy, etc.).

## Sécurité

- Restreindre les tables exposées via ALLOWED_TABLES
- Utiliser HTTPS, configurer une authentification (token, API Key, etc.) si l’API est publique
- Stocker les secrets dans Azure Key Vault + App Service Managed Identity
