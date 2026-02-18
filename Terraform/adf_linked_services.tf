############################################
# E5 - Azure Data Factory Linked Services
# Connexions aux sources de donnees
############################################

# ============================================
# LINKED SERVICE : Azure Data Lake Storage Gen2
# Connexion au Data Lake (zones raw, staging, curated)
# ============================================

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls_datalake"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = "https://${azurerm_storage_account.datalake.name}.dfs.core.windows.net"

  # Authentification via Managed Identity
  use_managed_identity = true

  description = "Connexion au Data Lake ADLS Gen2 - zones raw, staging, curated"
}

# ============================================
# LINKED SERVICE : Azure SQL Database (DWH)
# Connexion a l'entrepot de donnees
# ============================================

resource "azurerm_data_factory_linked_service_azure_sql_database" "dwh" {
  name              = "ls_azure_sql_dwh"
  data_factory_id   = azurerm_data_factory.adf.id

  # Connection string avec reference au Key Vault pour le mot de passe
  connection_string = "Server=tcp:${azurerm_mssql_server.sql.fully_qualified_domain_name},1433;Database=${azurerm_mssql_database.sql.name};User ID=${var.sql_admin_login};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"

  # Mot de passe stocke dans Key Vault
  key_vault_password {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.kv.name
    secret_name         = "sql-admin-password"
  }

  description = "Connexion a l'entrepot de donnees Azure SQL"

  depends_on = [
    azurerm_data_factory_linked_service_key_vault.kv,
    azurerm_key_vault_secret.sql_password
  ]
}

# ============================================
# LINKED SERVICE : Key Vault
# Stockage securise des secrets
# ============================================

resource "azurerm_data_factory_linked_service_key_vault" "kv" {
  name            = "ls_key_vault"
  data_factory_id = azurerm_data_factory.adf.id
  key_vault_id    = azurerm_key_vault.kv.id

  description = "Connexion au Key Vault pour les secrets"
}

# ============================================
# SECRET : Mot de passe SQL dans Key Vault
# ============================================

resource "azurerm_key_vault_secret" "sql_password" {
  name         = "sql-admin-password"
  value        = var.sql_admin_password
  key_vault_id = azurerm_key_vault.kv.id

  content_type = "password"

  tags = {
    environment = "production"
    purpose     = "dwh-connection"
  }
}

# ============================================
# SECRET : Connection string ADLS dans Key Vault
# ============================================

resource "azurerm_key_vault_secret" "adls_connection_string" {
  name         = "adls-connection-string"
  value        = azurerm_storage_account.datalake.primary_connection_string
  key_vault_id = azurerm_key_vault.kv.id

  content_type = "connection-string"

  tags = {
    environment = "production"
    purpose     = "datalake-connection"
  }
}

# ============================================
# DATASETS ADF : Sources de donnees
# ============================================

# Dataset pour les fichiers CSV dans le Data Lake
resource "azurerm_data_factory_dataset_delimited_text" "csv_source" {
  name                = "ds_csv_raw"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.adls.name

  azure_blob_fs_location {
    file_system = "raw"
    path        = "csv"
  }

  column_delimiter      = ","
  row_delimiter         = "\n"
  first_row_as_header   = true
  quote_character       = "\""
  escape_character      = "\\"
  encoding              = "UTF-8"

  description = "Dataset pour les fichiers CSV sources dans la zone raw"
}

# Dataset pour les fichiers JSON (communes)
resource "azurerm_data_factory_dataset_json" "json_source" {
  name                = "ds_json_raw"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.adls.name

  azure_blob_storage_location {
    container = "raw"
    path      = "geo"
    filename  = "communes.json"
  }

  encoding = "UTF-8"

  description = "Dataset pour le fichier communes.json"
}

# Dataset pour les tables SQL du DWH
# Remplacement par la ressource compatible azurerm 3.46 (dataset SQL Server)
resource "azurerm_data_factory_dataset_sql_server_table" "dwh_tables" {
  name                = "ds_sql_dwh"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_azure_sql_database.dwh.name

  # Dataset generique pointant sur une table existante du DWH
  table_name = "dwh.dim_temps"

  description = "Dataset generique pour les tables du Data Warehouse"
}

# ============================================
# OUTPUTS pour les connexions
# ============================================

output "adf_linked_service_adls" {
  value       = azurerm_data_factory_linked_service_data_lake_storage_gen2.adls.name
  description = "Nom du linked service ADLS"
}

output "adf_linked_service_sql" {
  value       = azurerm_data_factory_linked_service_azure_sql_database.dwh.name
  description = "Nom du linked service SQL"
}

output "key_vault_sql_secret_name" {
  value       = azurerm_key_vault_secret.sql_password.name
  description = "Nom du secret SQL dans Key Vault"
}
