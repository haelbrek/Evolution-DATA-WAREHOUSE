############################################
# E5 - Configuration RBAC Azure
# Acces aux ressources pour les equipes
############################################

# ============================================
# VARIABLES POUR LA GESTION DES ACCES
# ============================================

variable "analyst_group_object_id" {
  type        = string
  description = "Object ID du groupe Azure AD des analystes (optionnel)"
  default     = ""
}

variable "etl_service_principal_id" {
  type        = string
  description = "Object ID du service principal pour les ETL (optionnel)"
  default     = ""
}

# ============================================
# ROLE : Storage Blob Data Reader
# Lecture des donnees du Data Lake
# ============================================

# Acces lecture au Data Lake pour Data Factory (Managed Identity)
resource "azurerm_role_assignment" "adf_storage_reader" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id

  description = "Data Factory peut lire les donnees du Data Lake"
}

# Acces lecture/ecriture au Data Lake pour Data Factory
resource "azurerm_role_assignment" "adf_storage_contributor" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id

  description = "Data Factory peut ecrire dans le Data Lake"
}

# ============================================
# ROLE : Acces au Key Vault pour Data Factory
# ============================================

resource "azurerm_role_assignment" "adf_keyvault_reader" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id

  description = "Data Factory peut lire les secrets du Key Vault"
}

# ============================================
# ROLE : Acces pour les analystes (si groupe configure)
# ============================================

resource "azurerm_role_assignment" "analysts_storage_reader" {
  count = var.analyst_group_object_id != "" ? 1 : 0

  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = var.analyst_group_object_id

  description = "Les analystes peuvent lire les donnees du Data Lake (zone curated)"
}

# ============================================
# CONFIGURATION FIREWALL SQL
# Regles d'acces reseau
# ============================================

# Autoriser les services Azure a acceder au SQL Server
resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  count = var.sql_allow_azure_services ? 1 : 0

  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# Autoriser l'IP publique courante pour les connexions SQL (évite le 40615)
resource "azurerm_mssql_firewall_rule" "allow_current_ip" {
  name             = "AllowCurrentPublicIP"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "45.81.84.9"
  end_ip_address   = "45.81.84.9"
}

# ============================================
# OUTPUTS RBAC
# ============================================

output "adf_principal_id" {
  value       = azurerm_data_factory.adf.identity[0].principal_id
  description = "Principal ID de Data Factory pour les acces RBAC"
}

output "storage_account_id" {
  value       = azurerm_storage_account.datalake.id
  description = "ID du compte de stockage Data Lake"
}
