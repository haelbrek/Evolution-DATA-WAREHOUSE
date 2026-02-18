variable "resource_group_name" {
  type        = string
  description = "Nom du groupe de ressources Azure"
}

variable "resource_group_location" {
  type        = string
  description = "RÃƒÂ©gion Azure oÃƒÂ¹ le groupe de ressources est crÃƒÂ©ÃƒÂ©"
}

variable "datalake_storage_account_name" {
  type        = string
  description = "Nom du compte ADLS Gen2 (3-24, minuscules et chiffres)"
  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.datalake_storage_account_name))
    error_message = "Le nom du Storage Account doit ÃƒÂªtre 3-24 caractÃƒÂ¨res alphanumÃƒÂ©riques minuscules."
  }
}

variable "datalake_filesystems" {
  type        = list(string)
  description = "Liste des filesystems ADLS Gen2 (zones)"
  default     = ["raw", "staging", "curated"]
}

# Key Vault
variable "key_vault_name" {
  type        = string
  description = "Nom du Key Vault (3-24, lettres/chiffres et tirets, commence/finit par alphanum)"
  validation {
    condition     = can(regex("^[a-zA-Z0-9]([a-zA-Z0-9-]{1,22}[a-zA-Z0-9])$", var.key_vault_name))
    error_message = "Le nom du Key Vault doit faire 3-24 caractÃƒÂ¨res, alphanumÃƒÂ©riques et tirets, sans commencer/finir par un tiret."
  }
}

variable "kv_additional_reader_object_ids" {
  type        = list(string)
  description = "(Optionnel) Liste d'Object IDs (Azure AD) ÃƒÂ  autoriser en lecture de secrets (rÃƒÂ´le Secrets User)"
  default     = []
}

# Data Factory
variable "data_factory_name" {
  type        = string
  description = "Nom de l'Azure Data Factory"
}

# Upload local -> Data Lake via Terraform (optionnel)
variable "upload_files_enabled" {
  type        = bool
  description = "Activer l'upload de fichiers locaux vers le Data Lake"
  default     = false
}

variable "upload_source_dir" {
  type        = string
  description = "RÃƒÂ©pertoire local (relatif ÃƒÂ  Terraform/) contenant les fichiers ÃƒÂ  uploader"
  default     = "../uploads/landing"
}

variable "upload_datalake_filesystem" {
  type        = string
  description = "Filesystem ADLS cible pour l'upload (par defaut: raw)"
  default     = "raw"
}

variable "run_fetch_communes" {
  type        = bool
  description = "Executer le script ingestion/API/fetch_communes.py pendant terraform apply"
  default     = false
}

variable "fetch_communes_extra_args" {
  type        = string
  description = "Arguments supplementaires a passer au script fetch_communes.py (ex: \"--departements 59 62 --geometry centre\")"
  default     = ""
}

# Azure SQL Database
variable "sql_server_name" {
  type        = string
  description = "Nom du serveur Azure SQL (globalement unique, 3-63 caracteres alphanumeriques ou tirets)"
}

variable "sql_admin_login" {
  type        = string
  description = "Login administrateur du serveur Azure SQL"
}

variable "sql_admin_password" {
  type        = string
  description = "Mot de passe admin pour Azure SQL (8-128 caracteres, complexite Azure)"
  sensitive   = true
}

variable "sql_database_name" {
  type        = string
  description = "Nom de la base de donnees Azure SQL"
}

variable "sql_database_sku_name" {
  type        = string
  description = "SKU / niveau de service de la base Azure SQL (ex: Basic, S0, GP_Gen5_2)"
  default     = "S0"
}

variable "sql_database_collation" {
  type        = string
  description = "Collation de la base Azure SQL"
  default     = "SQL_Latin1_General_CP1_CI_AS"
}

variable "sql_firewall_rules" {
  description = "Regles firewall a appliquer au serveur Azure SQL"
  type = list(object({
    name     = string
    start_ip = string
    end_ip   = string
  }))
  default = []
}

variable "sql_allow_azure_services" {
  type        = bool
  description = "Autoriser Azure services (0.0.0.0) a acceder au serveur SQL"
  default     = true
}

# Databricks
variable "databricks_host" {
  type        = string
  description = "URL du workspace Databricks (ex: https://adb-xxxxxxxx.azuredatabricks.net)"
}

variable "databricks_token" {
  type        = string
  description = "Token PAT Databricks (a fournir via tfvars ou variable d'env)"
  sensitive   = true
}

variable "databricks_notebook_path" {
  type        = string
  description = "Notebook Databricks a executer (ex: /Shared/parquet_etl)"
  default     = "/Shared/parquet_etl"
}

variable "databricks_workspace_name" {
  type        = string
  description = "Nom du workspace Databricks a creer"
}

variable "databricks_managed_rg_name" {
  type        = string
  description = "Nom du resource group gere (managed RG) pour Databricks (si vide, derivé du RG principal)."
  default     = ""
}

# ============================================
# E5 - Data Warehouse
# ============================================
variable "deploy_dwh" {
  type        = bool
  description = "Deployer la structure du Data Warehouse (schemas, dimensions, faits, datamarts)"
  default     = false
}

variable "dwh_force_redeploy" {
  type        = bool
  description = "Forcer le redeploiement du DWH meme si deja deploye"
  default     = false
}

