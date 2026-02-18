resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.resource_group_location
}

############################################
# Stockage : Data Lake (ADLS Gen2)
############################################

resource "azurerm_storage_account" "datalake" {
  name                      = var.datalake_storage_account_name
  resource_group_name       = azurerm_resource_group.rg.name
  location                  = azurerm_resource_group.rg.location
  account_tier              = "Standard"
  account_replication_type  = "LRS"
  min_tls_version           = "TLS1_2"
  enable_https_traffic_only = true
  is_hns_enabled            = true
  tags = {
    env     = "demo"
    purpose = "datalake"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "zones" {
  for_each           = toset(var.datalake_filesystems)
  name               = each.value
  storage_account_id = azurerm_storage_account.datalake.id
}

# Workspace Databricks (Azure)
resource "azurerm_databricks_workspace" "dbw" {
  name                        = var.databricks_workspace_name
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = "standard"
  managed_resource_group_name = local.databricks_managed_rg_name
}


############################################
# Upload local -> Data Lake (optionnel)
############################################

locals {
  upload_files = var.upload_files_enabled ? [
    for file in fileset("${path.module}/${var.upload_source_dir}", "**")
    : file
    if length(regexall("(?i)\\.(csv|xlsx|parquet)$", file)) > 0
  ] : []
  sql_firewall_rules = var.sql_allow_azure_services ? concat([
    {
      name     = "Allow_Azure_Services"
      start_ip = "0.0.0.0"
      end_ip   = "0.0.0.0"
    }
  ], var.sql_firewall_rules) : var.sql_firewall_rules
  databricks_managed_rg_name = var.databricks_managed_rg_name != "" ? var.databricks_managed_rg_name : "${var.resource_group_name}-dbw-managed"
}

resource "azurerm_storage_blob" "uploaded" {
  for_each               = toset(local.upload_files)
  name                   = each.value
  storage_account_name   = azurerm_storage_account.datalake.name
  storage_container_name = azurerm_storage_data_lake_gen2_filesystem.zones[var.upload_datalake_filesystem].name
  type                   = "Block"
  source                 = "${path.module}/${var.upload_source_dir}/${each.value}"
  content_md5            = filemd5("${path.module}/${var.upload_source_dir}/${each.value}")
}

resource "null_resource" "run_fetch_communes" {
  count = var.run_fetch_communes ? 1 : 0

  triggers = {
    script_hash = filesha256("${path.module}/../ingestion/API/fetch_communes.py")
    args_hash   = md5(var.fetch_communes_extra_args)
    filesystem  = var.upload_datalake_filesystem
  }

  provisioner "local-exec" {
    command     = "python ../ingestion/API/fetch_communes.py --container ${var.upload_datalake_filesystem}${var.fetch_communes_extra_args != "" ? " ${var.fetch_communes_extra_args}" : ""}"
    working_dir = path.module
    environment = {
      AZURE_STORAGE_CONNECTION_STRING = azurerm_storage_account.datalake.primary_connection_string
    }
  }

  depends_on = [
    azurerm_storage_data_lake_gen2_filesystem.zones
  ]
}

############################################
# Key Vault (Access Policies)
############################################

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv" {
  name                = var.key_vault_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  enable_rbac_authorization  = false
  purge_protection_enabled   = true
  soft_delete_retention_days = 90

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = [
      "Get", "List", "Set", "Delete", "Purge", "Recover"
    ]
  }

  dynamic "access_policy" {
    for_each = toset(var.kv_additional_reader_object_ids)
    content {
      tenant_id          = data.azurerm_client_config.current.tenant_id
      object_id          = access_policy.value
      secret_permissions = ["Get", "List"]
    }
  }

  tags = {
    env     = "demo"
    purpose = "secrets"
  }
}

############################################
# Data Factory
############################################

resource "azurerm_data_factory" "adf" {
  name                = var.data_factory_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    env     = "demo"
    purpose = "orchestration"
  }
}

resource "azurerm_key_vault_access_policy" "kv_adf_reader" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_data_factory.adf.identity[0].principal_id

  secret_permissions = ["Get", "List"]
  depends_on         = [azurerm_data_factory.adf]
}

############################################
# Azure SQL Database
############################################

resource "azurerm_mssql_server" "sql" {
  name                = var.sql_server_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  version             = "12.0"

  administrator_login          = var.sql_admin_login
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
  public_network_access_enabled = true

  tags = {
    env     = "demo"
    purpose = "sql"
  }
}

resource "azurerm_mssql_database" "sql" {
  name      = var.sql_database_name
  server_id = azurerm_mssql_server.sql.id
  sku_name  = var.sql_database_sku_name
  collation = var.sql_database_collation

  # E6 - Politique de retention des backups
  short_term_retention_policy {
    retention_days           = 14
    backup_interval_in_hours = 12
  }

  long_term_retention_policy {
    weekly_retention  = "P4W"    # 4 semaines
    monthly_retention = "P12M"   # 12 mois
    yearly_retention  = "P3Y"    # 3 ans
    week_of_year      = 1
  }

  tags = {
    env     = "demo"
    purpose = "sqldb"
  }
}

# Base SQL secondaire (suffixe _bis par defaut)
resource "azurerm_mssql_database" "sql_bis" {
  name      = "${var.sql_database_name}_bis"
  server_id = azurerm_mssql_server.sql.id
  sku_name  = var.sql_database_sku_name
  collation = var.sql_database_collation

  tags = {
    env     = "demo"
    purpose = "sqldb-bis"
  }
}

locals {
  sql_firewall_rule_map = { for rule in local.sql_firewall_rules : rule.name => rule }
}

resource "azurerm_mssql_firewall_rule" "sql" {
  for_each = local.sql_firewall_rule_map

  name                = each.value.name
  server_id           = azurerm_mssql_server.sql.id
  start_ip_address    = each.value.start_ip
  end_ip_address      = each.value.end_ip
}

############################################
# Databricks (cluster + job)
############################################

resource "databricks_cluster" "etl_parquet" {
  cluster_name  = "etl-parquet"
  spark_version = "13.3.x-scala2.12"

  # SKU plus léger pour limiter le risque d’épuisement de quota
  node_type_id = "Standard_DS3_v2"

  autoscale {
    min_workers = 0
    max_workers = 1
  }

  autotermination_minutes = 30
}

resource "databricks_job" "parquet_etl" {
  name = "parquet-etl"

  task {
    task_key            = "parquet_etl_task"
    existing_cluster_id = databricks_cluster.etl_parquet.id
    notebook_task {
      notebook_path = var.databricks_notebook_path
    }
  }
}

############################################
# E5 - Data Warehouse Deployment
############################################

resource "null_resource" "deploy_dwh" {
  count = var.deploy_dwh ? 1 : 0

  triggers = {
    # Redemployer si les scripts SQL changent
    schemas_hash    = filesha256("${path.module}/sql/001_create_schemas.sql")
    dimensions_hash = filesha256("${path.module}/sql/002_create_dimensions.sql")
    facts_hash      = filesha256("${path.module}/sql/003_create_facts.sql")
    populate_hash   = filesha256("${path.module}/sql/004_populate_dimensions.sql")
    datamarts_hash  = filesha256("${path.module}/sql/005_create_datamarts.sql")
    # E6 - Nouveaux scripts
    logging_hash    = filesha256("${path.module}/sql/008_configure_logging.sql")
    backup_hash     = filesha256("${path.module}/sql/009_configure_backup.sql")
    scd_hash        = filesha256("${path.module}/sql/010_scd_dimensions.sql")
    force_redeploy  = var.dwh_force_redeploy ? timestamp() : "stable"
  }

  provisioner "local-exec" {
    command     = "python sql/deploy_dwh.py --tfvars terraform.tfvars"
    working_dir = path.module
    environment = {
      AZURE_SQL_SERVER   = "${var.sql_server_name}.database.windows.net"
      AZURE_SQL_DATABASE = var.sql_database_name
      AZURE_SQL_USER     = var.sql_admin_login
      AZURE_SQL_PASSWORD = var.sql_admin_password
    }
  }

  depends_on = [
    azurerm_mssql_database.sql,
    azurerm_mssql_firewall_rule.sql
  ]
}

  resource "azurerm_mssql_firewall_rule" "mon_ip" {
    name             = "MonIP"
    server_id        = azurerm_mssql_server.sql.id
    start_ip_address = "80.215.227.45"
    end_ip_address   = "80.215.227.45"
  }
