output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "datalake_storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "datalake_filesystems" {
  value = [for fs in azurerm_storage_data_lake_gen2_filesystem.zones : fs.name]
}

output "datalake_primary_connection_string" {
  value       = azurerm_storage_account.datalake.primary_connection_string
  description = "Chaine de connexion a utiliser pour ecrire dans le Data Lake"
  sensitive   = true
}

output "datalake_dfs_endpoint" {
  value       = azurerm_storage_account.datalake.primary_dfs_endpoint
  description = "Endpoint DFS pour les opÃ©rations ADLS Gen2"
}

output "key_vault_name" {
  value       = azurerm_key_vault.kv.name
  description = "Nom du Key Vault"
}

output "key_vault_uri" {
  value       = azurerm_key_vault.kv.vault_uri
  description = "URI du Key Vault (https://<nom>.vault.azure.net)"
}

output "data_factory_name" {
  value       = azurerm_data_factory.adf.name
  description = "Nom de l'Azure Data Factory"
}

output "data_factory_identity_principal_id" {
  value       = azurerm_data_factory.adf.identity[0].principal_id
  description = "Object ID de l'identitÃ© managÃ©e de Data Factory"
}

output "upload_files_enabled" {
  value       = var.upload_files_enabled
  description = "Upload local -> Data Lake activÃ©"
}

output "upload_file_count" {
  value       = length(local.upload_files)
  description = "Nombre de fichiers dÃ©tectÃ©s dans upload_source_dir"
}
output "sql_server_name" {
  value       = azurerm_mssql_server.sql.name
  description = "Nom du serveur Azure SQL"
}

output "sql_server_fqdn" {
  value       = azurerm_mssql_server.sql.fully_qualified_domain_name
  description = "FQDN du serveur Azure SQL"
}

output "sql_database_name" {
  value       = azurerm_mssql_database.sql.name
  description = "Nom de la base Azure SQL provisionnee"
}

output "sql_database_id" {
  value       = azurerm_mssql_database.sql.id
  description = "ID de la base Azure SQL"
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.dbw.workspace_url
  description = "URL du workspace Databricks cree"
}



