terraform {
  required_version = ">=1.1.7"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.46.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.58.0"
    }
  }
}

# Provider Azure Resource Manager
provider "azurerm" {
  features {}
}

# Provider Databricks (necessite le host et un PAT)
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
