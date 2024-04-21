terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}

provider "databricks" {
  host  = azurerm_databricks_workspace.nvs-databrickss-dev.workspace_url
}