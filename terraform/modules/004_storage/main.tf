/*

Run this using CLI authentication. 

Create :
  1. Resource Group:
 

*/
 

# Create a resource group

resource "azurerm_resource_group" "nvs-rg-strg" {
  name     = var.rg_name_storage
  location = "East Us"
  tags = {
    environment = "dev"
  }
}


resource "azurerm_storage_account" "nvs-project" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.nvs-rg-strg.name
  location                 = azurerm_resource_group.nvs-rg-strg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_container" "bronze" {
  name                  = var.bronze
  storage_account_name  = azurerm_storage_account.nvs-project.name
  container_access_type = "private"
}


resource "azurerm_storage_container" "silver" {
  name                  = var.silver
  storage_account_name  = azurerm_storage_account.nvs-project.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = var.gold
  storage_account_name  = azurerm_storage_account.nvs-project.name
  container_access_type = "private"
}

/*
resource "azurerm_storage_account" "nvs-silver" {
  name                     = var.silver
  resource_group_name      = azurerm_resource_group.nvs-rg-strg.name
  location                 = azurerm_resource_group.nvs-rg-strg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}



resource "azurerm_storage_account" "nvs-gold" {
  name                     = var.gold
  resource_group_name      = azurerm_resource_group.nvs-rg-strg.name
  location                 = azurerm_resource_group.nvs-rg-strg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}*/


output "storage_account_id" {
  value = azurerm_storage_account.nvs-project.id
  description = "The ID of the created Azure Storage Account"
}