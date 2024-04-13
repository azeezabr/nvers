terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">=3.0.0"
    }
  }
}


provider "azurerm" {
  skip_provider_registration = true # This is only required when the User, Service Principal, or Identity running Terraform lacks the permissions to register Azure Resource Providers.
  features {}
}

module "security" {
  source = "./modules/001_security"
  count  = var.exclude_security_module ? 1 : 0
}

module "compute" {
  source = "./modules/002_compute"
}


module "network" {
  source = "./modules/003_network"
}

module "storage" {
  source               = "./modules/004_storage"
  //network_interface_ids = [module.network.subnet.id]  // Overriding default if needed

}






/*
 
data "azurerm_resource_group" "nvs-rg" {
  name = "data_engineering_rg"
}

resource "azuread_application" "example" {
  display_name = "example-application"
}

resource "azurerm_virtual_network" "nvs-net" {
  name                = "nvs-network"
  resource_group_name = data.azurerm_resource_group.nvs-rg.name
  location            = data.azurerm_resource_group.nvs-rg.location
  address_space       = ["10.0.0.0/16"]
  tags = {
    environment = "dev"
  }
}


resource "azurerm_subnet" "nvs-subnet" {
  name                 = "nvs-subnet"
  resource_group_name  = data.azurerm_resource_group.nvs-rg.name
  virtual_network_name = azurerm_virtual_network.nvs-net.name
  address_prefixes     = ["10.0.1.0/24"]


}


resource "azurerm_storage_account" "nvrs-storage" {
  name                     = "nvrsstorage1"
  resource_group_name      = data.azurerm_resource_group.nvs-rg.name
  location                 = data.azurerm_resource_group.nvs-rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}
*/