/*

Run this using CLI authentication. 

Create :
  1. Resource Group:
  
*/
 

# Create a resource group


resource "azurerm_resource_group" "nvs-rg-ntwk" {
  name     = var.rg_name_network
  location = "East Us"
  tags = {
    environment = "dev"
  }
}

