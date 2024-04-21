/*

Run this using CLI authentication. 

Create :
  1. Resource Group:
  2. create an eventhub workspace
  3. create event_hub namespace
  4. create eventhub
  


*/

 

# Create a resource group

resource "azurerm_resource_group" "nvs-rg-compute" {
  name     = var.rg_name_compute
  location = "East Us"
  tags = {
    environment = "dev"
  }
}

resource "azurerm_eventhub_namespace" "nvs-event-hub-nm-space" {
  name                = var.nvs_event_hub_nm_space
  location            = azurerm_resource_group.nvs-rg-compute.location
  resource_group_name = azurerm_resource_group.nvs-rg-compute.name
  sku                 = "Standard"  

  capacity            = 1           # Number of throughput units

  

  tags = {
    environment = "dev"
  }
}



resource "azurerm_eventhub" "nvs-stock-hub" {
  name                = var.stock_event_hub
  namespace_name      = azurerm_eventhub_namespace.nvs-event-hub-nm-space.name
  resource_group_name = azurerm_resource_group.nvs-rg-compute.name
  partition_count     = 2
  message_retention   = 1
/*
  capture_description {
    enabled             = true
    encoding            = "Avro"
    interval_in_seconds = 300
    size_limit_in_bytes = 314572800
    destination {
      archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      blob_container_name = "eventhub-capture"
      name                = "EventHubArchive.AzureBlockBlob"
      storage_account_id  = azurerm_storage_account.example.id
    }
  }
  
  https://chat.openai.com/share/c2c96bfa-c104-426a-8930-f483401d38f2
  
  */
}

 
resource "azurerm_databricks_workspace" "nvs-databrickss-dev" {
  name                = "databricks-dev"
  resource_group_name = azurerm_resource_group.nvs-rg-compute.name
  location            = azurerm_resource_group.nvs-rg-compute.location
  sku                 = "trial"

  #count  = var.exclude_databricks_trial ? 1 : 0

  tags = {
    Environment = "dev"
  }
} 


 
resource "databricks_cluster" "dev_cluster" {
  cluster_name            = "dev2-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 20

  autoscale {
    min_workers = 2
    max_workers = 8
  }

 
  library {
    maven {
      coordinates = "org.jsoup:jsoup:1.13.1"
    }
  }
}
 