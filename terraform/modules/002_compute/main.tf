/*

Run this using CLI authentication. 

Create :
  1. Resource Group:
  2. create an eventhub workspace
  3. create event_hub
  


*/

 

# Create a resource group

resource "azurerm_resource_group" "nvs-rg-compute" {
  name     = var.rg_name_compute
  location = "East Us"
  tags = {
    environment = "dev"
  }
}

/*

resource "azurerm_eventhub_namespace" "example" {
  name                = "example-eventhubns"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
  sku                 = "Standard"  # Kafka is not available on the Basic tier

  kafka_enabled       = true
  capacity            = 1           # Number of throughput units

  tags = {
    environment = "Production"
  }
}



resource "azurerm_eventhub" "example" {
  name                = "example-eventhub"
  namespace_name      = azurerm_eventhub_namespace.example.name
  resource_group_name = azurerm_resource_group.example.name
  partition_count     = 2
  message_retention   = 1

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
}

*/