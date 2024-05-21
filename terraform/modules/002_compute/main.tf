/*

Run this using CLI authentication. 

Create :
  1. Resource Group:
  2. create an databricks workspace
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


/*
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

 // capture_description {
 //  enabled             = true
 //  encoding            = "Avro"
  //  interval_in_seconds = 300
  //  size_limit_in_bytes = 314572800
   // destination {
  //    archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
   //   blob_container_name = "eventhub-capture"
   //   name                = "EventHubArchive.AzureBlockBlob"
    //  storage_account_id  = azurerm_storage_account.example.id
   // }
  //}
  
 // https://chat.openai.com/share/c2c96bfa-c104-426a-8930-f483401d38f2
  
  
}
*/
 
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

  

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

  depends_on = [
    azurerm_databricks_workspace.nvs-databrickss-dev
  ]

  library {
    maven {
      coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
    }
  }
}

/*
resource "databricks_cluster" "cheap_dev_cluster" {
  cluster_name            = "cheap_dev-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS1_v2"
  autotermination_minutes = 20

  

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

  depends_on = [
    azurerm_databricks_workspace.nvs-databrickss-dev
  ]

  library {
    maven {
      coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
    }
  }
}

*/


data "local_file" "notebook_files" {
  for_each = fileset(var.notebooks_dir, "*.ipynb")  # Adjust the file extension as needed

  filename = "${var.notebooks_dir}/${each.value}"
}

resource "databricks_notebook" "from_directory" {
  for_each = fileset(var.notebooks_dir, "*.ipynb")  // Assuming you are using Jupyter notebooks

  path     = "/Workspace/Users/a.azeez@techrole.ca/${each.key}"
  source   = "${var.notebooks_dir}/${each.value}"
  language = "PYTHON"  // Assuming Python for .ipynb files
  format   = "DBC"  // Assuming these are plain text Jupyter notebooks
}


resource "databricks_secret_scope" "kv" {
  name = "keyvault-managed"

  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }
}

