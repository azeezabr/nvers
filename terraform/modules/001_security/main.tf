/*

Run this using CLI authentication. 

Create :
  1. Resource Group:
  2. Service Principal
  4. Assign contributor role to Service principal
  5. Create Keyvault
  6. Add service principal secretes to key fault
  7. Elevation keyvault access policy.
  8. assign storage contributor role 


*/


# Create a resource group
 
resource "azurerm_resource_group" "nvs-rg-sec" {
  name     = var.rg_name_security
  location = "East Us"
  tags = {
    environment = "dev"
  }
}


# Create Entra Aplicatrion


data "azuread_client_config" "current" {}

#resource "random_uuid" "app_role_id" {}

resource "azuread_application_registration" "entra_app" {
  display_name     = var.entra_app_service_principal
  description      = "My entra_app application"
  sign_in_audience = "AzureADMyOrg"

   
}
 


# Create Service Principal

data "azuread_client_config" "current2" {} 
 
resource "azuread_service_principal" "service_princ" {
  client_id                    = azuread_application_registration.entra_app.client_id
  app_role_assignment_required = false
  owners                       = [data.azuread_client_config.current2.object_id]
}

# Create a Service Principal Time based secrete
resource "time_rotating" "time_rot" {
  rotation_days = 7
}

resource "azuread_service_principal_password" "srvc_pass" {
  service_principal_id = azuread_service_principal.service_princ.object_id
  rotate_when_changed = {
    rotation = time_rotating.time_rot.id
  }
}

resource "azurerm_role_assignment" "strg-contr" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.service_princ.id
}



# Assign a Role to the Service Principal at the Subscription Level

data "azurerm_subscription" "current" {}

resource "azurerm_role_assignment" "svc_princ_role" {
  scope                = data.azurerm_subscription.current.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.service_princ.id
}


# Create KeyVault


data "azurerm_client_config" "kv_current" {}

resource "random_uuid" "kv_object" {}

 
resource "azurerm_key_vault" "nvs-kv" {
  name                        = var.key_vault_nm
  location                    = azurerm_resource_group.nvs-rg-sec.location
  resource_group_name         = azurerm_resource_group.nvs-rg-sec.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.kv_current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.kv_current.tenant_id
    object_id = data.azurerm_client_config.kv_current.object_id

    key_permissions = [
      "Get","List"
    ]

    secret_permissions = [
      "Set","Get","List","Delete","Recover","Backup","Restore","Purge"
    ]

    storage_permissions = [
      "Get","List"
    ] 
  }

  

  tags = {
      environment = "dev"
    }

}


# create access policy for the service principal

data "azurerm_client_config" "sp_plc_current" {}

resource "azurerm_key_vault_access_policy" "sp_access_policy" {
  key_vault_id = azurerm_key_vault.nvs-kv.id

  tenant_id = data.azurerm_client_config.sp_plc_current.tenant_id
  object_id = azuread_service_principal.service_princ.object_id

  key_permissions = [
      "Get","List"
    ]

    secret_permissions = [
      "Set","Get","List","Delete","Recover","Backup","Restore","Purge"
    ]

    storage_permissions = [
      "Get","List"
    ] 

  depends_on = [
    azuread_service_principal.service_princ,
    azurerm_key_vault.nvs-kv
  ]
}


# Add secrets to keyvault
 

resource "azurerm_key_vault_secret" "application_id" {
  name         = var.serviceprincid
  value        = azuread_application_registration.entra_app.client_id
  key_vault_id = azurerm_key_vault.nvs-kv.id

  depends_on = [
    azurerm_key_vault.nvs-kv
  ]
}

data "azuread_client_config" "tennant_current" {}


resource "azurerm_key_vault_secret" "tenant_id" {
  name         = var.tenant_name
  value        = data.azuread_client_config.tennant_current.tenant_id
  key_vault_id = azurerm_key_vault.nvs-kv.id

  depends_on = [
    azurerm_key_vault.nvs-kv
  ]
}

resource "azurerm_key_vault_secret" "secret" {
  name         = var.sevc_prc_secrt
  value        = azuread_service_principal_password.srvc_pass.value
  key_vault_id = azurerm_key_vault.nvs-kv.id

  depends_on = [
    azurerm_key_vault.nvs-kv
  ]
}
 
#output keyvault info to use in compute module to create secrete scope for databriks
 output "key_vault_id" {
  value = azurerm_key_vault.nvs-kv.id
  description = "The ID of the Key Vault"
}

output "key_vault_uri" {
  value = azurerm_key_vault.nvs-kv.vault_uri
  description = "The DNS URI of the Key Vault"
}
