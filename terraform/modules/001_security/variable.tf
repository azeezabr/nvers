
variable "rg_name_security"{
    type = string
    default = "nvs-rg-sec"
}



variable "entra_app_service_principal"{
    type = string
    default = "terraform_auth"
}



variable "key_vault_nm"{
    type = string
    default = "nvs-key-vault"
}


variable "serviceprincid"{
    type = string
    default = "servicePrincipalApplicationId"
}

 
variable "tenant_name"{
    type = string
    default = "TenantID"
}
 
 

variable "sevc_prc_secrt"{
    type = string
    default = "servicePrincipalSecret"
}


variable "storage_account_id" {
  description = "The ID of the Azure Storage Account"
  type        = string
}
