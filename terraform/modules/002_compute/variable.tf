 
variable "rg_name_compute"{
    type = string
    default = "nvs-rg-proc"
}


variable "nvs_event_hub_nm_space"{
    type = string
    default = "nvs-event-hub-namespace"
}


variable "stock_event_hub"{
    type = string
    default = "stock-hub"
}
 

variable "exclude_databricks_trial" {
  description = "to exclude databricks trial"
  default     = true 
}


variable "notebooks_dir" {
  description = "Path to the notebooks directory"
  type        = string
  default     = "/Users/azeez/Projects/nvers/util/notebook"
}
