variable "exclude_security_module" {
  description = "to exclude default module from running every time"
  default     = true # turn this to true if you want to run the security module
   /*another way to do this is use terraform workspace to manage a different state 
  terraform workspace new special-setup
    terraform apply # In the special-setup workspace
    terraform workspace select default # switch back to the default workspace */


}


variable "exclude_eventhub" {
  description = "event hub"
  default     = false

}