variable "location" {
  description = "Region Azure for infrastructure"
  type        = string
  default     = "polandcentral"
}

variable "resource_group_name" {
  description = "Main name for resource gruop"
  type        = string
  default     = "rg-water-tracker"
}
variable "kafka_event_hub_namespace" {
  description = "Main name for resource gruop"
  type        = string
  default     = "eh-ns-water-tracker-bb234"
}


variable "kafka_event_hub_conn_str" {
  description = "Main name for resource gruop"
  type        = string
  sensitive   = true
}
variable "postgress_user" {
  description = ""
  type        = string
  sensitive   = true
}
variable "postgress_pwd" {
  description = ""
  type        = string
  sensitive   = true 
}
variable "postgress_localhost" {
  description = "....postgres.database.azure.com"
  type        = string
  sensitive   = true
}
variable "postgress_db_name" {
  description = "DB name"
  type        = string
  sensitive   = true
}
variable "acr_password" {
  description = "DB name"
  type        = string
  sensitive   = true
}
