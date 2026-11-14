resource "azurerm_log_analytics_workspace" "res-3" {
  location            = var.location
  name                = "workspace-rgwatertrackerj2Am"
  resource_group_name = var.resource_group_name
}