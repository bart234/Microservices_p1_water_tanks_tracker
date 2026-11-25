resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location
  tags = {
    Environment = var.environment,
    Project     = var.project_name,
  ManagedBy = "Terraform" }
}