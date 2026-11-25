resource "azurerm_virtual_network" "main" {
  name                = "vnet-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  address_space       = ["10.123.0.0/16"]
  tags = {
    Environment = var.environment,
    Project     = var.project_name,
  ManagedBy = "Terraform" }
}

resource "azurerm_subnet" "aca" {
  name                 = "subnet-aca-${var.environment}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.123.0.0/23"]
  delegation {
    name = "0"
    service_delegation {
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
      name    = "Microsoft.App/environments"
    }
  }
}

resource "azurerm_subnet" "db" {
  name                 = "subnet-db-${var.environment}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.123.2.0/24"]
  delegation {
    name = "0"
    service_delegation {
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
    }
  }
}