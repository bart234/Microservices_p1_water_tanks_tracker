resource "azurerm_virtual_network" "main" {
  address_space       = ["10.0.0.0/16"]
  location            = var.location
  name                = "vnet-water-tracker"
  resource_group_name = var.resource_group_name
}
resource "azurerm_subnet" "aca_subnet" {
  address_prefixes     = ["10.0.0.0/23"]
  name                 = "subnet-aca-water-tracker"
  resource_group_name  = azurerm_virtual_network.main.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  delegation {
    name = "0"
    service_delegation {
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
      name    = "Microsoft.App/environments"
    }
  }
}