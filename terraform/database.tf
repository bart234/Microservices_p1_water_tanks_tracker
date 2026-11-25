resource "random_password" "pwd_generator" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

#we create private dns for postgress
resource "azurerm_private_dns_zone" "postgres" {
  name                = "privatelink.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgres" {
  name                = "vnl-psql-${var.project_name}-${var.environment}"
  private_dns_zone_id = azurerm_private_dns_zone.postgres.id
  virtual_network_id  = azurerm_virtual_network.main.id
  depends_on          = [azurerm_subnet.db]
}

#here: we create just server 
resource "azurerm_postgresql_flexible_server" "main" {
  name                          = "psql-${var.project_name}-${var.environment}-6555"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = azurerm_resource_group.main.location
  version                       = "16"
  delegated_subnet_id           = azurerm_subnet.db.id
  private_dns_zone_id           = azurerm_private_dns_zone.postgres.id
  public_network_access_enabled = false
  administrator_login           = "psqladmin"
  administrator_password        = random_password.pwd_generator.result
  sku_name                      = "B_Standard_B1ms"
  storage_mb                    = 32768
  zone                          = "1"
  depends_on                    = [azurerm_private_dns_zone_virtual_network_link.postgres]
}

#create db on server
resource "azurerm_postgresql_flexible_server_database" "water_tracker" {
  name      = "water_tank_tracker"
  server_id = azurerm_postgresql_flexible_server.main.id
  collation = "en_US.utf8"
  charset   = "UTF8"
}


#we save to our vault
resource "azurerm_key_vault_secret" "db_password" {
  name         = "postgres-password"
  value        = random_password.pwd_generator.result
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv-admin]
}