
data "azurerm_client_config" "current" {}

#azurerm_user_assigned_identity 
#containers will use that identity to auth to other azure services

resource "azurerm_user_assigned_identity" "aca" {
  name                = "id-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
}

#we create vault for our secrets
#azure_key_vault.name - have to be uniqe in all azure world
#who can use that vault - objects with role assigment like below vault
#each resource provider have to be first time registered: az provider register --namespace Microsoft.KeyVault

resource "azurerm_key_vault" "main" {
  name                        = "ob8-${var.project_name}${var.environment}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  rbac_authorization_enabled  = true
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false
  sku_name                    = "standard"
}

#name can be skipped - terraform will generate random uuid
#here we create role from scratch soe we ahve to name it 
#principal_id = who get access, 
#role_definition_name =what can do - officer - full access, user- restricted
#scope = on what resource have that access

#access rwd, it is assign to me, will be use by terraform to login and edit under my log in
#also terraform after create soem resource can take it pwd and save in vault directly  
resource "azurerm_role_assignment" "kv-admin" {
  principal_id         = data.azurerm_client_config.current.object_id
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
}

#can read secrets only - container with that role assigment will be useing that
resource "azurerm_role_assignment" "aca-reader" {
  principal_id         = azurerm_user_assigned_identity.aca.principal_id
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
}

#auth for acr to azure vault
resource "azurerm_role_assignment" "aca_acr_pull" {
  principal_id         = azurerm_user_assigned_identity.aca.principal_id
  scope                = data.azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
}