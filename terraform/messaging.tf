resource "azurerm_eventhub_namespace" "main" {
  name                = "ns-${var.project_name}${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Standard"
  capacity            = 1
  tags = {
    Environment = var.environment,
    Project     = var.project_name,
  ManagedBy = "Terraform" }
}

resource "azurerm_eventhub" "topics" {
  for_each = toset(["email_service.message",
    "front_api_message_service",
    "front_api_return_msg",
    "notification_service",
    "sms_service.message",
  "water_tank_power"])
  name              = each.value
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}

#we save eh conn str into vault
#depends_on (explicit dependency)  - it force terraform to create vault with kv-admin role, and at the end that resource
#if we not foce that dependency terraform will try to create all messaging  in same time like security
#vault and role needed to be present before that update secret key appear
resource "azurerm_key_vault_secret" "eventhub_conn_str" {
  name         = "kafka-eventhub-connection-string"
  value        = azurerm_eventhub_namespace.main.default_primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv-admin]
}
