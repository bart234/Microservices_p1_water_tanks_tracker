resource "azurerm_eventhub_namespace" "main" {
  location            = var.location
  name                = "eh-ns-water-tracker-bb234"
  resource_group_name = var.resource_group_name
  sku                 = "Standard"
}
resource "azurerm_eventhub" "email_service_message" {
  name              = "email_service.message"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}

resource "azurerm_eventhub" "front_api_message_service" {
  name              = "front_api_message_service"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}

resource "azurerm_eventhub" "front_api_return_msg" {
  name              = "front_api_return_msg"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}

resource "azurerm_eventhub" "notification_service" {
  name              = "notification_service"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}

resource "azurerm_eventhub" "sms_service_message" {
  name              = "sms_service.message"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}

resource "azurerm_eventhub" "water_tank_power" {
  name              = "water_tank_power"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 1
  message_retention = 1
}
