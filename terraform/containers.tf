data "azurerm_container_registry" "acr" {
  name                = "bart234cr"
  resource_group_name = "resource_group_1"
}

resource "azurerm_container_app_environment" "main" {
  name                       = "caenv-${var.project_name}-${var.environment}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  logs_destination           = "log-analytics"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  infrastructure_subnet_id   = azurerm_subnet.aca.id
  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }
}

resource "azurerm_container_app" "redis" {
  name                         = "redis-${var.environment}"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  ingress {
    external_enabled = false
    target_port      = 6379
    transport        = "tcp"
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }
  template {
    container {
      name   = "infra-redis"
      image  = "redis:7-alpine"
      cpu    = 0.25
      memory = "0.5Gi"
    }
  }
}

resource "azurerm_container_app" "backend-core" {
  container_app_environment_id = azurerm_container_app_environment.main.id
  max_inactive_revisions       = 3
  name                         = "backend-core"
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  ingress {
    external_enabled = true
    target_port      = 80
    transport        = "http"
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aca.id]
  }

  registry {
    server   = data.azurerm_container_registry.acr.login_server
    identity = azurerm_user_assigned_identity.aca.id
  }
  secret {
    name                = "postgres-password"
    key_vault_secret_id = azurerm_key_vault_secret.db_password.versionless_id
    identity            = azurerm_user_assigned_identity.aca.id
  }

  secret {
    name                = "kafka-conn-str"
    key_vault_secret_id = azurerm_key_vault_secret.eventhub_conn_str.versionless_id
    identity            = azurerm_user_assigned_identity.aca.id
  }
  template {
    container {
      cpu    = 0.25
      image  = "${data.azurerm_container_registry.acr.login_server}/backendcore:latest"
      memory = "0.5Gi"
      name   = "backend-core"
      env {
        name  = "POSTGRES_USER"
        value = azurerm_postgresql_flexible_server.main.administrator_login
      }
      env {
        name        = "POSTGRES_PASSWORD"
        secret_name = "postgres-password"
      }
      env {
        name  = "POSTGRES_DB_LOCALHOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }
      env {
        name  = "POSTGRES_DB"
        value = azurerm_postgresql_flexible_server_database.water_tracker.name
      }
      env {
        name        = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        secret_name = "kafka-conn-str"
      }
      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = azurerm_eventhub_namespace.main.name
      }
      env {
        name  = "KAFKA_WAIT_TIME_BETWEE_POOLS"
        value = "1"
      }
      env {
        name  = "KAFKA_INCOMING_TOPICS"
        value = "front_api_return_msg,front_api_message_service"
      }
      env {
        name  = "KAFKA_GROUP_ID"
        value = "Api_feedback_loop.id"
      }
      env {
        name = "KAFKA_TOPIC_OUTGOING"
      }
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }
}

locals {
  workers = {
    "service-db-to-kafka" = {
      image    = "servicedbtokafka:latest"
      needs_db = true
      env = {
        PROD_MODE                          = "True"
        RECORD_LIMIT                       = "10"
        SLEEP_TIME                         = "5"
        TOPIC_LIST_FOR_NOTIFICATION_CENTER = "water_tank_power,sms_service,email_service"

        POSTGRES_USER = azurerm_postgresql_flexible_server.main.administrator_login
        #POSTGRES_PASSWORD = passed by secret by dynamic block
        POSTGRES_DB_LOCALHOST = azurerm_postgresql_flexible_server.main.fqdn
        POSTGRES_DB           = azurerm_postgresql_flexible_server_database.water_tracker.name
      }
    }
    "service-notification-center" = {
      image    = "servicenotificationcenter:latest"
      needs_db = true
      env = {
        KAFKA_INCOMING_TOPICS = "notification_service,front_api_return_msg"
        KAFKA_GROUP_ID        = "NotificationCenter.id"
        KAFKA_TOPIC_OUTGOING  = "topic.message"

        REDIS_HOST               = azurerm_container_app.redis.name
        REDIS_PORT               = "6379"
        REDIS_TIME_TO_EXPIRE_KEY = "20"

        POSTGRES_USER = azurerm_postgresql_flexible_server.main.administrator_login
        #POSTGRES_PASSWORD = passed by secret by dynamic block
        POSTGRES_DB_LOCALHOST = azurerm_postgresql_flexible_server.main.fqdn
        POSTGRES_DB           = azurerm_postgresql_flexible_server_database.water_tracker.name
      }
    }
    "service-on-off-watertank" = {
      image    = "serviceturnonwatertank:latest"
      needs_db = false
      env = {
        KAFKA_INCOMING_TOPICS = "water_tank_power"
        KAFKA_GROUP_ID        = "Water_tank_power.id"
        KAFKA_TOPIC_OUTGOING  = "front_api_return_msg"
      }
    }
    "service-sms" = {
      image    = "servicesms:latest"
      needs_db = false
      env = {
        KAFKA_INCOMING_TOPICS = "sms_service.message"
        KAFKA_GROUP_ID        = "ServiceSMS.id"
        KAFKA_TOPIC_OUTGOING  = "front_api_message_service"
      }
    }

  }
}

resource "azurerm_container_app" "worker" {
  for_each                     = local.workers
  name                         = each.key
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"
  max_inactive_revisions       = 3

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aca.id]
  }

  registry {
    server   = data.azurerm_container_registry.acr.login_server
    identity = azurerm_user_assigned_identity.aca.id
  }
  secret {
    name                = "postgres-password"
    key_vault_secret_id = azurerm_key_vault_secret.db_password.versionless_id
    identity            = azurerm_user_assigned_identity.aca.id
  }

  secret {
    name                = "kafka-conn-str"
    key_vault_secret_id = azurerm_key_vault_secret.eventhub_conn_str.versionless_id
    identity            = azurerm_user_assigned_identity.aca.id
  }



  template {
    container {
      cpu    = 0.25
      image  = "${data.azurerm_container_registry.acr.login_server}/${each.value.image}"
      memory = "0.5Gi"
      name   = each.key

      #shared variables for each worker
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
      env {
        name  = "KAFKA_WAIT_TIME_BETWEE_POOLS"
        value = "1"
      }


      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = azurerm_eventhub_namespace.main.name
      }

      #env variable with secret
      env {
        name        = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        secret_name = "kafka-conn-str"
      }

      #conditional secret pass
      dynamic "env" {

        for_each = each.value.needs_db ? [1] : []
        content {
          name        = "POSTGRES_PASSWORD"
          secret_name = "postgres-password"

        }
      }

      dynamic "env" {
        for_each = each.value.env

        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }

}