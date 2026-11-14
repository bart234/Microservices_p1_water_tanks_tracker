
resource "azurerm_container_app_environment" "res-1" {
  infrastructure_resource_group_name = "ME_env-subnet-water-tracker_rg-water-tracker_polandcentral"
  infrastructure_subnet_id           = azurerm_subnet.aca_subnet.id
  location                           = var.location
  log_analytics_workspace_id         = azurerm_log_analytics_workspace.res-3.id
  logs_destination                   = "log-analytics"
  name                               = "env-subnet-water-tracker"
  resource_group_name                = var.resource_group_name
  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }
}

resource "azurerm_container_app" "res-1" {
  container_app_environment_id = azurerm_container_app_environment.res-1.id
  max_inactive_revisions       = 100
  name                         = "backend-core"
  resource_group_name          = var.resource_group_name
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
  registry {
    password_secret_name = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    server               = "bart234cr-brgebeh5cgcpcnfb.azurecr.io"
    username             = "bart234cr"
  }
  secret {
    name  = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    value = var.acr_password
  }
  template {
    container {
      cpu    = 0.25
      image  = "bart234cr-brgebeh5cgcpcnfb.azurecr.io/backendcore:latest"
      memory = "0.5Gi"
      name   = "backend-core"
      env {
        name  = "POSTGRES_USER"
        value = var.postgress_user
      }
      env {
        name  = "POSTGRES_PASSWORD"
        value = var.postgress_pwd
      }
      env {
        name  = "POSTGRES_DB_LOCALHOST"
        value = var.postgress_localhost
      }
      env {
        name  = "POSTGRES_DB"
        value = var.postgress_db_name
      }
      env {
        name  = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        value = var.kafka_event_hub_conn_str
      }
      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = var.kafka_event_hub_namespace
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
resource "azurerm_container_app" "res-2" {
  container_app_environment_id = azurerm_container_app_environment.res-1.id
  max_inactive_revisions       = 100
  name                         = "infra-redis"
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  ingress {
    target_port = 6379
    transport   = "tcp"
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }
  template {
    container {
      cpu    = 0.25
      image  = "redis:7-alpine"
      memory = "0.5Gi"
      name   = "infra-redis"
    }
  }
}
resource "azurerm_container_app" "res-3" {
  container_app_environment_id = azurerm_container_app_environment.res-1.id
  max_inactive_revisions       = 100
  name                         = "service-db-to-kafka"
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  registry {
    password_secret_name = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    server               = "bart234cr-brgebeh5cgcpcnfb.azurecr.io"
    username             = "bart234cr"
  }
  secret {
    name  = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    value = var.acr_password
  }
  template {
    container {
      cpu    = 0.25
      image  = "bart234cr-brgebeh5cgcpcnfb.azurecr.io/servicedbtokafka:latest"
      memory = "0.5Gi"
      name   = "service-db-to-kafka"
      env {
        name  = "PROD_MODE"
        value = "True"
      }
      env {
        name  = "RECORD_LIMIT"
        value = "10"
      }
      env {
        name  = "SLEEP_TIME"
        value = "5"
      }
      env {
        name  = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        value = var.kafka_event_hub_conn_str
      }
      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = var.kafka_event_hub_namespace
      }
      env {
        name  = "POSTGRES_USER"
        value = var.postgress_user
      }
      env {
        name  = "POSTGRES_PASSWORD"
        value = var.postgress_pwd
      }
      env {
        name  = "POSTGRES_DB_LOCALHOST"
        value = var.postgress_localhost
      }
      env {
        name  = "POSTGRES_DB"
        value = var.postgress_db_name
      }
      env {
        name  = "TOPIC_LIST_FOR_NOTIFICATION_CENTER"
        value = "water_tank_power,sms_service,email_service"
      }
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }
}
resource "azurerm_container_app" "res-4" {
  container_app_environment_id = azurerm_container_app_environment.res-1.id
  max_inactive_revisions       = 100
  name                         = "service-notification-center"
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  registry {
    password_secret_name = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    server               = "bart234cr-brgebeh5cgcpcnfb.azurecr.io"
    username             = "bart234cr"
  }
  secret {
    name  = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    value = var.acr_password
  }
  template {
    container {
      cpu    = 0.25
      image  = "bart234cr-brgebeh5cgcpcnfb.azurecr.io/servicenotificationcenter:latest"
      memory = "0.5Gi"
      name   = "service-notification-center"
      env {
        name  = "REDIS_HOST"
        value = "infra-redis"
      }
      env {
        name  = "REDIS_PORT"
        value = "6379"
      }
      env {
        name  = "REDIS_TIME_TO_EXPIRE_KEY"
        value = "20"
      }
      env {
        name  = "POSTGRES_USER"
        value = var.postgress_user
      }
      env {
        name  = "POSTGRES_PASSWORD"
        value = var.postgress_pwd
      }
      env { 
        name  = "POSTGRES_DB_LOCALHOST"
        value = var.postgress_localhost
      }
      env {
        name  = "POSTGRES_DB"
        value = var.postgress_db_name
      }
      env {
        name  = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        value = var.kafka_event_hub_conn_str
      }
      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = var.kafka_event_hub_namespace
      }
      env {
        name  = "KAFKA_WAIT_TIME_BETWEE_POOLS"
        value = "1"
      }
      env {
        name  = "KAFKA_INCOMING_TOPICS"
        value = "notification_service,front_api_return_msg"
      }
      env {
        name  = "KAFKA_GROUP_ID"
        value = "NotificationCenter.id"
      }
      env {
        name  = "KAFKA_TOPIC_OUTGOING"
        value = "topic.message"
      }
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }
}
resource "azurerm_container_app" "res-5" {
  container_app_environment_id = azurerm_container_app_environment.res-1.id
  max_inactive_revisions       = 100
  name                         = "service-on-off-watertank"
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  registry {
    password_secret_name = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    server               = "bart234cr-brgebeh5cgcpcnfb.azurecr.io"
    username             = "bart234cr"
  }
  secret {
    name  = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    value = var.acr_password
  }
  template {
    container {
      cpu    = 0.25
      image  = "bart234cr-brgebeh5cgcpcnfb.azurecr.io/serviceturnonwatertank:latest"
      memory = "0.5Gi"
      name   = "service-on-off-watertank"
      env {
        name  = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        value = var.kafka_event_hub_conn_str
      }
      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = var.kafka_event_hub_namespace
      }
      env {
        name  = "KAFKA_WAIT_TIME_BETWEE_POOLS"
        value = "1"
      }
      env {
        name  = "KAFKA_INCOMING_TOPICS"
        value = "water_tank_power"
      }
      env {
        name  = "KAFKA_GROUP_ID"
        value = "water_tank_power.id"
      }
      env {
        name  = "KAFKA_TOPIC_OUTGOING"
        value = "front_api_return_msg"
      }
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }
}
resource "azurerm_container_app" "res-6" {
  container_app_environment_id = azurerm_container_app_environment.res-1.id
  max_inactive_revisions       = 100
  name                         = "service-sms"
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  registry {
    password_secret_name = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    server               = "bart234cr-brgebeh5cgcpcnfb.azurecr.io"
    username             = "bart234cr"
  }
  secret {
    name  = "bart234cr-brgebeh5cgcpcnfbazurecrio-bart234cr"
    value = var.acr_password
  }
  template {
    container {
      cpu    = 0.25
      image  = "bart234cr-brgebeh5cgcpcnfb.azurecr.io/servicesms:latest"
      memory = "0.5Gi"
      name   = "service-sms"
      env {
        name  = "KAFKA_EVENT_HUB_CONNECTION_STRING"
        value = var.kafka_event_hub_conn_str
      }
      env {
        name  = "KAFKA_EVENT_HUB_NAMESPACE"
        value = var.kafka_event_hub_namespace
      }
      env {
        name  = "KAFKA_WAIT_TIME_BETWEE_POOLS"
        value = "1"
      }
      env {
        name  = "KAFKA_INCOMING_TOPICS"
        value = "sms_service.message"
      }
      env {
        name  = "KAFKA_GROUP_ID"
        value = "ServiceSMS.id"
      }
      env {
        name  = "KAFKA_TOPIC_OUTGOING"
        value = "front_api_message_service"
      }
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }
}