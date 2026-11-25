#how: order: provider -> terraform -> variables -> main -> network -> security -> messaging -> database

terraform {
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "terrstateforwatertracker"
    container_name       = "tfstate"
    key                  = "v2-clean.terraform.tfstate"
    use_azuread_auth     = true
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "5.11.0"

    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}
