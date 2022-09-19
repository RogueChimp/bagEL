data "azurerm_resource_group" "rg" {
  name = var.rg_name
}

data "azurerm_container_registry" "acr" {
  name                = var.acr_name
  resource_group_name = data.azurerm_resource_group.rg.name
}

data "azurerm_storage_account" "store_acct" {
  name                     = var.storage_account_name
  resource_group_name      = var.rg_name
}

#okta
resource "azurerm_container_group" "okta_cg" {
  name                = var.okta_container_group_name
  resource_group_name = data.azurerm_resource_group.rg.name
  location            = data.azurerm_resource_group.rg.location
  os_type             = "Linux"
  restart_policy      = "Never"

  image_registry_credential {
    username = data.azurerm_container_registry.acr.admin_username
    password = data.azurerm_container_registry.acr.admin_password
    server   = data.azurerm_container_registry.acr.login_server
  }

  container {
    name   = "okta"
    image  = "${data.azurerm_container_registry.acr.login_server}/okta:${var.build_id}"
    cpu    = "0.5"
    memory = "1.5"

    environment_variables =  {
      AZURE_CONTAINER = var.azure_container
      AZURE_TABLE = var.azure_table
      STORAGE_ACCOUNT = var.storage_account
      STORAGE_ACCOUNT_ENDPOINT = var.storage_account_endpoint
    }

    secure_environment_variables =  {
      STORAGE_ACCOUNT_KEY = data.azurerm_storage_account.store_acct.primary_access_key
      STORAGE_ACCOUNT_CONNECTION_STRING = var.storage_account_connection_string
      OKTA_SECRET = var.okta_secret
    }

    ports {
          port     = 443
          protocol = "TCP"
    }
  }

}

#looker
resource "azurerm_container_group" "looker_cg" {
  name                = var.looker_container_group_name 
  resource_group_name = data.azurerm_resource_group.rg.name
  location            = data.azurerm_resource_group.rg.location
  os_type             = "Linux"
  restart_policy      = "Never"

  image_registry_credential {
    username = data.azurerm_container_registry.acr.admin_username
    password = data.azurerm_container_registry.acr.admin_password
    server   = data.azurerm_container_registry.acr.login_server
  }

  container {
    name   = "looker"
    image  = "${data.azurerm_container_registry.acr.login_server}/looker:${var.build_id}"
    cpu    = "0.5"
    memory = "1.5"
 
    environment_variables = {
      AZURE_CONTAINER = var.azure_container
      AZURE_TABLE = var.azure_table
      LOOKER_CLIENT_ID = var.looker_client_id
      LOOKER_URL = var.looker_url
      LOOKER_PORT = var.looker_port
      LOOKER_API_ENDPOINT = var.looker_api_endpoint
      STORAGE_ACCOUNT = var.storage_account
      STORAGE_ACCOUNT_ENDPOINT = var.storage_account_endpoint
    }

    secure_environment_variables =  {
      STORAGE_ACCOUNT_KEY = data.azurerm_storage_account.store_acct.primary_access_key
      STORAGE_ACCOUNT_CONNECTION_STRING = var.storage_account_connection_string
      LOOKER_CLIENT_SECRET = var.looker_client_secret
    }

    ports {
          port     = 443
          protocol = "TCP"
    }
  }
}

#looker sdk
resource "azurerm_container_group" "looker_sdk_cg" {
  name                = var.looker_sdk_container_group_name 
  resource_group_name = data.azurerm_resource_group.rg.name
  location            = data.azurerm_resource_group.rg.location
  os_type             = "Linux"
  restart_policy      = "Never"

  image_registry_credential {
    username = data.azurerm_container_registry.acr.admin_username
    password = data.azurerm_container_registry.acr.admin_password
    server   = data.azurerm_container_registry.acr.login_server
  }

  container {
    name   = "looker-sdk"
    image  = "${data.azurerm_container_registry.acr.login_server}/looker_sdk:${var.build_id}"
    cpu    = "0.5"
    memory = "1.5"
 
    environment_variables = {
      AZURE_CONTAINER = var.azure_container
      AZURE_TABLE = var.azure_table
      STORAGE_ACCOUNT = var.storage_account
      STORAGE_ACCOUNT_ENDPOINT = var.storage_account_endpoint
      LOOKERSDK_BASE_URL = var.looker_sdk_base_url
      LOOKERSDK_CLIENT_ID = var.looker_sdk_client_id
      
    }

    secure_environment_variables =  {
      STORAGE_ACCOUNT_KEY = data.azurerm_storage_account.store_acct.primary_access_key
      STORAGE_ACCOUNT_CONNECTION_STRING = var.storage_account_connection_string
      LOOKERSDK_CLIENT_SECRET = var.looker_sdk_client_secret
    }

    ports {
          port     = 443
          protocol = "TCP"
    }
  }
}

#liferay analytics cloud
# resource "azurerm_container_group" "liferay_analytics_cloud_cg" {
#   name                = var.liferay_analytics_cloud_container_group_name 
#   resource_group_name = data.azurerm_resource_group.rg.name
#   location            = data.azurerm_resource_group.rg.location
#   os_type             = "Linux"
#   restart_policy      = "Never"

#   image_registry_credential {
#     username = data.azurerm_container_registry.acr.admin_username
#     password = data.azurerm_container_registry.acr.admin_password
#     server   = data.azurerm_container_registry.acr.login_server
#   }

#   container {
#     name   = "liferay-analytics-cloud"
#     image  = "${data.azurerm_container_registry.acr.login_server}/liferay_analytics_cloud:${var.build_id}"
#     cpu    = "0.5"
#     memory = "1.5"
 
#     environment_variables = {
#       AZURE_CONTAINER = var.azure_container
#       AZURE_TABLE = var.azure_table
#       STORAGE_ACCOUNT = var.storage_account
#       STORAGE_ACCOUNT_ENDPOINT = var.storage_account_endpoint
#     }

#     secure_environment_variables =  {
#       STORAGE_ACCOUNT_KEY = data.azurerm_storage_account.store_acct.primary_access_key
#       STORAGE_ACCOUNT_CONNECTION_STRING = var.storage_account_connection_string
#       LIFERAY_ANALYTICS_CLOUD_TOKEN = var.liferay_analytics_cloud_token
#     }

#     ports {
#           port     = 443
#           protocol = "TCP"
#     }
#   }
# }

resource "azurerm_container_group" "etq_cg" {
  name                = var.etq_container_group_name
  resource_group_name = data.azurerm_resource_group.rg.name
  location            = data.azurerm_resource_group.rg.location
  os_type             = "Linux"
  restart_policy      = "Never"

  image_registry_credential {
    username = data.azurerm_container_registry.acr.admin_username
    password = data.azurerm_container_registry.acr.admin_password
    server   = data.azurerm_container_registry.acr.login_server
  }

  container {
    name   = "etq"
    image  = "${data.azurerm_container_registry.acr.login_server}/etq:${var.build_id}"
    cpu    = "0.5"
    memory = "1.5"

    environment_variables =  {
      AZURE_CONTAINER = var.azure_container
      AZURE_TABLE = var.azure_table
      STORAGE_ACCOUNT = var.storage_account
      STORAGE_ACCOUNT_ENDPOINT = var.storage_account_endpoint
      ETQ_USER = var.etq_user
    }

    secure_environment_variables =  {
      STORAGE_ACCOUNT_KEY = data.azurerm_storage_account.store_acct.primary_access_key
      STORAGE_ACCOUNT_CONNECTION_STRING = var.storage_account_connection_string
      ETQ_PASSWORD = var.etq_password
    }

    ports {
          port     = 443
          protocol = "TCP"
    }
  }

}