resource "azurerm_storage_account" "store_acct" {
  name                     = var.storage_account_name
  resource_group_name      = var.rg_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version = "TLS1_2"
}

resource "azurerm_storage_container" "bagel_container" {
  name                  = "bagel"
  storage_account_name  = azurerm_storage_account.store_acct.name
}

resource "azurerm_storage_table" "bagel_table" {
  name                 = "bagel"
  storage_account_name = azurerm_storage_account.store_acct.name
}

resource "azurerm_container_registry" "acr" {
  name                = var.acr_name
  resource_group_name = var.rg_name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = true
}
