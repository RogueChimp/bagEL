variable "rg_name" {
  type        = string
  description = "Resource Group name to where resources are going to be deployed"
}

variable "acr_name" {
  type        = string
  description = "Azure Container Registry name"
}

variable "location" {
  type        = string
  description = "Location of Azure resources"
}

variable "build_id" {
  type        = string
  description = "Build ID of Azure DevOps Pipeline"
}

variable "azure_container" {
  type        = string
  description = "Name of container to be pushed to in Azure"
}

variable "azure_table" {
  type        = string
  description = "Name of table to be pushed to in Azure"
}

variable "storage_account" {
  type        = string
  description = "Azure Storage Account"
}

variable "storage_account_name" {
  type        = string
  description = "Name of Azure Storage Account"
}

variable "storage_account_endpoint" {
  type        = string
  description = "Azure storage account endpoint / connection string"
}

variable "storage_account_connection_string" {
  type        = string
  description = "Azure storage account connection string"
}

variable "okta_secret" {
  type        = string
  description = "Okta authentication token"
}

variable "okta_container_group_name" {
  type        = string
  description = "Okta container-group name in Azure"
}

variable "looker_container_group_name" {
  type        = string
  description = "Looker container-group name in Azure"
}

variable "looker_client_id" {
  type        = string
  description = "Looker authentication client id"
}

variable "looker_client_secret" {
  type        = string
  description = "Looker authentication client secret "
}

variable "looker_url" {
  type        = string
  description = "Looker url for the given environment / container"
}

variable "looker_port" {
  type        = string
  description = "Looker port number"
}

variable "looker_api_endpoint" {
  type        = string
  description = "Looker api endpoint with version"
}

variable "etq_container_group_name" {
  type        = string
  description = "ETQ container-group name in Azure"
}

variable "etq_user" {
  type        = string
  description = "ETQ username"
}

variable "etq_password" {
  type        = string
  description = "ETQ password"
}

variable "itsm_user" {
  type        = string
  description = "ITSM username"
}

variable "itsm_password" {
  type        = string
  description = "ITSM password"
}

variable "doclink_container_group_name" {
  type        = string
  description = "DocLink container-group name in Azure"
}

variable "doclink_username" {
  type        = string
  description = "DocLink username"
}

variable "doclink_password" {
  type        = string
  description = "DocLink password"
}

variable "doclink_site_code" {
  type        = string
  description = "DocLink site code"
}

variable "doclink_base_url" {
  type        = string
  description = "DocLink base url"
}