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
  description = "ETQ authentication user"
}

variable "etq_password" {
  type        = string
  description = "ETQ authentication password"
}

variable "etq_base_url" {
  type        = string
  description = "ETQ base URL per env"
}

variable "aha_container_group_name" {
  type        = string
  description = "Aha! container-group name in Azure"
}

variable "aha_token" {
  type        = string
  description = "Aha! API token"
}


variable "liferay_analytics_cloud_container_group_name" {
  type        = string
  description = "Liferay Analytics Cloud container-group name in Azure"
}

variable "liferay_analytics_cloud_token" {
  type        = string
  description = "Liferay Analytics Cloud API token"
}

variable "looker_sdk_client_id" {
  type        = string
  description = "looker client id"
}
variable "looker_sdk_client_secret" {
  type        = string
  description = "looker client secret"
}
variable "looker_sdk_base_url" {
  type        = string
  description = "looker url with port"
}
variable "looker_sdk_container_group_name" {
  type        = string
  description = "looker sdk container group name"
}
variable "national_vulnerability_database_secret" {
  type        = string
  description = "NVD API Authorization key"
}
variable "national_vulnerability_database_container_group_name" {
  type        = string
  description = "NVD container-group name in Azure"
}