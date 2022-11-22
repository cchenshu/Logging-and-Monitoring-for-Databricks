variable "rg_name" {
  description = "resource group name to put the dashboard"
  default     = "rg1"
}

variable "rg_location" {
  description = "resource location"
  default     = "West US"
}

variable "dashboard_name" {
  description = "the name of the dashboard"
  default = "Monitoring_Dashboard"
}

variable "resource_id" {
  description = "Log analytics workspace resource ID."
  type        = string
  default = "/subscriptions/c022430e-7e85-46f0-898b-57c4edf3acc5/resourcegroups/psr-demo/providers/microsoft.operationalinsights/workspaces/loganalytics-psrdemo"
}