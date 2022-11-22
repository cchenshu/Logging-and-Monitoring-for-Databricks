terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "3.32.0"
    }
  }
}

provider "azurerm" {
  features {
  }
  # Configuration options
}

locals {
  count = "${length(split("/","${var.resource_id}"))-1}"
  diagnostic_name = "${split("/","${var.resource_id}")["${local.count}"]}"
  tags = {
    owner = "chenshu"
    usage = "terraform_sample"
  }
}

resource "azurerm_dashboard" "my-board" {
  name                = var.dashboard_name
  resource_group_name = var.rg_name
  location            = var.rg_location
  tags                = local.tags 
  dashboard_properties = <<DASH
{
    "lenses": {
      "0": {
        "order": 0,
        "parts": {
          "0": {
            "position": {
              "x": 0,
              "y": 0,
              "colSpan": 5,
              "rowSpan": 4
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkMetric_CL\n| where name_s contains \"driver.jvm.total.\"\n| where executorId_s == \"driver\"\n| extend memUsed_GB = value_d / 1000000000\n| project TimeGenerated, memUsed_GB\n| summarize max(memUsed_GB) by bin(TimeGenerated, 1m)\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "AnalyticsGrid",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "Query": "SparkMetric_CL\n| where name_s contains \"driver.jvm.total.\"\n| where executorId_s == \"driver\"\n| extend memUsed_GB = value_d / 1000000000\n| project TimeGenerated, memUsed_GB\n| summarize max(memUsed_GB) by bin(TimeGenerated, 1m)\n\n",
                  "ControlType": "FrameControlChart",
                  "SpecificChart": "Line",
                  "PartTitle": "Memory Usage",
                  "Dimensions": {
                    "xAxis": {
                      "name": "TimeGenerated",
                      "type": "datetime"
                    },
                    "yAxis": [
                      {
                        "name": "max_memUsed_GB",
                        "type": "real"
                      }
                    ],
                    "splitBy": [],
                    "aggregation": "Sum"
                  },
                  "LegendOptions": {
                    "isEnabled": true,
                    "position": "Bottom"
                  }
                }
              }
            }
          },
          "1": {
            "position": {
              "x": 6,
              "y": 0,
              "colSpan": 5,
              "rowSpan": 4
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkMetric_CL\n| where name_s contains \"executor.RunTime\"\n| project TimeGenerated , runTime=count_d\n| join kind=inner (\n    SparkMetric_CL\n| where name_s contains \"executor.cpuTime\"\n| project TimeGenerated , cpuTime=count_d/1000000, clusterName_s\n) on TimeGenerated\n| extend cpuUsage=(cpuTime/runTime)*100\n| summarize max(cpuUsage) by bin(TimeGenerated, 1m), clusterName_s\n| order by TimeGenerated asc nulls last\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "FrameControlChart",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "value": "Line",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "value": {
                    "xAxis": {
                      "name": "TimeGenerated",
                      "type": "datetime"
                    },
                    "yAxis": [
                      {
                        "name": "max_cpuUsage",
                        "type": "real"
                      }
                    ],
                    "splitBy": [],
                    "aggregation": "Sum"
                  },
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "value": {
                    "isEnabled": true,
                    "position": "Bottom"
                  },
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "PartTitle": "Cpu Usage"
                }
              }
            }
          },
          "2": {
            "position": {
              "x": 12,
              "y": 0,
              "colSpan": 5,
              "rowSpan": 4
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkLoggingEvent_CL\n| where Message contains \"healthcheck\"\n| extend Cluster_Readiness = iff((Message contains \"healthcheck successful\"), 1, 0)\n| project TimeGenerated, Cluster_Readiness, Message\n| order by TimeGenerated desc nulls last \n\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "FrameControlChart",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "value": "Line",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "value": {
                    "xAxis": {
                      "name": "TimeGenerated",
                      "type": "datetime"
                    },
                    "yAxis": [
                      {
                        "name": "Cluster_Readiness",
                        "type": "long"
                      }
                    ],
                    "splitBy": [],
                    "aggregation": "Sum"
                  },
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "value": {
                    "isEnabled": true,
                    "position": "Bottom"
                  },
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "PartTitle": "Cluster Readiness"
                }
              }
            }
          },
          "3": {
            "position": {
              "x": 0,
              "y": 5,
              "colSpan": 12,
              "rowSpan": 5
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "let results = SparkLoggingEvent_CL\n    | where Message has 'source_system' and Message has \"run_id\"\n    | extend _job_name = tostring(parse_json(Message).job_name)\n    | extend _job_id = tostring(parse_json(Message).job_id)\n    | extend _run_id = tostring(parse_json(Message).run_id)\n    | extend _message1 = tostring(parse_json(Message).message)\n    | summarize endtime = arg_max(TimeGenerated, *) by _run_id\n    | join kind = inner (\n        SparkLoggingEvent_CL\n        | where Message has 'source_system' and Message has \"run_id\"\n        | extend _job_name = tostring(parse_json(Message).job_name)\n        | extend _job_id = tostring(parse_json(Message).job_id)\n        | extend _run_id = tostring(parse_json(Message).run_id)\n        | extend _message2 = tostring(parse_json(Message).message)\n        )\n        on _run_id;\nresults\n| summarize starttime = arg_min(TimeGenerated, *) by _run_id\n| extend NewAge=replace(@'master_data_source', @'L2R', _job_name)\n| extend NewAge=replace(@'master_data_transform', @'R2P', NewAge)\n| extend NewAge=replace(@'event_data_transform', @'L2P', NewAge)\n| extend NewAge=replace(@'event_data_curation', @'P2C', NewAge)\n| extend tmp = split(NewAge, \"_\")\n| extend stage = tmp[-1]\n| extend _source_system = tostring(parse_json(Message).source_system)\n| extend _customer_id = tostring(parse_json(Message).customer_id)\n| project\n    _job_name,\n    _job_id1,\n    _source_system,\n    _customer_id,\n    _run_id1,\n    tostring(stage),\n    tostring(starttime),\n    tostring(endtime),\n    duration = tostring(datetime_diff('minute', endtime, starttime)),\n    clusterName_s,\n    clusterId_s\n| order by starttime desc nulls last\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "AnalyticsGrid",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "GridColumnsWidth": {
                    "ActionName": "130px",
                    "stage": "115px"
                  },
                  "Query": "let results = SparkLoggingEvent_CL\n    | where Message has 'source_system' and Message has \"run_id\"\n    | extend _job_name = tostring(parse_json(Message).job_name)\n    | extend _job_id = tostring(parse_json(Message).job_id)\n    | extend _run_id = tostring(parse_json(Message).run_id)\n    | summarize endtime = arg_max(TimeGenerated, *) by _run_id\n    | join kind = inner (\n        SparkLoggingEvent_CL\n        | where Message has 'source_system' and Message has \"run_id\"\n        | extend _job_name = tostring(parse_json(Message).job_name)\n        | extend _job_id = tostring(parse_json(Message).job_id)\n        | extend _run_id = tostring(parse_json(Message).run_id)\n        )\n        on _run_id;\nresults\n| summarize starttime = arg_min(TimeGenerated, *) by _run_id\n| join kind = leftouter(\nDatabricksJobs\n    | where ActionName contains_cs \"run\" and ActionName!=\"runTriggered\"\n    | extend _run_id = tostring(parse_json(RequestParams).multitaskParentRunId)\n    | summarize endtime = arg_max(TimeGenerated, *) by _run_id\n    | project _run_id, ActionName\n    ) on _run_id\n| where _job_name contains \"pimvo1psr1_Dynamo\"\n| extend NewAge=replace(@'master_data_source', @'L2R', _job_name)\n| extend NewAge=replace(@'master_data_transform', @'R2P', NewAge)\n| extend NewAge=replace(@'event_data_transform', @'L2P', NewAge)\n| extend NewAge=replace(@'event_data_curation', @'P2C', NewAge)\n| extend tmp = split(NewAge, \"_\")\n| extend stage = tmp[-1]\n| extend _source_system = tostring(parse_json(Message).source_system)\n| extend _customer_id = tostring(parse_json(Message).customer_id)\n| project\n    _job_name,\n    _job_id1,\n    _source_system,\n    _customer_id,\n    _run_id1,\n    tostring(ActionName),\n    tostring(stage),\n    time_taken = tostring(datetime_diff('second', endtime, starttime)),\n    tostring(starttime),\n    tostring(endtime),\n    clusterName_s,\n    clusterId_s\n| order by starttime desc nulls last\n\n",
                  "PartTitle": "Pipeline Status"
                }
              }
            }
          },
          "4": {
            "position": {
              "x": 0,
              "y": 11,
              "colSpan": 12,
              "rowSpan": 5
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkLoggingEvent_CL\n| where logger_name_s !contains \"hon_edm_solution\"\n| where Level == \"ERROR\"\n| where tostring(traceInfo_s) contains \"job\" and tostring(traceInfo_s) contains \"run\"\n| extend job_id = extract(\"job.([0-9]+)\",1,tostring(traceInfo_s))\n| extend task_run_id =extract(\"run.([0-9]+)\",1,tostring(traceInfo_s))\n| project TimeGenerated, job_id, task_run_id, tostring(Message), logger_name_s, Level\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "AnalyticsGrid",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "GridColumnsWidth": {
                    "logger_name_s": "449px",
                    "Message": "625px",
                    "task_run_id": "151.9967px",
                    "job_id": "134.998px"
                  },
                  "Query": "SparkLoggingEvent_CL\n| where logger_name_s !contains \"hon_edm_solution\"\n| where Level == \"ERROR\"\n| where tostring(traceInfo_s) contains \"job\" and tostring(traceInfo_s) contains \"run\"\n| extend job_id = extract(\"job.([0-9]+)\",1,tostring(traceInfo_s))\n| where job_id != \"\"\n| extend _run_id =extract(\"run.([0-9]+)\",1,tostring(traceInfo_s))\n| join kind = leftouter     (\n        SparkLoggingEvent_CL\n        | where Message has 'source_system' and Message has \"run_id\"\n        | extend _job_name = tostring(parse_json(Message).job_name)\n        | extend _run_id = tostring(parse_json(Message).run_id)\n        | extend job_id = tostring(parse_json(Message).job_id)\n        | where _job_name contains \"pimvo1psr1_Dynamo\"\n        | summarize starttime = arg_min(TimeGenerated, *) by job_id\n        | project _job_name, _run_id, job_id\n        )\non job_id\n| project TimeGenerated, job_id,_job_name, _run_id, tostring(Message), logger_name_s, Level\n\n",
                  "PartTitle": "Error Jobs"
                }
              }
            }
          },
          "5": {
            "position": {
              "x": 0,
              "y": 17,
              "colSpan": 12,
              "rowSpan": 5
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkLoggingEvent_CL\n| where Message has 'source_system' and Message has \"run_id\"\n| where Level == 'ERROR'\n| extend _job_name = tostring(parse_json(Message).job_name)\n| extend _job_id = tostring(parse_json(Message).job_id)\n| extend _task_run_id = tostring(parse_json(Message).run_id)\n| extend _message = tostring(parse_json(Message).message)\n| project TimeGenerated, _job_name, _job_id, _task_run_id, _message, Level\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "AnalyticsGrid",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "GridColumnsWidth": {},
                  "PartTitle": "Error Logs"
                }
              }
            }
          },
          "6": {
            "position": {
              "x": 0,
              "y": 23,
              "colSpan": 7,
              "rowSpan": 5
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkLoggingEvent_CL\n| where Message contains \"Streaming query made progress\"\n| extend streaming_progress=parse_json(replace_string(Message, \"Streaming query made progress: \", \"\"))\n| extend timestamp = streaming_progress.timestamp\n| extend id = streaming_progress.id\n| extend runId = streaming_progress.runId\n| extend batchId = streaming_progress.batchId\n| extend numInputRows = streaming_progress.numInputRows\n| extend inputRowsPerSecond = streaming_progress.inputRowsPerSecond\n| extend processedRowsPerSecond = streaming_progress.processedRowsPerSecond\n| extend durationMs_latestOffset = streaming_progress.durationMs.latestOffset\n| extend durationMs_triggerExecution = streaming_progress.durationMs.triggerExecution\n| extend sink_description = streaming_progress.sink.description\n| extend sink_dnumOutputRows = streaming_progress.sink.numOutputRows\n| project timestamp, id, runId, batchId, numInputRows, inputRowsPerSecond,processedRowsPerSecond, durationMs_latestOffset, durationMs_triggerExecution, sink_description, sink_dnumOutputRows\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "AnalyticsGrid",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "GridColumnsWidth": {
                    "id": "70.9978px",
                    "runId": "278.997px",
                    "batchId": "76.9967px",
                    "inputRowsPerSecond": "186px",
                    "processedRowsPerSecond": "237.997px",
                    "sink_description": "630.998px",
                    "tableName": "277.999px",
                    "timestamp": "154.998px"
                  },
                  "Query": "SparkLoggingEvent_CL\n| where Message contains \"Streaming query made progress\"\n| extend streaming_progress=parse_json(replace_string(Message, \"Streaming query made progress: \", \"\"))\n| extend timestamp = streaming_progress.timestamp\n| extend id = streaming_progress.id\n| extend runId = streaming_progress.runId\n| extend batchId = streaming_progress.batchId\n| extend tableName = streaming_progress.name\n| extend numInputRows = streaming_progress.numInputRows\n| extend inputRowsPerSecond = streaming_progress.inputRowsPerSecond\n| extend processedRowsPerSecond = streaming_progress.processedRowsPerSecond\n| extend durationMs_latestOffset = streaming_progress.durationMs.latestOffset\n| extend durationMs_triggerExecution = streaming_progress.durationMs.triggerExecution\n| extend sink_description = streaming_progress.sink.description\n| extend sink_dnumOutputRows = streaming_progress.sink.numOutputRows\n| where numInputRows > 0\n| project timestamp, tableName, numInputRows, inputRowsPerSecond,processedRowsPerSecond, id, runId, batchId,durationMs_latestOffset, durationMs_triggerExecution, sink_description, sink_dnumOutputRows, streaming_progress\n\n",
                  "PartTitle": "Streaming Throughput"
                }
              }
            }
          },
          "7": {
            "position": {
              "x": 7,
              "y": 23,
              "colSpan": 7,
              "rowSpan": 5
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkLoggingEvent_CL\n| where Message contains \"Streaming query made progress\"\n| extend streaming_progress = parse_json(replace_string(Message, \"Streaming query made progress: \", \"\"))\n| extend timestamp = tostring(streaming_progress.timestamp)\n| extend id = tostring(streaming_progress.id)\n| extend runId = tostring(streaming_progress.runId)\n| extend name = tostring(streaming_progress.name)\n| extend batchId = tostring(streaming_progress.batchId)\n| extend numInputRows = tostring(streaming_progress.numInputRows)\n| extend inputRowsPerSecond = tostring(streaming_progress.inputRowsPerSecond)\n| extend processedRowsPerSecond = tostring(streaming_progress.processedRowsPerSecond)\n| extend durationMs_latestOffset = tostring(streaming_progress.durationMs.latestOffset)\n| extend durationMs_triggerExecution = tostring(streaming_progress.durationMs.triggerExecution)\n| extend sink_description = tostring(streaming_progress.sink.description)\n| extend sink_numOutputRows = tostring(streaming_progress.sink.numOutputRows)\n| project\n    timestamp,\n    id,\n    runId,\n    name,\n    batchId,\n    numInputRows,\n    inputRowsPerSecond,\n    processedRowsPerSecond,\n    durationMs_latestOffset,\n    durationMs_triggerExecution,\n    sink_description,\n    sink_numOutputRows \n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "AnalyticsGrid",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "GridColumnsWidth": {
                    "timestamp": "164px"
                  },
                  "Query": "SparkLoggingEvent_CL\n| where  Message contains \"Streaming query made progress\"\n| extend streaming_progress=parse_json(replace_string(Message, \"Streaming query made progress: \", \"\"))\n| project TimeGenerated, batchId=streaming_progress.batchId, numInputRows=tolong(streaming_progress.numInputRows), processedRowsPerSecond=streaming_progress.processedRowsPerSecond\n",
                  "ControlType": "FrameControlChart",
                  "SpecificChart": "StackedColumn",
                  "PartTitle": "Streaming Throughput",
                  "Dimensions": {
                    "xAxis": {
                      "name": "TimeGenerated",
                      "type": "datetime"
                    },
                    "yAxis": [
                      {
                        "name": "numInputRows",
                        "type": "long"
                      }
                    ],
                    "splitBy": [],
                    "aggregation": "Sum"
                  },
                  "LegendOptions": {
                    "isEnabled": true,
                    "position": "Bottom"
                  }
                }
              }
            }
          },
          "8": {
            "position": {
              "x": 0,
              "y": 29,
              "colSpan": 12,
              "rowSpan": 5
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceTypeMode",
                  "isOptional": true
                },
                {
                  "name": "ComponentId",
                  "isOptional": true
                },
                {
                  "name": "Scope",
                  "value": {
                    "resourceIds": [
                      "${var.resource_id}"
                    ]
                  },
                  "isOptional": true
                },
                {
                  "name": "PartId",
                  "value": "xxxx",
                  "isOptional": true
                },
                {
                  "name": "Version",
                  "value": "2.0",
                  "isOptional": true
                },
                {
                  "name": "TimeRange",
                  "value": "P1D",
                  "isOptional": true
                },
                {
                  "name": "DashboardId",
                  "isOptional": true
                },
                {
                  "name": "DraftRequestParameters",
                  "isOptional": true
                },
                {
                  "name": "Query",
                  "value": "SparkLoggingEvent_CL\n| where Message has 'psr_msg_type' and Message has 'tracking'\n| extend _message=parse_json(tostring(parse_json(Message).message))\n| extend _cnt=tolong(_message.['cnt'])\n| extend _table_name=_message.['table_name']\n| extend _tmp=strcat(tostring(_message.['ts']),\":00\")\n| extend _ts=split(_tmp,\",\")\n| extend _timet=strcat(_ts[0],_ts[1])\n| extend _time=todatetime(_timet)\n| where _table_name contains \"events\"\n| project  _cnt, _table_name, _time\n| summarize max(_cnt) by _time,tostring(_table_name)\n| order by _time asc nulls last\n",
                  "isOptional": true
                },
                {
                  "name": "ControlType",
                  "value": "FrameControlChart",
                  "isOptional": true
                },
                {
                  "name": "SpecificChart",
                  "value": "Line",
                  "isOptional": true
                },
                {
                  "name": "PartTitle",
                  "value": "Analytics",
                  "isOptional": true
                },
                {
                  "name": "PartSubTitle",
                  "value": "${local.diagnostic_name}",
                  "isOptional": true
                },
                {
                  "name": "Dimensions",
                  "value": {
                    "xAxis": {
                      "name": "_time",
                      "type": "datetime"
                    },
                    "yAxis": [
                      {
                        "name": "max__cnt",
                        "type": "long"
                      }
                    ],
                    "splitBy": [],
                    "aggregation": "Max"
                  },
                  "isOptional": true
                },
                {
                  "name": "LegendOptions",
                  "value": {
                    "isEnabled": true,
                    "position": "Bottom"
                  },
                  "isOptional": true
                },
                {
                  "name": "IsQueryContainTimeRange",
                  "value": false,
                  "isOptional": true
                }
              ],
              "type": "Extension/Microsoft_OperationsManagementSuite_Workspace/PartType/LogsDashboardPart",
              "settings": {
                "content": {
                  "GridColumnsWidth": {
                    "number_of_rows_updated": "206px"
                  },
                  "Query": "SparkLoggingEvent_CL\n| where Message has 'psr_msg_type' and Message has 'tracking'\n| extend _message=parse_json(tostring(parse_json(Message).message))\n| extend _cnt=tolong(_message.['cnt'])\n| extend _table_name=_message.['table_name']\n| extend _tmp=strcat(tostring(_message.['ts']),\":00\")\n| extend _ts=split(_tmp,\",\")\n| extend _timet=strcat(_ts[0],_ts[1])\n| extend _time=todatetime(_timet)\n| project  _cnt, _table_name, _time\n| summarize max(_cnt) by _time,tostring(_table_name)\n| project-rename number_of_rows_updated = max__cnt\n| order by _time asc nulls last\n\n",
                  "ControlType": "AnalyticsGrid",
                  "PartTitle": "Number of Rows Updated in Each Table"
                }
              }
            }
          }
        }
      }
    },
    "metadata": {
      "model": {
        "timeRange": {
          "value": {
            "relative": {
              "duration": 24,
              "timeUnit": 1
            }
          },
          "type": "MsPortalFx.Composition.Configuration.ValueTypes.TimeRange"
        },
        "filterLocale": {
          "value": "en-us"
        },
        "filters": {
          "value": {
            "MsPortalFx_TimeRange": {
              "model": {
                "format": "utc",
                "granularity": "auto",
                "relative": "24h"
              },
              "displayCache": {
                "name": "UTC Time",
                "value": "Past 24 hours"
              },
              "filteredPartIds": [
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx",
                "StartboardPart-LogsDashboardPart-xxxx"
              ]
            }
          }
        }
      }
    }
  }
DASH
}