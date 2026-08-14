# KQL Troubleshooting Cookbook

## Core Telemetry Tables

### ASTrace
Execution trace telemetry from Analysis Services.

**Use for:**
- Execution verification
- Performance bottlenecks
- Failure, timeout, or throttling analysis
- RootActivityId correlation

### Evt5653745998362568492All
Cross-cluster engine snapshot view.

**Use for:**
- Dataset size history
- Model growth analysis
- Metadata state analysis
- Capacity history

### ASEngineReportModelTraits
Model classification metadata.

### MonVdwQueryOperation
Query execution telemetry.

### CapacityCpuPerWorkloadOperation
CPU consumption by workload, artifact, and execution.

---

## Capacity Load Graph
```kusto
let StartTime = datetime(2024-04-08 0:00:00);
let FinishTime = datetime(2024-04-17 0:00:00);
CapacityCpuPerWorkloadSummary
| where TIMESTAMP >= StartTime and TIMESTAMP < FinishTime
| where PremiumCapacityId == "B7347A3E-D59F-4D90-B637-A18B2E0DDC5A"
| extend UpdatedBaseCoreCount = case(PremiumCapacitySku in ('A1','EM1'),0.5, PremiumCapacitySku in ('A2','EM2'),1.0, BaseCoreCount * 1.0)
| extend UpdateAutoScaleCoreCount = iff(AutoScaleCoreCount < 1.0, 0.0, AutoScaleCoreCount / 2.0)
| extend TotalCoreCount = UpdateAutoScaleCoreCount + UpdatedBaseCoreCount
| extend MaxCpuMS = TotalCoreCount * 30.0 * 1000.0
| extend PercentageUsage = (CpuTimeMs * 100.0 / MaxCpuMS)
| project TIMESTAMP, PercentageUsage
| render timechart
```

## Error Investigation
```kusto
Trace
| where RootActivityId == "<RootActivityId>"
| project TIMESTAMP, EventText, Level, InstanceId, ActivityType, TraceActivityId, RootActivityId, ClientActivityId
| where Level <= 3
```

## Dataset Refresh History
```kusto
let pbiDataset = '<DatasetId>';
let StartTime = datetime(2024-03-01 0:00:00);
let EndTime = datetime(2024-03-26 0:00:00);
cluster('wabinam.kusto.windows.net').database('BIAzureKustoProd').FormattedTrace
| where TIMESTAMP between (StartTime .. EndTime)
| where ActivityType in ('WRLA', 'WMRM')
| where EventText hasprefix 'Event: FireActivityCompleted' and EventText has pbiDataset
```

## Pipeline Check
```kusto
cluster('adfcus.kusto.windows.net').database('AzureDataFactory').ActivityRuns
| union cluster('adfneu.kusto.windows.net').database('AzureDataFactory').ActivityRuns
| where pipelineName has '<PipelineId>'
| where status has 'Failed'
```

## God Mode
```kusto
let AllWabiTrace = union
cluster('https://wabinam.kusto.windows.net').database('BIAzureKustoProd').FormattedTrace,
cluster('https://wabicentralus.centralus.kusto.windows.net').database('BIAzureKustoProd').FormattedTrace,
cluster('https://wabieastus.eastus.kusto.windows.net').database('BIAzureKustoProd').FormattedTrace,
cluster('https://biazure.kusto.windows.net').database('BIAzureKustoProd').FormattedTrace;
```
### Check if dw is under pressure
```kusto
// sqlazue
MonVdwWorkloadManager
| where LogicalServerName == 'd0c42ca3-ea35-4595-be45-19f2c80ad68c'
| where originalEventTimestamp > ago(1d)
//let startTime = datetime(2026-06-12T08:18:14.9656746);
//let endTime = datetime(2026-06-12T08:18:15.9835508);
| where event == "wg_load"
| where wg_name == "SELECT"
| project originalEventTimestamp, wg_name, wg_status, number_queries_executing, number_queries_waiting, wg_action_in_state, wg_allocations, demand_topology_size
| order by originalEventTimestamp desc
```
### Resource demanding queries
```kusto
// 3) Top resource-demanding queries
let poolName = '9ad489b8-6ae7-4b89-ac10-dbae66953980';
let startTime = datetime(2026-08-12 21:54);
let endTime = datetime(2026-08-14 21:54);
let workloadGroup = 'SELECT';
let SingleNodeCapacity=MonVdwConfigurations
| where LogicalServerName == poolName
| where todatetime(originalEventTimestamp) < endTime
| where service_name == 'dqp'
| where service_config_context == 'all_except_wlm'
| where event == 'report_configuration'
| top 1 by originalEventTimestamp
| project j = parse_json(service_config_text)
| project max_topology=j.WorkloadGroupDefaultSettings.TopologySettings.MaxSize, singleNodeCapacity=j.ClusterConfiguration.ClusterViewConfigurations[0].ComputeNodeCapacity
| project SingNodeCpu=coalesce(singleNodeCapacity.CpuWeight, singleNodeCapacity.C, singleNodeCapacity.BaseCapacity.C),
    SingNodeMemory=coalesce(singleNodeCapacity.MemoryWeight, singleNodeCapacity.M, singleNodeCapacity.BaseCapacity.M),
    SingNodeDisk=coalesce(singleNodeCapacity.DiskWeight, singleNodeCapacity.D, singleNodeCapacity.BaseCapacity.D);
let SingNodeCpu=todouble(toscalar(SingleNodeCapacity|project SingNodeCpu));
let SingNodeMemory=todouble(toscalar(SingleNodeCapacity|project SingNodeMemory));
let SingNodeDisk=todouble(toscalar(SingleNodeCapacity|project SingNodeDisk));
let data = materialize(MonVdwWorkloadManager
| where originalEventTimestamp between (startTime .. endTime)
| where LogicalServerName == poolName
| where wg_name == workloadGroup
| where event == 'hypergraph'
| where action in ('InSnapshot', 'AwaitingSnapshot')
| extend distributed_statement_id=case(isempty(target_query), substring(target_query,6, 38),substring(['queries'],6, 38))
| extend distributed_query_hash=case(isempty(target_query), substring(target_query,54, 18),substring(['queries'],54, 18))
| summarize arg_min(originalEventTimestamp, *) by distributed_statement_id
| project originalEventTimestamp, action, distributed_statement_id, distributed_query_hash, graph_resource_demand, target_query, ['queries'], wg_name
| extend j = parse_json(graph_resource_demand)
| extend LocalityAbsoluteDemand=j.resourceDemands.Locality.MaxParallelismPipelineAbsoluteDemand, LocalityNormalizedDemand=j.resourceDemands.Locality.MaxParallelismPipelineNormalizedDemand
| extend UtilityAbsoluteDemand=j.resourceDemands.Utility.MaxParallelismPipelineAbsoluteDemand, UtilityNormalizedDemand=j.resourceDemands.Utility.MaxParallelismPipelineNormalizedDemand
| extend LocalityCpu = coalesce(LocalityAbsoluteDemand.CpuWeight, LocalityAbsoluteDemand.C)/SingNodeCpu, LocalityMem = coalesce(LocalityAbsoluteDemand.MemoryWeight,LocalityAbsoluteDemand.M)/SingNodeMemory, LocalityDisk = coalesce(LocalityAbsoluteDemand.DiskWeight, LocalityAbsoluteDemand.D)/SingNodeDisk
| extend UtilityCpu = coalesce(UtilityAbsoluteDemand.CpuWeight, UtilityAbsoluteDemand.C)/SingNodeCpu, UtilityMem = coalesce(UtilityAbsoluteDemand.MemoryWeight, UtilityAbsoluteDemand.M)/SingNodeMemory, UtilityDisk = coalesce(UtilityAbsoluteDemand.DiskWeight, UtilityAbsoluteDemand.D)/SingNodeDisk
| extend LocalityAndUtilityCpu = LocalityCpu + UtilityCpu
| project-away graph_resource_demand, target_query, ['queries'],j, LocalityAbsoluteDemand,UtilityAbsoluteDemand, LocalityNormalizedDemand, UtilityNormalizedDemand);
data
| order by LocalityAndUtilityCpu, LocalityCpu, UtilityCpu
 // Dummy comment to prevent an empty line in case FilterClause is empty.
| take 100
```
