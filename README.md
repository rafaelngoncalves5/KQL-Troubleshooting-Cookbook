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
