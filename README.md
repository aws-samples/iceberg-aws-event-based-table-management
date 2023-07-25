# Iceberg AWS event based table management

This offering lets users to collect table activities during writes to make better decisions on how to manage each table differently based on events.

Currently, only the `optimize-data` optimization is supported.
Do note that the Iceberg AWS event based table management feature works with Iceberg v1.2.0 and above.

## Build

Users can build the project using maven:

```
$ mvn clean install
...
...
...
$ ls target/iceberg-aws-event-based-table-management-0.1.jar
```

## Manage Iceberg Tables

Users can manage their Iceberg tables by keeping the `iceberg-aws-event-based-table-management` jar in classpath.
For example, to manage the Iceberg table based on events, you can start the Spark 3.3 SQL shell as highlighted below:

### Athena Optimize Data Executor

Please make sure that `athena:StartQueryExecution`, and `athena:GetQueryExecution` permission policy is enabled.

```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=<s3-bucket> \
    --conf spark.sql.catalog.my_catalog.metrics-reporter-impl=org.apache.iceberg.aws.manage.AwsTableManagementMetricsEvaluator \
    --conf spark.sql.catalog.my_catalog.optimize-data.impl=org.apache.iceberg.aws.manage.AthenaOptimizeDataExecutor \
    --conf spark.sql.catalog.my_catalog.optimize-data.athena.output-bucket=<s3-bucket>
```

### EMR on EC2 Optimize Data Executor

Please make sure that `elasticmapreduce:AddJobFlowSteps` permission policy is enabled.

```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=<s3-bucket> \
    --conf spark.sql.catalog.my_catalog.metrics-reporter-impl=org.apache.iceberg.aws.manage.AwsTableManagementMetricsEvaluator \
    --conf spark.sql.catalog.my_catalog.optimize-data.impl=org.apache.iceberg.aws.manage.EmrOnEc2OptimizeDataExecutor \
    --conf spark.sql.catalog.my_catalog.optimize-data.emr.cluster-id=<cluster-id>
```

### EMR on EKS Optimize Data Executor

Please make sure that `emr-containers:StartJobRun`, and `emr-containers:DescribeJobRun` permission policy is enabled.

```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=<s3-bucket> \
    --conf spark.sql.catalog.my_catalog.metrics-reporter-impl=org.apache.iceberg.aws.manage.AwsTableManagementMetricsEvaluator \
    --conf spark.sql.catalog.my_catalog.optimize-data.impl=org.apache.iceberg.aws.manage.EmrOnEksOptimizeDataExecutor \
    --conf spark.sql.catalog.my_catalog.optimize-data.emr.cluster-id=<virtual-cluster-id> \
    --conf spark.sql.catalog.my_catalog.optimize-data.iam.execution-role-arn=<iam-role-arn> \
    --conf spark.sql.catalog.my_catalog.optimize-data.emr.release-label=emr-6.10.0-latest \
    --conf spark.sql.catalog.my_catalog.optimize-data.emr.upload-bucket=<s3-bucket>
```

### EMR Serverless Optimize Data Executor 

Please make sure that `emr-serverless:StartJobRun`, and `emr-serverless:GetJobRun` permission policy is enabled.

```
spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=<s3-bucket> \
    --conf spark.sql.catalog.my_catalog.metrics-reporter-impl=org.apache.iceberg.aws.manage.AwsTableManagementMetricsEvaluator \
    --conf spark.sql.catalog.my_catalog.optimize-data.impl=org.apache.iceberg.aws.manage.EmrServerlessOptimizeDataExecutor \
    --conf spark.sql.catalog.my_catalog.optimize-data.emr.cluster-id=<application-id> \
    --conf spark.sql.catalog.my_catalog.optimize-data.iam.execution-role-arn=<iam-role-arn> \
    --conf spark.sql.catalog.my_catalog.optimize-data.emr.upload-bucket=<s3-bucket>
```

### Catalog Properties

The following catalog properties are available to manage an Iceberg table:

| Catalog Property                          | Description                                                                                                                                                                                                                                                                   | Mandatory                                                                                                                | Default Value                                                                                                                                  |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| optimize-data.commit-threshold            | Number of commits since the last optimize. If this threshold is met then the optimize data job gets triggered.                                                                                                                                                                | No                                                                                                                       | 10                                                                                                                                             |
| optimize-data.time-threshold-ms           | Time in milli seconds since the last optimize. If this threshold is met then the optimize data job gets triggered.                                                                                                                                                            | No                                                                                                                       | 10800000 (3 hours)                                                                                                                             |                                                                                              
| optimize-data.synchronous-enabled         | Flag to enable synchronous optimize.                                                                                                                                                                                                                                          | No                                                                                                                       | False                                                                                                                                          |
| optimize-data.sleep-wait-ms               | Sleep time while checking status of the optimize data job. This comes into effect when `optimize-data.synchronous-enabled` is set as `true`.                                                                                                                                  | No                                                                                                                       | 2000                                                                                                                                           |                        
| optimize-data.impl                        | Implementation which decides in which engine the optimize data job should be triggered. Available implementations are: `AthenaOptimizeDataExecutor`, `EmrOnEc2OptimizeDataExecutor`, `EmrOnEksOptimizeDataExecutor`, `EmrServerlessOptimizeDataExecutor`.                     | Yes                                                                                                                      |                                                                                                                                                |
| optimize-data.options.<compaction-option> | Optimize options with which the optimize data job is launched. For example: `optimize-data.options.partial-progress.enabled=true`. Please refer applicable options: https://iceberg.apache.org/javadoc/1.1.0/org/apache/iceberg/actions/OptimizeDataFiles.html#field.summary. | No                                                                                                                       | partial-progress.enabled=true, max-file-group-size-bytes=10737418240                                                                           |
| optimize-data.spark.configs.<spark-conf>  | Spark configurations with which the optimize data job is launched. For example: `optimize-data.spark.configs.spark.executor.instances=10`. Please refer applicable configurations: https://spark.apache.org/docs/latest/configuration.html.                                   | No                                                                                                                       | spark.driver.memory=32g, spark.executor.cores=4, spark.executor.memory=16g, spark.executor.instances=10, spark.dynamicAllocation.enabled=false |
| optimize-data.strategy                    | Optimize strategy. Applicable values are binpack, sort.                                                                                                                                                                                                                       | No                                                                                                                       | binpack                                                                                                                                        |
| optimize-data.sort-order                  | Sort order for the optimize job. Applicable only when `optimize-data.strategy=sort`.                                                                                                                                                                                          | No                                                                                                                       |                                                                                                                                                |
| optimize-data.athena.output-bucket        | `AthenaOptimizeDataExecutor` uses the Athena output bucket name to store the query results.                                                                                                                                                                                   | Yes, when `AthenaOptimizeDataExecutor` is used                                                                           |                                                                                                                                                |
| optimize-data.athena.data-catalog         | Athena data catalog used by `AthenaOptimizeDataExecutor` to execute the optimize job.                                                                                                                                                                                         | No                                                                                                                       | AwsDataCatalog                                                                                                                                 |
| optimize-data.emr.cluster-id              | EMR cluster id for `EmrOnEc2OptimizeDataExecutor`, virtual cluster id for `EmrOnEksOptimizeDataExecutor`, and application id for `EmrServerlessOptimizeDataExecutor`.                                                                                                         | Yes, when `EmrOnEc2OptimizeDataExecutor`, `EmrOnEksOptimizeDataExecutor`, or `EmrServerlessOptimizeDataExecutor` is used |                                                                                                                                                |
| optimize-data.iam.execution-role-arn      | IAM execution role ARN which is used by `EmrOnEksOptimizeDataExecutor` and `EmrServerlessOptimizeDataExecutor` to execute the optimize job.                                                                                                                                   | Yes, when `EmrOnEksOptimizeDataExecutor` and `EmrServerlessOptimizeDataExecutor` is used                                 |                                                                                                                                                |
| optimize-data.emr.release-label           | EMR release label which is used by `EmrOnEksOptimizeDataExecutor` to execute the optimize job.                                                                                                                                                                                | Yes, when `EmrOnEksOptimizeDataExecutor` is used                                                                         |                                                                                                                                                |
| optimize-data.emr.upload-bucket           | `EmrOnEksOptimizeDataExecutor` and `EmrServerlessOptimizeDataExecutor` uses this bucket to upload any runtime resources.                                                                                                                                                      | Yes, when `EmrOnEksOptimizeDataExecutor` and `EmrServerlessOptimizeDataExecutor` is used                                 |                                                                                                                                                |
		 