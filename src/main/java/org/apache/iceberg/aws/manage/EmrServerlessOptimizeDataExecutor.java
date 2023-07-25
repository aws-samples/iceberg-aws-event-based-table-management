package org.apache.iceberg.aws.manage;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.GetJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.GetJobRunResponse;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;

import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_EMR_UPLOAD_BUCKET;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN;

/**
 * An implementation of {@link Executor} to optimize data by launching Spark SQL jobs in EMR Serverless
 */
public class EmrServerlessOptimizeDataExecutor extends BaseEmrOptimizeDataExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(EmrServerlessOptimizeDataExecutor.class);

  private EmrServerlessClient emrServerlessClient;

  private String emrUploadBucket;

  private String executionRoleArn;

  private String queryFilePath;

  private static final List<String> PYTHON_JOB_TEMPLATE = ImmutableList.of(
      "from pyspark.sql import SparkSession",
      "spark = (SparkSession.builder.getOrCreate())",
      "spark.sql(\"%s\").show()");

  @Override
  public void initialize(Table table, Map<String, String> properties) {
    super.initialize(table, properties);
    this.emrServerlessClient =
        EmrServerlessClient.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
    this.executionRoleArn = properties.get(OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN);
    Preconditions.checkArgument(
        null != executionRoleArn,
        "%s should be be set",
        OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN);
    this.emrUploadBucket = properties.get(OPTIMIZE_DATA_EMR_UPLOAD_BUCKET);
    Preconditions.checkArgument(
        null != emrUploadBucket,
        "%s should be be set",
        OPTIMIZE_DATA_EMR_UPLOAD_BUCKET);
    this.queryFilePath =
        String.format("%s/query/query-%s.py", emrUploadBucket, java.util.UUID.randomUUID());
  }

  @Override
  public void execute() {
    createFile(table.io(), buildFileContent(), queryFilePath);
    String jobId = submitJob();
    if (synchronous) {
      waitForJobToComplete(jobId);
    }
    emrServerlessClient.close();
  }

  /**
   * Submit a job to an EMR Serverless cluster
   *
   * @return job id
   */
  private String submitJob() {
    StartJobRunResponse response =
        emrServerlessClient.startJobRun(
            StartJobRunRequest.builder()
                .name(String.format("OptimizeJob-%s", tableName))
                .applicationId(emrClusterId)
                .executionRoleArn(executionRoleArn)
                .jobDriver(
                    JobDriver.builder()
                        .sparkSubmit(
                            SparkSubmit.builder()
                                .entryPoint(queryFilePath)
                                .sparkSubmitParameters(buildSparkSqlParameters())
                                .build())
                        .build())
                .configurationOverrides(
                    ConfigurationOverrides.builder()
                        .monitoringConfiguration(
                            MonitoringConfiguration.builder()
                                .s3MonitoringConfiguration(
                                    S3MonitoringConfiguration.builder()
                                        .logUri(String.format("%s/logs/", emrUploadBucket))
                                        .build())
                                .build())
                        .build())
                .build());
    if (response != null) {
      String jobId = response.jobRunId();
      LOG.info(
          "Optimize job submitted for table {} on EMR Serverless cluster {} (job id {})",
          tableName,
          emrClusterId,
          jobId);
      return jobId;
    }
    return null;
  }

  /**
   * Build the file content
   *
   * @return File content
   */
  private String buildFileContent() {
    StringBuilder fileContent = new StringBuilder();
    for (String parameter : PYTHON_JOB_TEMPLATE) {
      fileContent.append(parameter);
      fileContent.append("\n");
    }

    StringBuilder query = new StringBuilder();
    for (String parameter : EMRSparkUtil.buildSparkSqlOptimizeDataFilesCommand(tableName, properties)) {
      query.append(parameter).append(" ");
    }
    return String.format(fileContent.toString(), query);
  }

  /**
   * Build the Spark SQL Parameters
   *
   * @return Spark SQL Parameters
   */
  private String buildSparkSqlParameters() {
    List<String> parameters = sparkSqlConfigurations(properties);
    StringBuilder stringBuilder = new StringBuilder();
    for (String parameter : parameters) {
      stringBuilder.append(parameter).append(" ");
    }
    return stringBuilder.toString();
  }

  /**
   * Checks job status regularly for a specific EMR job id
   */
  private void waitForJobToComplete(String jobId) {
    Preconditions.checkArgument(null != jobId, "Invalid job id: null");
    JobRunState jobRunState = jobRunState(emrClusterId, jobId);
    while (jobRunState == JobRunState.SUBMITTED
        || jobRunState == JobRunState.PENDING
        || jobRunState == JobRunState.SCHEDULED
        || jobRunState == JobRunState.RUNNING) {
      jobRunState = jobRunState(emrClusterId, jobId);
      LOG.info("Status of optimize data job: {}", jobRunState);
      try {
        // Sleep an amount of time before retrying again
        Thread.sleep(sleepWaitDurationMs);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            String.format("Failed to request status of the optimize data job for table %s", tableName),
            e);
      }
    }
  }

  /**
   * Get EMR {@link JobRunState} for a given cluster id and job id
   *
   * @param clusterId EMR cluster id
   * @param jobId     EMR job id
   * @return EMR {@link JobRunState}
   */
  private JobRunState jobRunState(String clusterId, String jobId) {
    GetJobRunResponse getJobRunResponse =
        emrServerlessClient.getJobRun(
            GetJobRunRequest.builder().applicationId(clusterId).jobRunId(jobId).build());
    return getJobRunResponse.jobRun().state();
  }
}
