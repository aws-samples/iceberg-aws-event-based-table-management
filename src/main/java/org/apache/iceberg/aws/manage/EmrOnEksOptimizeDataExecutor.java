package org.apache.iceberg.aws.manage;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.emrcontainers.EmrContainersClient;
import software.amazon.awssdk.services.emrcontainers.model.CloudWatchMonitoringConfiguration;
import software.amazon.awssdk.services.emrcontainers.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrcontainers.model.DescribeJobRunRequest;
import software.amazon.awssdk.services.emrcontainers.model.DescribeJobRunResponse;
import software.amazon.awssdk.services.emrcontainers.model.JobDriver;
import software.amazon.awssdk.services.emrcontainers.model.JobRunState;
import software.amazon.awssdk.services.emrcontainers.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrcontainers.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrcontainers.model.SparkSqlJobDriver;
import software.amazon.awssdk.services.emrcontainers.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrcontainers.model.StartJobRunResponse;

import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_EMR_RELEASE_LABEL;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_EMR_UPLOAD_BUCKET;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN;

/**
 * An implementation of {@link Executor} to optimize data by launching Spark SQL jobs in EMR-on-EKS
 */
public class EmrOnEksOptimizeDataExecutor extends BaseEmrOptimizeDataExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(EmrOnEksOptimizeDataExecutor.class);

  private static final String PERSISTENT_APP_UI_ENABLED = "ENABLED";

  private static final String EMR_CONTAINERS_LOG_GROUP_NAME = "/aws/emr-containers";

  private static final String EMR_CONTAINERS_LOG_PREFIX = "iceberg";

  private EmrContainersClient emrContainersClient;

  private String emrReleaseLabel;

  private String emrUploadBucket;

  private String executionRoleArn;

  private String queryFilePath;

  @Override
  public void initialize(Table table, Map<String, String> properties) {
    super.initialize(table, properties);
    this.emrContainersClient =
        EmrContainersClient.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
    this.executionRoleArn = properties.get(OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN);
    Preconditions.checkArgument(
        null != executionRoleArn,
        "%s should be be set",
        OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN);
    this.emrReleaseLabel = properties.get(OPTIMIZE_DATA_EMR_RELEASE_LABEL);
    Preconditions.checkArgument(
        null != emrReleaseLabel,
        "%s should be be set",
        OPTIMIZE_DATA_EMR_RELEASE_LABEL);
    this.emrUploadBucket = properties.get(OPTIMIZE_DATA_EMR_UPLOAD_BUCKET);
    Preconditions.checkArgument(
        null != emrUploadBucket,
        "%s should be be set",
        OPTIMIZE_DATA_EMR_UPLOAD_BUCKET);
    this.queryFilePath =
        String.format("%s/query/query-%s.sql", emrUploadBucket, java.util.UUID.randomUUID());
  }

  @Override
  public void execute() {
    createFile(table.io(), buildFileContent(), queryFilePath);
    String jobId = submitJob();
    if (synchronous) {
      waitForJobToComplete(jobId);
    }
    emrContainersClient.close();
  }

  /**
   * Submit a job to an EMR-on-EKS cluster
   *
   * @return job id
   */
  private String submitJob() {
    StartJobRunResponse response =
        emrContainersClient.startJobRun(
            StartJobRunRequest.builder()
                .name(String.format("OptimizeJob-%s", tableName))
                .virtualClusterId(emrClusterId)
                .executionRoleArn(executionRoleArn)
                .releaseLabel(emrReleaseLabel)
                .jobDriver(
                    JobDriver.builder()
                        .sparkSqlJobDriver(
                            SparkSqlJobDriver.builder()
                                .entryPoint(queryFilePath)
                                .sparkSqlParameters(buildSparkSqlParameters())
                                .build())
                        .build())
                .configurationOverrides(
                    ConfigurationOverrides.builder()
                        .monitoringConfiguration(
                            MonitoringConfiguration.builder()
                                .persistentAppUI(PERSISTENT_APP_UI_ENABLED)
                                .cloudWatchMonitoringConfiguration(
                                    CloudWatchMonitoringConfiguration.builder()
                                        .logGroupName(EMR_CONTAINERS_LOG_GROUP_NAME)
                                        .logStreamNamePrefix(EMR_CONTAINERS_LOG_PREFIX)
                                        .build())
                                .s3MonitoringConfiguration(
                                    S3MonitoringConfiguration.builder()
                                        .logUri(String.format("%s/logs/", emrUploadBucket))
                                        .build())
                                .build())
                        .build())
                .build());
    if (response != null) {
      String jobId = response.id();
      LOG.info(
          "Optimize job submitted for table {} on EMR-on-EKS cluster {} (job id {})",
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
    for (String parameter : EMRSparkUtil.buildSparkSqlOptimizeDataFilesCommand(tableName, properties)) {
      fileContent.append(parameter).append(" ");
    }
    return fileContent.toString();
  }

  /**
   * Build the Spark SQL Parameters
   *
   * @return Spark SQL Parameters
   */
  private String buildSparkSqlParameters() {
    List<String> parameters = sparkSqlConfigurations(properties);
    parameters.addAll(EMRSparkUtil.buildSparkKubernetesFileUploadPath(emrUploadBucket));
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
    Preconditions.checkArgument(null != jobId, "Invalid query job id: null");
    JobRunState jobRunState = jobRunState(emrClusterId, jobId);
    while (jobRunState == JobRunState.SUBMITTED
        || jobRunState == JobRunState.PENDING
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
    DescribeJobRunResponse describeJobRunResponse =
        emrContainersClient.describeJobRun(
            DescribeJobRunRequest.builder().virtualClusterId(clusterId).id(jobId).build());
    return describeJobRunResponse.jobRun().state();
  }
}