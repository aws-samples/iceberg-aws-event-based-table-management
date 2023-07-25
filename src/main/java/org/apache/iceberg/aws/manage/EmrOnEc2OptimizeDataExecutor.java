package org.apache.iceberg.aws.manage;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsResponse;
import software.amazon.awssdk.services.emr.model.DescribeStepRequest;
import software.amazon.awssdk.services.emr.model.DescribeStepResponse;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.StepConfig;
import software.amazon.awssdk.services.emr.model.StepState;

/**
 * An implementation of {@link Executor} to optimize data by launching Spark SQL jobs in EMR-on-EC2
 */
public class EmrOnEc2OptimizeDataExecutor extends BaseEmrOptimizeDataExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(EmrOnEc2OptimizeDataExecutor.class);

  private static final String EMR_STEP_CONTINUE = "CONTINUE";

  private static final String COMMAND_RUNNER_JAR = "command-runner.jar";

  private static final int EMR_STEP_INDEX = 0;

  private EmrClient emrClient;

  @Override
  public void initialize(Table table, Map<String, String> properties) {
    super.initialize(table, properties);
    this.emrClient = EmrClient.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
  }

  @Override
  public void execute() {
    String stepId = submitEmrStep();
    if (synchronous) {
      waitForStepToComplete(stepId);
    }
    emrClient.close();
  }

  /**
   * Submit an EMR step to an EMR cluster
   *
   * @return EMR step id
   */
  private String submitEmrStep() {
    AddJobFlowStepsResponse response =
        emrClient.addJobFlowSteps(
            AddJobFlowStepsRequest.builder()
                .jobFlowId(emrClusterId)
                .steps(
                    StepConfig.builder()
                        .name(String.format("optimize data job for %s", tableName))
                        .actionOnFailure(EMR_STEP_CONTINUE)
                        .hadoopJarStep(
                            HadoopJarStepConfig.builder()
                                .jar(COMMAND_RUNNER_JAR)
                                .args(buildArguments().toArray(new String[0]))
                                .build())
                        .build())
                .build());
    if (response != null && response.hasStepIds()) {
      String stepId = response.stepIds().get(EMR_STEP_INDEX);
      LOG.info(
          "Optimize job submitted for table {} on EMR-EC2 cluster {} (step {})",
          tableName,
          emrClusterId,
          stepId);
      return stepId;
    }
    return null;
  }

  /**
   * Build the {@link HadoopJarStepConfig} arguments
   *
   * @return {@link HadoopJarStepConfig} arguments
   */
  private List<String> buildArguments() {
    List<String> arguments = Lists.newArrayList(EMRSparkUtil.SPARK_SQL);
    arguments.addAll(sparkSqlConfigurations(properties));
    arguments.add(EMRSparkUtil.SPARK_SQL_EXECUTE_FLAG);
    arguments.addAll(EMRSparkUtil.buildSparkSqlOptimizeDataFilesCommand(tableName, properties));
    return arguments;
  }

  /**
   * Checks job status regularly for a specific EMR step id
   *
   * @param stepId EMR step id
   */
  private void waitForStepToComplete(String stepId) {
    Preconditions.checkArgument(null != stepId, "Invalid step id: null");
    StepState stepState = stepState(emrClusterId, stepId);
    while (stepState == StepState.PENDING || stepState == StepState.RUNNING) {
      stepState = stepState(emrClusterId, stepId);
      LOG.info("Status of optimize data job: {}", stepState);
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
   * Get EMR {@link StepState} for a given cluster id and step id
   *
   * @param clusterId EMR cluster id
   * @param stepId    EMR step id
   * @return EMR {@link StepState}
   */
  private StepState stepState(String clusterId, String stepId) {
    DescribeStepResponse describeStepResponse =
        emrClient.describeStep(
            DescribeStepRequest.builder().clusterId(clusterId).stepId(stepId).build());
    return describeStepResponse.step().status().state();
  }
}