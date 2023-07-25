package org.apache.iceberg.aws.manage;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Class to hold the related catalog properties.
 */
public class AwsManageProperties {

  public static final String OPTIMIZE_DATA_COMMIT_THRESHOLD = "optimize-data.commit-threshold";

  public static final int OPTIMIZE_DATA_COMMIT_THRESHOLD_DEFAULT = 10;

  public static final String OPTIMIZE_DATA_TIME_THRESHOLD_MS = "optimize-data.time-threshold-ms";

  // Default time threshold is 3 hours
  public static final int OPTIMIZE_DATA_TIME_THRESHOLD_MS_DEFAULT = 180 * 60 * 1000;

  public static final String OPTIMIZE_DATA_SYNCHRONOUS_ENABLED = "optimize-data.synchronous-enabled";

  public static final boolean OPTIMIZE_DATA_SYNCHRONOUS_ENABLED_DEFAULT = false;

  public static final String OPTIMIZE_DATA_IMPL = "optimize-data.impl";

  public static final String OPTIMIZE_DATA_SLEEP_WAIT_MS = "optimize-data.sleep-wait-ms";

  // Default sleep wait time is 2000 ms
  public static final int OPTIMIZE_DATA_SLEEP_WAIT_MS_DEFAULT = 2000;

  public static final String OPTIMIZE_DATA_ATHENA_OUTPUT_BUCKET = "optimize-data.athena.output-bucket";

  public static final String OPTIMIZE_DATA_ATHENA_DATA_CATALOG = "optimize-data.athena.data-catalog";

  public static final String OPTIMIZE_DATA_ATHENA_DATA_CATALOG_DEFAULT = "AwsDataCatalog";

  public static final String OPTIMIZE_DATA_EMR_CLUSTER_ID = "optimize-data.emr.cluster-id";

  public static final String OPTIMIZE_DATA_IAM_EXECUTION_ROLE_ARN = "optimize-data.iam.execution-role-arn";

  public static final String OPTIMIZE_DATA_EMR_RELEASE_LABEL = "optimize-data.emr.release-label";

  public static final String OPTIMIZE_DATA_EMR_UPLOAD_BUCKET = "optimize-data.emr.upload-bucket";

  public static final String OPTIMIZE_DATA_OPTIONS_PREFIX = "optimize-data.options.";

  // Partial progress is enabled along with 10G file group size as default
  public static final Map<String, String> OPTIMIZE_DATA_OPTIONS_DEFAULT =
      ImmutableMap.of("partial-progress.enabled", "true",
          "max-file-group-size-bytes", "10737418240");

  public static final String OPTIMIZE_DATA_STRATEGY = "optimize-data.strategy";

  public static final String OPTIMIZE_DATA_STRATEGY_DEFAULT = "binpack";

  public static final String OPTIMIZE_DATA_SORT_ORDER = "optimize-data.sort-order";

  public static final String OPTIMIZE_DATA_SPARK_CONFIGS_PREFIX = "optimize-data.spark.configs.";

  public static final Map<String, String> OPTIMIZE_DATA_SPARK_CONFIGURATIONS_DEFAULT =
      ImmutableMap.of("spark.driver.cores", "4",
          "spark.driver.memory", "32g",
          "spark.executor.cores", "4",
          "spark.executor.memory", "16g",
          "spark.executor.instances", "10",
          "spark.dynamicAllocation.enabled", "false");
}
