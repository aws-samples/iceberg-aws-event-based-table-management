package org.apache.iceberg.aws.manage;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_EMR_CLUSTER_ID;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SLEEP_WAIT_MS;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SLEEP_WAIT_MS_DEFAULT;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SYNCHRONOUS_ENABLED;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SYNCHRONOUS_ENABLED_DEFAULT;

/**
 * Base class for all EMR specific Optimize Data Executors
 */
public abstract class BaseEmrOptimizeDataExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(BaseEmrOptimizeDataExecutor.class);

  protected Table table;

  protected String tableName;

  protected boolean synchronous;

  protected int sleepWaitDurationMs;

  protected String emrClusterId;

  protected Map<String, String> properties;

  public void initialize(Table table, Map<String, String> properties) {
    this.table = table;
    this.tableName = table.name();
    this.synchronous =
        PropertyUtil.propertyAsBoolean(
            properties,
            OPTIMIZE_DATA_SYNCHRONOUS_ENABLED,
            OPTIMIZE_DATA_SYNCHRONOUS_ENABLED_DEFAULT);
    this.sleepWaitDurationMs =
        PropertyUtil.propertyAsInt(
            properties,
            OPTIMIZE_DATA_SLEEP_WAIT_MS,
            OPTIMIZE_DATA_SLEEP_WAIT_MS_DEFAULT);
    this.emrClusterId = properties.get(OPTIMIZE_DATA_EMR_CLUSTER_ID);
    Preconditions.checkArgument(
        null != emrClusterId,
        "%s should be be set",
        OPTIMIZE_DATA_EMR_CLUSTER_ID);
    this.properties = properties;
  }

  /**
   * Build the Spark SQL Configurations
   *
   * @return Spark SQL Configurations
   */
  protected List<String> sparkSqlConfigurations(Map<String, String> properties) {
    List<String> configurations = Lists.newArrayList(EMRSparkUtil.buildSparkSqlExtensions());
    configurations.addAll(EMRSparkUtil.buildSparkSqlCatalog(properties));
    configurations.addAll(EMRSparkUtil.buildSparkConfigurations(properties));
    configurations.addAll(EMRSparkUtil.buildSparkIcebergJarPath());
    return configurations;
  }

  /**
   * Uses a {@link FileIO} to create a file with content at a specified path
   *
   * @param fileIO  {@link FileIO}
   * @param content Content to be stored in the file
   * @param path    Path where the file is to be created
   */
  protected void createFile(FileIO fileIO, String content, String path) {
    OutputFile out = fileIO.newOutputFile(path);
    try (OutputStream os = out.createOrOverwrite()) {
      os.write(content.getBytes());
    } catch (IOException e) {
      LOG.error("Exception occurred while writing the {}: ", path, e);
    }
  }
}
