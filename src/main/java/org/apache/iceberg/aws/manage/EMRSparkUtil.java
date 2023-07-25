package org.apache.iceberg.aws.manage;

import java.util.Iterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_OPTIONS_PREFIX;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_OPTIONS_DEFAULT;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SORT_ORDER;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SPARK_CONFIGS_PREFIX;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SPARK_CONFIGURATIONS_DEFAULT;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_STRATEGY;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_STRATEGY_DEFAULT;

/**
 * Utility class to hold Spark related method to manage an Iceberg table.
 */
public class EMRSparkUtil {

  private static final String SPARK_CONF_FLAG = "--conf";

  public static final String SPARK_CATALOG_NAME = "aws_ebtm";

  public static final String SPARK_SQL = "spark-sql";

  public static final String SPARK_SQL_EXECUTE_FLAG = "-e";

  /**
   * Build Spark SQL extensions configuration
   *
   * @return Spark SQL extensions configuration
   */
  public static List<String> buildSparkSqlExtensions() {
    return ImmutableList.of(
        SPARK_CONF_FLAG,
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
  }

  /**
   * Build Spark SQL catalog configuration
   *
   * @param properties Catalog properties
   * @return Spark SQL Catalog configuration
   */
  public static List<String> buildSparkSqlCatalog(Map<String, String> properties) {
    List<String> catalog = Lists.newArrayList(
        SPARK_CONF_FLAG,
        String.format("spark.sql.catalog.%s=org.apache.iceberg.spark.SparkCatalog", SPARK_CATALOG_NAME));
    for (Map.Entry<String, String> property : properties.entrySet()) {
      catalog.add(SPARK_CONF_FLAG);
      catalog.add(String.format(
          "spark.sql.catalog.%s.%s=%s",
          SPARK_CATALOG_NAME,
          property.getKey(),
          property.getValue()));
    }
    return catalog;
  }

  /**
   * Build Spark SQL job configurations. Users can override the Spark SQL job configurations
   * using {@link org.apache.iceberg.aws.manage.AwsManageProperties#OPTIMIZE_DATA_SPARK_CONFIGS_PREFIX}
   * property.
   *
   * @param properties Catalog properties
   * @return Default Spark SQL job configurations
   */
  public static List<String> buildSparkConfigurations(Map<String, String> properties) {
    Map<String, String> sparkConfigurations =
        PropertyUtil.propertiesWithPrefix(properties, OPTIMIZE_DATA_SPARK_CONFIGS_PREFIX);
    List<String> configurations = Lists.newArrayList();
    if (sparkConfigurations.isEmpty()) {
      sparkConfigurations = OPTIMIZE_DATA_SPARK_CONFIGURATIONS_DEFAULT;
    }

    for (Map.Entry<String, String> sparkConfiguration : sparkConfigurations.entrySet()) {
      configurations.add(SPARK_CONF_FLAG);
      configurations.add(String.format(sparkConfiguration.getKey()
          .concat("=")
          .concat(sparkConfiguration.getValue())));
    }
    return configurations;
  }

  /**
   * Build Spark SQL optimize data command. Users can configure the command using
   * {@link AwsManageProperties#OPTIMIZE_DATA_OPTIONS_PREFIX},
   * {@link AwsManageProperties#OPTIMIZE_DATA_STRATEGY},
   * {@link AwsManageProperties#OPTIMIZE_DATA_SORT_ORDER} properties. For
   * example, the resulting Spark SQL optimize data command will be like CALL
   * catalog_name.system.rewrite_data_files(table => 'db_name.table_name', options =>
   * map('partial-progress.enabled','true'), strategy => 'binpack')
   *
   * @param tableName  Table Name
   * @param properties Catalog properties
   * @return Spark SQL optimize data command
   */
  public static List<String> buildSparkSqlOptimizeDataFilesCommand(String tableName, Map<String, String> properties) {
    String options = buildSparkSqlOptimizeDataFilesOptions(properties);
    String strategy =
        PropertyUtil.propertyAsString(
            properties,
            OPTIMIZE_DATA_STRATEGY,
            OPTIMIZE_DATA_STRATEGY_DEFAULT);
    String sortOrder = properties.get(OPTIMIZE_DATA_SORT_ORDER);
    // Build the Spark procedure SQL
    StringBuilder OptimizeDataFilesCommand = new StringBuilder();
    OptimizeDataFilesCommand.append(String.format("CALL %s.system.rewrite_data_files", SPARK_CATALOG_NAME));
    OptimizeDataFilesCommand.append("(");
    OptimizeDataFilesCommand.append(String.format("table => '%s'", tableName));
    if (!options.isEmpty()) {
      OptimizeDataFilesCommand.append(String.format(", options => %s", options));
    }

    if (strategy != null && !strategy.isEmpty()) {
      OptimizeDataFilesCommand.append(String.format(", strategy => '%s'", strategy));
    }

    if (sortOrder != null && !sortOrder.isEmpty()) {
      OptimizeDataFilesCommand.append(String.format(", sort_order => '%s'", sortOrder));
    }

    OptimizeDataFilesCommand.append(")");
    return ImmutableList.of(OptimizeDataFilesCommand.toString());
  }

  /**
   * Build Spark SQL optimize data options
   *
   * @param properties Catalog Properties
   * @return Optimize job options, for example: map('partial-progress.enabled','true')
   */
  public static String buildSparkSqlOptimizeDataFilesOptions(Map<String, String> properties) {
    Map<String, String> options =
        PropertyUtil.propertiesWithPrefix(properties, OPTIMIZE_DATA_OPTIONS_PREFIX);
    if (options.isEmpty()) {
      options = OPTIMIZE_DATA_OPTIONS_DEFAULT;
    }

    StringBuilder optionStringBuilder = new StringBuilder();
    Iterator<Map.Entry<String, String>> entryIterator = options.entrySet().iterator();
    optionStringBuilder.append("map(");
    while (entryIterator.hasNext()) {
      Map.Entry<String, String> entry = entryIterator.next();
      optionStringBuilder
          .append("'")
          .append(entry.getKey())
          .append("'")
          .append(",'")
          .append(entry.getValue())
          .append("'");
      if (entryIterator.hasNext()) {
        optionStringBuilder.append(",");
      }
    }

    optionStringBuilder.append(")");
    return optionStringBuilder.toString();
  }

  /**
   * Build Spark Kubernetes file upload path configuration
   *
   * @param path Path where the files are to be uploaded
   * @return Spark Kubernetes file upload path configuration
   */
  public static List<String> buildSparkKubernetesFileUploadPath(String path) {
    return ImmutableList.of(SPARK_CONF_FLAG, String.format("spark.kubernetes.file.upload.path=%s", path));
  }

  /**
   * Build Iceberg jar path
   *
   * @return Iceberg jar path
   */
  public static List<String> buildSparkIcebergJarPath() {
    return ImmutableList.of("--jars", "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar");
  }
}
