package org.apache.iceberg.aws.manage;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_COMMIT_THRESHOLD;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_COMMIT_THRESHOLD_DEFAULT;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_IMPL;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_TIME_THRESHOLD_MS;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_TIME_THRESHOLD_MS_DEFAULT;
import static org.apache.iceberg.aws.manage.EMRSparkUtil.SPARK_CATALOG_NAME;

/**
 * An implementation of {@link MetricsReporter} to decide when to optimize Iceberg tables. The
 * implementation lets users to collect table activities during writes to make better decisions on
 * how to optimize each table differently.
 */
public class AwsTableManagementMetricsEvaluator implements MetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(AwsTableManagementMetricsEvaluator.class);

  private Catalog catalog;

  private Map<String, String> properties;

  private int commitThreshold;

  private int timeThreshold;

  @Override
  public void initialize(Map<String, String> properties) {
    this.commitThreshold =
        PropertyUtil.propertyAsInt(
            properties,
            OPTIMIZE_DATA_COMMIT_THRESHOLD,
            OPTIMIZE_DATA_COMMIT_THRESHOLD_DEFAULT);
    this.timeThreshold =
        PropertyUtil.propertyAsInt(
            properties,
            OPTIMIZE_DATA_TIME_THRESHOLD_MS,
            OPTIMIZE_DATA_TIME_THRESHOLD_MS_DEFAULT);
    this.properties = removeReporterProperties(properties);
    this.catalog = CatalogUtil.buildIcebergCatalog(SPARK_CATALOG_NAME, properties, null);
  }

  @Override
  public void report(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
    // CommitReport restricts OptimizeTableReporter only to the write path
    if (report instanceof CommitReport) {
      CommitReport commitReport = (CommitReport) report;
      LOG.info("Received metrics report: {}", CommitReportParser.toJson(commitReport));
      TableIdentifier tableIdentifier = AwsManageUtil.buildTableIdentifier(commitReport.tableName());
      Table table = catalog.loadTable(tableIdentifier);
      // If the table should be optimized to eliminate small files
      if (shouldOptimizeDataFiles(SnapshotUtil.currentAncestors(table))) {
        String OptimizeDataFilesImpl = properties.get(OPTIMIZE_DATA_IMPL);
        Preconditions.checkArgument(
            null != OptimizeDataFilesImpl,
            "Optimize data implementation %s should be set",
            OPTIMIZE_DATA_IMPL);
        Executor executor = loadExecutor(OptimizeDataFilesImpl);
        executor.initialize(table, properties);
        executor.execute();
      }
    }
  }

  /**
   * Remove the reporter related catalog properties to avoid recursing indefinitely
   *
   * @param catalogProperties Immutable catalog properties
   * @return Catalog properties without the reporter related properties
   */
  private Map<String, String> removeReporterProperties(Map<String, String> catalogProperties) {
    Map<String, String> properties = Maps.newHashMap(catalogProperties);
    properties.remove(CatalogProperties.METRICS_REPORTER_IMPL);
    properties.remove(OPTIMIZE_DATA_COMMIT_THRESHOLD);
    properties.remove(OPTIMIZE_DATA_TIME_THRESHOLD_MS);
    return properties;
  }

  /**
   * Determine if the table should be optimized based upon the defined thresholds (commit based,
   * time based)
   *
   * @param snapshotIterable Iterable of Snapshot
   * @return true/false based upon which thresholds are met
   */
  private boolean shouldOptimizeDataFiles(Iterable<Snapshot> snapshotIterable) {
    Iterator<Snapshot> snapshotIterator = snapshotIterable.iterator();
    int numSnapshotsSinceLastOptimize = 0;
    // Traverse back from the latest snapshot
    while (snapshotIterator.hasNext()) {
      Snapshot snapshot = snapshotIterator.next();
      if (snapshot.operation().equals(DataOperations.REPLACE)) {
        LOG.info("Reached the latest optimize data snapshot without meeting any thresholds");
        return false;
      }

      if (System.currentTimeMillis() - snapshot.timestampMillis() >= timeThreshold) {
        LOG.info(
            "Time threshold value {} has been met at snapshot {} with timestamp {}",
            timeThreshold,
            snapshot.snapshotId(),
            snapshot.timestampMillis());
        return true;
      }

      numSnapshotsSinceLastOptimize++;
      if (numSnapshotsSinceLastOptimize >= commitThreshold) {
        LOG.info(
            "Commit threshold value {} has been met at snapshot {}",
            commitThreshold,
            snapshot.snapshotId());
        return true;
      }
    }

    LOG.info("Traversed the entire snapshot list without meeting any thresholds");
    return false;
  }

  /**
   * Load a custom {@link Executor} implementation.
   *
   * <p>The implementation must have a no-arg constructor.
   *
   * @param impl full class name of a custom {@link Executor} implementation
   * @return An initialized {@link Executor}.
   * @throws IllegalArgumentException if class path not found or right constructor not found or the
   *                                  loaded class cannot be cast to the given interface type
   */
  private Executor loadExecutor(String impl) {
    LOG.info("Loading custom Executor implementation: {}", impl);
    DynConstructors.Ctor<Executor> ctor;
    try {
      ctor =
          DynConstructors.builder(Executor.class)
              .loader(EMRSparkUtil.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Executor, missing no-arg constructor: %s", impl), e);
    }

    Executor executor;
    try {
      executor = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Executor, %s does not implement OptimizeDataFiles.", impl),
          e);
    }

    return executor;
  }
}
