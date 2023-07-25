package org.apache.iceberg.aws.manage;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SLEEP_WAIT_MS;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SLEEP_WAIT_MS_DEFAULT;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SYNCHRONOUS_ENABLED;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_SYNCHRONOUS_ENABLED_DEFAULT;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_ATHENA_OUTPUT_BUCKET;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_ATHENA_DATA_CATALOG;
import static org.apache.iceberg.aws.manage.AwsManageProperties.OPTIMIZE_DATA_ATHENA_DATA_CATALOG_DEFAULT;

/**
 * An implementation of {@link Executor} to optimize data by launching OPTIMIZE jobs in Athena
 */
public class AthenaOptimizeDataExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(AthenaOptimizeDataExecutor.class);

  private static final String ATHENA_QUERY_FORMAT = "OPTIMIZE %s REWRITE DATA USING BIN_PACK;";

  private String tableName;

  private boolean synchronous;

  private int sleepWaitDurationMs;

  private AthenaClient athenaClient;

  private String athenaOutputBucket;

  private String athenaDataCatalog;

  @Override
  public void initialize(Table table, Map<String, String> properties) {
    this.tableName = AwsManageUtil.buildTableIdentifier(table.name()).toString();
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
    this.athenaClient = AthenaClient.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
    this.athenaOutputBucket = properties.get(OPTIMIZE_DATA_ATHENA_OUTPUT_BUCKET);
    Preconditions.checkArgument(null != athenaOutputBucket, "Invalid output bucket: null");
    this.athenaDataCatalog =
        PropertyUtil.propertyAsString(
            properties,
            OPTIMIZE_DATA_ATHENA_DATA_CATALOG,
            OPTIMIZE_DATA_ATHENA_DATA_CATALOG_DEFAULT);
  }

  @Override
  public void execute() {
    String queryExecutionId = submitAthenaQuery();
    if (synchronous) {
      waitForQueryToComplete(queryExecutionId);
    }
    athenaClient.close();
  }

  /**
   * Submits a sample query to Amazon Athena and returns the execution ID of the query.
   *
   * @return Query execution id
   */
  private String submitAthenaQuery() {
    try {
      // The QueryExecutionContext allows us to set the database.
      QueryExecutionContext queryExecutionContext =
          QueryExecutionContext.builder().database(athenaDataCatalog).build();

      // The result configuration specifies where the results of the query should go.
      ResultConfiguration resultConfiguration =
          ResultConfiguration.builder().outputLocation(athenaOutputBucket).build();

      StartQueryExecutionRequest startQueryExecutionRequest =
          StartQueryExecutionRequest.builder()
              .queryString(String.format(ATHENA_QUERY_FORMAT, tableName))
              .queryExecutionContext(queryExecutionContext)
              .resultConfiguration(resultConfiguration)
              .build();

      StartQueryExecutionResponse startQueryExecutionResponse =
          athenaClient.startQueryExecution(startQueryExecutionRequest);
      LOG.info(
          "Optimize job submitted for table {}.{} on Athena (query execution id: {})",
          athenaDataCatalog,
          tableName,
          startQueryExecutionResponse.queryExecutionId());
      return startQueryExecutionResponse.queryExecutionId();
    } catch (AthenaException e) {
      LOG.error("Exception occurred while submitting query: ", e);
    }
    return null;
  }

  /**
   * Wait for an Amazon Athena query to complete, fail or to be cancelled.
   *
   * @param queryExecutionId Query execution id
   */
  private void waitForQueryToComplete(String queryExecutionId) {
    Preconditions.checkArgument(null != queryExecutionId, "Invalid query execution id: null");
    GetQueryExecutionRequest getQueryExecutionRequest =
        GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build();

    GetQueryExecutionResponse getQueryExecutionResponse;
    boolean isQueryStillRunning = true;
    while (isQueryStillRunning) {
      getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
      QueryExecutionState queryState = getQueryExecutionResponse.queryExecution().status().state();
      switch (queryState) {
        case FAILED: {
          throw new RuntimeException(
              "The Amazon Athena query failed to run with error message: "
                  + getQueryExecutionResponse.queryExecution().status().stateChangeReason());
        }
        case CANCELLED: {
          throw new RuntimeException("The Amazon Athena query was cancelled.");
        }
        case SUCCEEDED: {
          isQueryStillRunning = false;
        }
        default: {
          // Sleep an amount of time before retrying again
          try {
            Thread.sleep(sleepWaitDurationMs);
          } catch (InterruptedException e) {
            LOG.error("Exception occurred while waiting for query to complete: ", e);
          }
        }
      }
      LOG.info("Status of optimize data job: {}", queryState);
    }
  }
}