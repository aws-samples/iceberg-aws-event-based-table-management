package org.apache.iceberg.aws.manage;

import java.util.Map;
import org.apache.iceberg.Table;

/**
 * Interface to execute a table maintenance activity in a specific engine
 */
public interface Executor {

  /**
   * Initialize the {@link Executor} implementation
   *
   * @param table      {@link Table} instance
   * @param properties Catalog properties
   */
  void initialize(Table table, Map<String, String> properties);

  /**
   * Execute the table maintenance activity for the {@link Executor} implementation
   */
  void execute();
}