package org.apache.iceberg.aws.manage;

import java.util.Arrays;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Utility class to manage an Iceberg table.
 */
public class AwsManageUtil {

  /**
   * Builds {@link TableIdentifier} by removing the first namespace level, and keeping the rest of the levels. For
   * example: catalog.db.table gets converted to db.table
   *
   * @param name Table name
   * @return TableIdentifier
   */
  public static TableIdentifier buildTableIdentifier(String name) {
    TableIdentifier tableIdentifier = TableIdentifier.parse(name);
    Namespace namespace = tableIdentifier.namespace();
    String[] levels = Arrays.copyOfRange(namespace.levels(), 1, namespace.levels().length);
    return TableIdentifier.of(Namespace.of(levels), tableIdentifier.name());
  }
}
