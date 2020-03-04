package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * An interface for exposing data columns for a table that are not in the table schema. For example,
 * a file source could expose a "file" column that contains the path of the file that contained each
 * row.
 * <p>
 * The columns returned by {@link #metadataColumns()} may be passed as {@link StructField} in
 * requested projections. Sources that implement this interface and column projection using
 * {@link SupportsPushDownRequiredColumns} must accept metadata fields passed to
 * {@link SupportsPushDownRequiredColumns#pruneColumns(StructType)}.
 */
@Evolving
public interface SupportsMetadataColumns extends Table {
  /**
   * Metadata columns that are supported by this {@link Table}.
   * <p>
   * The columns returned by this method may be passed as {@link StructField} in requested
   * projections using {@link SupportsPushDownRequiredColumns#pruneColumns(StructType)}.
   *
   * @return an array of {@link MetadataColumn}
   */
  MetadataColumn[] metadataColumns();
}
