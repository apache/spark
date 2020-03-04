package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;

/**
 * Interface for a metadata column.
 * <p>
 * A metadata column can expose additional metadata about a row. For example, rows from Kafka can
 * use metadata columns to expose a message's topic, partition number, and offset.
 * <p>
 * A metadata column could also be the result of a transform applied to a value in the row. For
 * example, a partition value produced by bucket(id, 16) could be exposed by a metadata column. In
 * this case, {@link #transform()} should return a non-null {@link Transform} that produced the
 * metadata column's values.
 */
@Evolving
public interface MetadataColumn {
  /**
   * The name of this metadata column.
   *
   * @return a String name
   */
  String name();

  /**
   * The data type of values in this metadata column.
   *
   * @return a {@link DataType}
   */
  DataType dataType();

  /**
   * @return whether values produced by this metadata column may be null
   */
  default boolean isNullable() {
    return true;
  }

  /**
   * Documentation for this metadata column, or null.
   *
   * @return a documentation String
   */
  String comment();

  /**
   * The {@link Transform} used to produce this metadata column from data rows, or null.
   *
   * @return a {@link Transform} used to produce the column's values, or null if there isn't one
   */
  Transform transform();
}
