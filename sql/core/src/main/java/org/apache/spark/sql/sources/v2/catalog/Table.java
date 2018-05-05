package org.apache.spark.sql.sources.v2.catalog;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * Represents table metadata from a {@link DataSourceCatalog}.
 */
public interface Table {
  /**
   * Return the table name.
   * @return this table's name
   */
  String name();

  /**
   * Return the database that contains the table.
   * @return this table's database
   */
  String database();

  /**
   * Return the table properties.
   * @return this table's map of string properties
   */
  Map<String, String> properties();

  /**
   * Return the table schema.
   * @return this table's schema as a struct type
   */
  StructType schema();

  /**
   * Return the table partitioning expressions.
   * @return this table's partitioning expressions
   */
  List<Expression> partitions();
}
