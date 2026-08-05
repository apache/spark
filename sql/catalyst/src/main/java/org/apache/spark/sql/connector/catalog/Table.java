/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * An interface representing a logical structured data set of a data source. For example, the
 * implementation can be a directory on the file system, a topic of Kafka, or a table in the
 * catalog, etc.
 * <p>
 * This interface can mixin {@code SupportsRead} and {@code SupportsWrite} to provide data reading
 * and writing ability.
 * <p>
 * The default implementation of {@link #partitioning()} returns an empty array of partitions, and
 * the default implementation of {@link #properties()} returns an empty map. These should be
 * overridden by implementations that support partitioning and table properties.
 * <p>
 * A {@code Table} is one kind of {@link Relation}; the other is {@link View}.
 *
 * @since 3.0.0
 */
@Evolving
public interface Table extends Relation {

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  String name();

  /**
   * An ID of the table that can be used to reliably check if two table objects refer to the same
   * metastore entity. If a table is dropped and recreated again with the same name, the new table
   * ID must be different. This method must return null if connectors don't support the notion of
   * table ID.
   */
  default String id() {
    return null;
  }

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * <p>
   * @deprecated This is deprecated. Please override {@link #columns} instead.
   */
  @Deprecated(since = "3.4.0")
  default StructType schema() {
    throw QueryCompilationErrors.mustOverrideOneMethodError("columns");
  }

  /**
   * Returns the columns of this table. If the table is not readable and doesn't have a schema, an
   * empty array can be returned here.
   */
  default Column[] columns() {
    return CatalogV2Util.structTypeToV2Columns(schema(), true /* keep IDs */);
  }

  /**
   * Returns the physical partitioning of this table.
   */
  default Transform[] partitioning() {
    return new Transform[0];
  }

  /**
   * Returns the string map of table properties.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * Returns the set of capabilities for this table.
   */
  Set<TableCapability> capabilities();

  /**
   * Returns the constraints for this table.
   */
  default Constraint[] constraints() { return new Constraint[0]; }

  /**
   * Returns the write distribution this table declares as the default for writes into it, or null
   * if it declares none.
   * <p>
   * A non-null value is one of {@link TableInfo#DISTRIBUTION_MODE_HASH},
   * {@link TableInfo#DISTRIBUTION_MODE_RANGE} or {@link TableInfo#DISTRIBUTION_MODE_NONE}.
   * <p>
   * This is a <em>declared default</em> and nothing more. It says what a write into this table uses
   * when the write itself does not ask for something else; an individual write may override it, and
   * a table may narrow it (a hash distribution is meaningless on an unpartitioned table, say). What
   * a given write actually requires is reported by
   * {@link org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering} on its
   * {@code Write}, which stays authoritative. In particular this makes no claim about how the data
   * already in the table is laid out -- for that, a scan reports its own {@code outputPartitioning}
   * and {@code outputOrdering}.
   *
   * @since 4.4.0
   */
  default String writeDistributionMode() { return null; }

  /**
   * Returns the write ordering this table declares as the default for writes into it, empty if it
   * declares none. A declared default in exactly the sense described on
   * {@link #writeDistributionMode()}.
   *
   * @since 4.4.0
   */
  default SortOrder[] writeOrdering() { return new SortOrder[0]; }

  /**
   * Returns the version of this table if versioning is supported, null otherwise.
   * <p>
   * This method must not trigger a refresh of the table metadata. It should return
   * the version that corresponds to the current state of this table instance.
   */
  default String version() { return null; }
}
