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
import org.apache.spark.sql.connector.expressions.Transform;
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
 *
 * @since 3.0.0
 */
@Evolving
public interface Table {

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  String name();

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * <p>
   * @deprecated This is deprecated. Please override {@link #columns} instead.
   */
  @Deprecated(since = "3.4.0")
  StructType schema();

  /**
   * Returns the columns of this table. If the table is not readable and doesn't have a schema, an
   * empty array can be returned here.
   */
  default Column[] columns() {
    return CatalogV2Util.structTypeToV2Columns(schema());
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
   * Returns the current table version if implementation supports versioning.
   * If the table is not versioned, null can be returned here.
   */
  default String currentVersion() { return null; }
}
