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
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * An interface that can be extended by DataSources that implement the {@link TableProvider}
 * that can create new tables for the given options. These tables are not stored in any catalog,
 * but have a mechanism to check whether a table can be created for the specified data source
 * options.
 */
@Evolving
public interface SupportsCreateTable extends TableProvider {

  /**
   * Check whether a new table can be created for the given options.
   *
   * @param options The options that should be sufficient to define and access a table
   * @return true if the table exists, false otherwise
   */
  boolean canCreateTable(CaseInsensitiveStringMap options);

  /**
   * Create a table with the given options. It is the data source's responsibility to check if
   * the provided schema and the transformations are acceptable in case a table already exists
   * for the given options.
   *
   * @param options The data source options that define how to access the table. This can contain
   *                the path for file based tables, kafka broker addresses to connect to Kafka or
   *                the JDBC URL to connect to a JDBC data source.
   * @param schema The schema of the new table, as a struct type
   * @param partitions Transforms to use for partitioning data in the table
   * @param properties A string map of table properties
   * @return Metadata for the new table. The table creation can be followed up by a write
   * @throws IllegalArgumentException If a table already exists for these options with a
   *                                  non-conforming schema or different partitioning specification.
   * @throws UnsupportedOperationException If a requested partition transform is not supported or
   *                                       table properties are not supported
   */
  Table buildTable(
    CaseInsensitiveStringMap options,
    StructType schema,
    Transform[] partitions,
    Map<String, String> properties);
}
