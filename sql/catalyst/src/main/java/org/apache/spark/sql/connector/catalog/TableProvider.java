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

import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * The base interface for v2 data sources which don't have a real catalog. Implementations must
 * have a public, 0-arg constructor.
 * <p>
 * Note that, TableProvider can only apply data operations to existing tables, like read, append,
 * delete, and overwrite. It does not support the operations that require metadata changes, like
 * create/drop tables.
 * <p>
 * The major responsibility of this interface is to return a {@link Table} for read/write.
 * </p>
 */
@Evolving
public interface TableProvider {

  /**
   * Return a {@link Table} instance with specified table properties to do read/write.
   * Implementations should infer the table schema and partitioning.
   *
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   */
  Table getTable(Map<String, String> properties);

  /**
   * Return a {@link Table} instance with specified table schema and properties to do read/write.
   * Implementations should infer the table partitioning.
   *
   * @param schema The specified schema.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   *
   * @throws IllegalArgumentException if the specified schema does not match the actual table
   *                                  schema.
   */
  Table getTable(StructType schema, Map<String, String> properties);

  /**
   * Return a {@link Table} instance with specified table schema, partitioning and properties to do
   * read/write.
   *
   * @param schema The specified schema.
   * @param partitioning The specified partitioning.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   *
   * @throws IllegalArgumentException if the specified schema/partitioning does not match the actual
   *                                  table schema/partitioning.
   */
  Table getTable(
      StructType schema,
      Transform[] partitioning,
      Map<String, String> properties);
}
