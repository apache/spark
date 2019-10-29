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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

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
   * Infer the schema of the table that is identified by the given options.
   *
   * @param options The options that can identify a table, e.g. file path, Kafka topic name, etc.
   *                It's an immutable case-insensitive string-to-string map.
   *
   */
  StructType inferSchema(CaseInsensitiveStringMap options);

  /**
   * Infer the partitioning of the table that is identified by the given options.
   *
   * @param schema The schema of the table.
   * @param options The options that can identify a table, e.g. file path, Kafka topic name, etc.
   *                It's an immutable case-insensitive string-to-string map.
   */
  Transform[] inferPartitioning(StructType schema, CaseInsensitiveStringMap options);

  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties
   * to do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   */
  Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties);
}
