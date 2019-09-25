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
 * A mix-in interface for {@link TableProvider}. Data sources can implement this interface to
 * return a table instance with specified schema and partitioning, so that they may avoid expensive
 * schema/partitioning inference.
 */
@Evolving
public interface SupportsSpecifiedSchemaPartitioning extends TableProvider {

  /**
   * Return a {@link Table} instance with the given table schema, partitioning and properties to do
   * read/write . The returned table must report the same schema and partitioning with the given
   * ones.
   *
   * @param schema The schema of the table to load.
   * @param partitions The data partitioning of the table to load.
   * @param properties The properties of the table to load. It should be sufficient to define and
   *                   access a table. The properties map may be {@link CaseInsensitiveStringMap}.
   */
  Table getTable(
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties);
}
