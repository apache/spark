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

package org.apache.spark.sql.connector.write;

import java.util.Map;
import java.util.Optional;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * This interface contains logical write information that data sources can use when generating a
 * {@link WriteBuilder}.
 *
 * @since 3.0.0
 */
@Evolving
public interface LogicalWriteInfo {
  /**
   * the options that the user specified when writing the dataset
   */
  CaseInsensitiveStringMap options();

  /**
   * {@code queryId} is a unique string of the query. It's possible that there are many queries
   * running at the same time, or a query is restarted and resumed. {@link BatchWrite} can use
   * this id to identify the query.
   */
  String queryId();

  /**
   * the schema of the input data from Spark to data source.
   * <p>
   * When {@link #columnUpdateSchema()} is present, this schema covers only newly inserted rows, as
   * updated, copied, and reinserted rows follow {@link #columnUpdateSchema()}. It is then empty
   * when the command inserts no new rows, such as UPDATE.
   */
  StructType schema();

  /**
   * the schema of the ID columns from Spark to data source.
   */
  default Optional<StructType> rowIdSchema() {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_ROW_ID_SCHEMA_NOT_IMPLEMENTED", Map.of("class", getClass().getName()));
  }

  /**
   * the schema of the input metadata from Spark to data source.
   */
  default Optional<StructType> metadataSchema() {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_METADATA_SCHEMA_NOT_IMPLEMENTED", Map.of("class", getClass().getName()));
  }

  /**
   * the schema of updated, copied, and reinserted rows from Spark to data source in a
   * column-level update. Present when the operation mixes in {@link SupportsColumnUpdates} and
   * Spark delivers these rows with the columns of
   * {@link SupportsColumnUpdates#requiredDataAttributes()}, which currently happens only for
   * UPDATE. It covers every data column if every data column is declared. When present,
   * {@link #schema()} covers only newly inserted rows. Empty when Spark delivers every row with
   * {@link #schema()}.
   * <p>
   * The default implementation throws {@code DATA_SOURCE_COLUMN_UPDATE_SCHEMA_NOT_IMPLEMENTED}.
   * Spark's own implementation overrides it, so an implementation that wraps or rebuilds the
   * info must forward it.
   *
   * @since 4.4.0
   */
  default Optional<StructType> columnUpdateSchema() {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_COLUMN_UPDATE_SCHEMA_NOT_IMPLEMENTED", Map.of("class", getClass().getName()));
  }
}
