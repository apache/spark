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

package org.apache.spark.sql.datasource;

import java.util.Map;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * A data source implemented with the columnar data source API.
 * <p>
 * The columnar data source API is a simpler way to plug a data source into Spark than
 * implementing the Data Source V2 interfaces directly. It follows the Python Data Source API
 * ({@code pyspark.sql.datasource}), but data is exchanged only as columnar batches: a source
 * returns {@link org.apache.spark.sql.vectorized.ColumnarBatch}es when reading and receives them
 * when writing. Spark adapts the source to Data Source V2, so it supports batch and streaming
 * reads and writes, and filter, column and limit pushdown.
 * <p>
 * A data source is implemented either on the JVM, registered through a
 * {@link DataSourceProvider}, or in native code such as Rust or C++, through the
 * {@link NativeBridge} binary interface.
 * <p>
 * Spark creates a new instance on the driver for each schema inference, scan and write. A data
 * source only needs to implement the operations it supports; the default implementations throw an
 * error saying that the operation is not supported.
 *
 * @since 4.4.0
 */
@Evolving
public interface DataSource {

  /**
   * Returns the schema of the data. Spark calls it only when the user does not specify a schema.
   */
  default StructType schema() {
    throw new SparkUnsupportedOperationException(
      "UNABLE_TO_INFER_SCHEMA", Map.of("format", getClass().getName()));
  }

  /**
   * Returns a reader for a batch scan.
   *
   * @param schema the schema to read: the one the user specified, or the one returned by
   *               {@link #schema()}
   */
  default DataSourceReader reader(StructType schema) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED", Map.of("description", getClass().getName()));
  }

  /**
   * Returns a reader for a micro-batch streaming scan.
   *
   * @param schema the schema to read: the one the user specified, or the one returned by
   *               {@link #schema()}
   */
  default DataSourceStreamReader streamReader(StructType schema) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED", Map.of("description", getClass().getName()));
  }

  /**
   * Returns a writer for a batch write.
   *
   * @param schema the schema of the data to write
   * @param overwrite whether to replace the existing data (save mode "overwrite") instead of
   *                  appending to it
   */
  default DataSourceWriter writer(StructType schema, boolean overwrite) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED", Map.of("description", getClass().getName()));
  }

  /**
   * Returns a writer for a streaming write.
   *
   * @param schema the schema of the data to write
   * @param overwrite whether to replace the existing data in each micro-batch (output mode
   *                  "complete") instead of appending to it
   */
  default DataSourceStreamWriter streamWriter(StructType schema, boolean overwrite) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED", Map.of("description", getClass().getName()));
  }
}
