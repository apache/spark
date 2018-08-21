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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
import org.apache.spark.sql.sources.v2.reader.BatchReadSupport;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data reading ability for batch processing.
 *
 * This interface is used to create {@link BatchReadSupport} instances when end users run
 * {@code SparkSession.read.format(...).option(...).load()}.
 */
@InterfaceStability.Evolving
public interface BatchReadSupportProvider extends DataSourceV2 {

  /**
   * Creates a {@link BatchReadSupport} instance to load the data from this data source with a user
   * specified schema, which is called by Spark at the beginning of each batch query.
   *
   * Spark will call this method at the beginning of each batch query to create a
   * {@link BatchReadSupport} instance.
   *
   * By default this method throws {@link UnsupportedOperationException}, implementations should
   * override this method to handle user specified schema.
   *
   * @param schema the user specified schema.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  default BatchReadSupport createBatchReadSupport(StructType schema, DataSourceOptions options) {
    return DataSourceV2Utils.failForUserSpecifiedSchema(this);
  }

  /**
   * Creates a {@link BatchReadSupport} instance to scan the data from this data source, which is
   * called by Spark at the beginning of each batch query.
   *
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  BatchReadSupport createBatchReadSupport(DataSourceOptions options);
}
