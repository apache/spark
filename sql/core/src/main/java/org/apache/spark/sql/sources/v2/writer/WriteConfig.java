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

package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamingWriteSupport;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

/**
 * WriteConfig carries configuration specific to a write operation for a table.
 * <p>
 * WriteConfig is created when Spark initializes a v2 write operation using factory methods provided
 * by the write support instance for the query type:
 * <ul>
 * <li>
 * {@link BatchWriteSupport#createWriteConfig(StructType, DataSourceOptions)}
 * </li>
 * <li>
 * {@link BatchOverwriteSupport#createOverwriteConfig(StructType, DataSourceOptions, Filter[])}
 * </li>
 * <li>
 * {@link BatchPartitionOverwriteSupport#createDynamicOverwriteConfig(StructType,DataSourceOptions)}
 * </li>
 * <li>
 * {@link StreamingWriteSupport#createWriteConfig(StructType, OutputMode, DataSourceOptions)}
 * </li>
 * </ul>
 * <p>
 * This class is passed to create a {@link DataWriterFactory} and is passed to commit and abort.
 */
@InterfaceStability.Evolving
public interface WriteConfig {
  /**
   * The schema of rows that will be written.
   */
  StructType writeSchema();

  /**
   * Options to configure the write.
   */
  DataSourceOptions writeOptions();
}
