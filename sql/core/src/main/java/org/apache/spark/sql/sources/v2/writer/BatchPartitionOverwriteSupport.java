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

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

/**
 * An interface that adds support to {@link BatchWriteSupport} for a replace data operation that
 * replaces partitions dynamically with the output of a write operation.
 * <p>
 * Data source implementations can implement this interface in addition to {@link BatchWriteSupport}
 * to support write operations that replace all partitions in the output table that are present
 * in the write's output data.
 * <p>
 * This is used to implement INSERT OVERWRITE ... PARTITIONS.
 */
public interface BatchPartitionOverwriteSupport {
  /**
   * Creates a {@link WriteConfig} for a batch overwrite operation, where the data partitions
   * written by the job replace any partitions that already exist in the output table.
   *
   * @param schema schema of the data that will be written
   * @param options options to configure the write operation
   * @return a new WriteConfig for the dynamic partition overwrite operation
   */
  // TODO: replace DataSourceOptions with CaseInsensitiveStringMap
  WriteConfig createDynamicOverwriteConfig(StructType schema, DataSourceOptions options);
}
