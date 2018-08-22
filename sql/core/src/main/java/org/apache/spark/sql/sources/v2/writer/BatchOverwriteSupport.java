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

import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

/**
 * An interface that adds support to {@link BatchWriteSupport} for a replace data operation that
 * replaces a subset of the output table with the output of a write operation. The subset removed is
 * determined by a set of filter expressions.
 * <p>
 * Data source implementations can implement this interface in addition to {@link BatchWriteSupport}
 * to support idempotent write operations that replace data matched by a set of delete filters with
 * the result of the write operation.
 * <p>
 * This is used to build idempotent writes. For example, a query that produces a daily summary
 * may be run several times as new data arrives. Each run should replace the output of the last
 * run for a particular day in the partitioned output table. Such a job would write using this
 * WriteSupport and would pass a filter matching the previous job's output, like
 * <code>$"day" === '2018-08-22'</code>, to remove that data and commit the replacement data at
 * the same time.
 */
public interface BatchOverwriteSupport extends BatchWriteSupport {
  /**
   * Creates a {@link WriteConfig} for a batch overwrite operation, where the data written replaces
   * the data matching the delete filters.
   * <p>
   * Implementations may reject the delete filters if the delete isn't possible without significant
   * effort. For example, partitioned data sources may reject deletes that do not filter by
   * partition columns because the filter may require rewriting files without deleted records.
   * To reject a delete implementations should throw {@link IllegalArgumentException} with a clear
   * error message that identifies which expression was rejected.
   *
   * @param schema schema of the data that will be written
   * @param options options to configure the write operation
   * @param deleteFilters filters that match data to be replaced by the data written
   * @return a new WriteConfig for the replace data (overwrite) operation
   * @throws IllegalArgumentException If the delete is rejected due to required effort
   */
  // TODO: replace DataSourceOptions with CaseInsensitiveStringMap
  WriteConfig createOverwriteConfig(StructType schema,
                                    DataSourceOptions options,
                                    Filter[] deleteFilters);
}
