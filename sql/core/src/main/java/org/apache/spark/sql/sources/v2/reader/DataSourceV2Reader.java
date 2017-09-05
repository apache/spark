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

package org.apache.spark.sql.sources.v2.reader;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * A data source reader that can mix in various query optimization interfaces and implement these
 * optimizations. The actual scan logic should be delegated to `ReadTask`s that are returned by
 * this data source reader.
 *
 * There are mainly 3 kinds of query optimizations:
 *   1. push operators downward to the data source, e.g., column pruning, filter push down, etc.
 *   2. propagate information upward to Spark, e.g., report statistics, report ordering, etc.
 *   3. special scans like columnar scan, unsafe row scan, etc. Note that a data source reader can
 *      implement at most one special scan.
 *
 * Spark first applies all operator push-down optimizations which this data source supports. Then
 * Spark collects information this data source provides for further optimizations. Finally Spark
 * issues the scan request and does the actual data reading.
 */
public interface DataSourceV2Reader {

  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   */
  StructType readSchema();

  /**
   * Returns a list of read tasks. Each task is responsible for outputting data for one RDD
   * partition. That means the number of tasks returned here is same as the number of RDD
   * partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source reader mixes in other optimization
   * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
   * Spark issues the scan request.
   */
  List<ReadTask<Row>> createReadTasks();
}
