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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.types.StructType;

/**
 * A data source reader that is returned by
 * {@link ReadSupport#createReader(DataSourceOptions)} or
 * {@link ReadSupport#createReader(StructType, DataSourceOptions)}.
 * It can mix in various query optimization interfaces to speed up the data scan. The actual scan
 * logic is delegated to {@link InputPartition}s, which are returned by
 * {@link #planInputPartitions()}.
 *
 * There are mainly 3 kinds of query optimizations:
 *   1. Operators push-down. E.g., filter push-down, required columns push-down(aka column
 *      pruning), etc. Names of these interfaces start with `SupportsPushDown`.
 *   2. Information Reporting. E.g., statistics reporting, ordering reporting, etc.
 *      Names of these interfaces start with `SupportsReporting`.
 *   3. Columnar scan if implements {@link SupportsScanColumnarBatch}.
 *
 * If an exception was throw when applying any of these query optimizations, the action will fail
 * and no Spark job will be submitted.
 *
 * Spark first applies all operator push-down optimizations that this data source supports. Then
 * Spark collects information this data source reported for further optimizations. Finally Spark
 * issues the scan request and does the actual data reading.
 */
@InterfaceStability.Evolving
public interface DataSourceReader {

  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  StructType readSchema();

  /**
   * Returns a list of {@link InputPartition}s. Each {@link InputPartition} is responsible for
   * creating a data reader to output data of one RDD partition. The number of input partitions
   * returned here is the same as the number of RDD partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source reader mixes in other optimization
   * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
   * Spark issues the scan request.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  List<InputPartition<InternalRow>> planInputPartitions();
}
