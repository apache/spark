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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.SupportsBatchRead;
import org.apache.spark.sql.sources.v2.Table;

/**
 * A {@link Scan} for batch queries.
 *
 * The execution engine will get an instance of {@link Table} first, then call
 * {@link Table#newScanConfigBuilder(DataSourceOptions)} and create an instance of
 * {@link ScanConfig}. The {@link ScanConfigBuilder} can apply operator pushdown and keep the
 * pushdown result in {@link ScanConfig}. Then
 * {@link SupportsBatchRead#createBatchScan(ScanConfig, DataSourceOptions)} will be called to create
 * a {@link BatchScan} instance, which will be used to create input partitions and reader factory to
 * scan data from the data source with a Spark job.
 */
@InterfaceStability.Evolving
public interface BatchScan extends Scan {

  /**
   * Returns a factory, which produces one {@link PartitionReader} for one {@link InputPartition}.
   */
  PartitionReaderFactory createReaderFactory();
}
