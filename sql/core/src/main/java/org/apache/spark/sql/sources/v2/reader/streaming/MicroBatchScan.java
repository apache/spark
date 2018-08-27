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

package org.apache.spark.sql.sources.v2.reader.streaming;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.SupportsMicroBatchRead;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.reader.*;

/**
 * A {@link Scan} for streaming queries with micro-batch mode.
 *
 * The execution engine will get an instance of {@link Table} first, then call
 * {@link Table#newScanConfigBuilder(DataSourceOptions)} and create an instance of
 * {@link ScanConfig}. The {@link ScanConfigBuilder} can apply operator pushdown and keep the
 * pushdown result in {@link ScanConfig}. Then
 * {@link SupportsMicroBatchRead#createMicroBatchInputStream(String, ScanConfig, DataSourceOptions)}
 * will be called to create a {@link MicroBatchInputStream} instance. The
 * {@link MicroBatchInputStream} manages offsets and creates a {@link MicroBatchScan} instance for
 * each micro-batch. The {@link MicroBatchScan} will be used to create input partitions and
 * reader factory to scan a micro-batch with a Spark job. At the end {@link InputStream#stop()}
 * will be called when the streaming execution is completed. Note that a single query may have
 * multiple executions due to restart or failure recovery.
 */
@InterfaceStability.Evolving
public interface MicroBatchScan extends Scan {

  /**
   * Returns a factory, which produces one {@link PartitionReader} for one {@link InputPartition}.
   */
  PartitionReaderFactory createReaderFactory();
}
