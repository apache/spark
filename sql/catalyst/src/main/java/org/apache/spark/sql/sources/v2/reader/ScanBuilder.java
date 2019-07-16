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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.TableCapability;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousScan;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchScan;

/**
 * An interface for building batch and streaming scans. Implementations can mixin
 * SupportsPushDownXYZ interfaces to do operator pushdown, and keep the operator pushdown result in
 * the returned scan.
 */
@Evolving
public interface ScanBuilder {

  /**
   * Returns a {@link BatchScan} to scan data from a batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this scan returns {@link TableCapability#BATCH_READ} support in
   * its {@link Table#capabilities()}.
   */
  default BatchScan buildForBatch() {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support batch scan");
  }

  /**
   * Returns a {@link MicroBatchScan} to scan data from a micro-batch streaming source. By default
   * this method throws exception, data sources must overwrite this method to provide an
   * implementation, if the {@link Table} that creates this scan returns
   * {@link TableCapability#MICRO_BATCH_READ} support in its {@link Table#capabilities()}.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Data streams for the same logical source in the same query
   *                           will be given the same checkpointLocation.
   */
  default MicroBatchScan buildForMicroBatchStreaming(String checkpointLocation) {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support micro-batch streaming scan");
  }

  /**
   * Returns a {@link ContinuousScan} to scan data from a continuous streaming source. By default
   * this method throws exception, data sources must overwrite this method to provide an
   * implementation, if the {@link Table} that creates this scan returns
   * {@link TableCapability#CONTINUOUS_READ} support in its {@link Table#capabilities()}.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Data streams for the same logical source in the same query
   *                           will be given the same checkpointLocation.
   */
  default ContinuousScan buildForContinuousStreaming(String checkpointLocation) {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support continuous streaming scan");
  }
}
