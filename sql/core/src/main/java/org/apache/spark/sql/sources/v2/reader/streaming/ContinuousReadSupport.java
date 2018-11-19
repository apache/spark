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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.execution.streaming.BaseStreamingSource;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.ScanConfig;
import org.apache.spark.sql.sources.v2.reader.ScanConfigBuilder;

/**
 * An interface that defines how to load the data from data source for continuous streaming
 * processing.
 *
 * The execution engine will get an instance of this interface from a data source provider
 * (e.g. {@link org.apache.spark.sql.sources.v2.ContinuousReadSupportProvider}) at the start of a
 * streaming query, then call {@link #newScanConfigBuilder(Offset)} and create an instance of
 * {@link ScanConfig} for the duration of the streaming query or until
 * {@link #needsReconfiguration(ScanConfig)} is true. The {@link ScanConfig} will be used to create
 * input partitions and reader factory to scan data with a Spark job for its duration. At the end
 * {@link #stop()} will be called when the streaming execution is completed. Note that a single
 * query may have multiple executions due to restart or failure recovery.
 */
@Evolving
public interface ContinuousReadSupport extends StreamingReadSupport, BaseStreamingSource {

  /**
   * Returns a builder of {@link ScanConfig}. Spark will call this method and create a
   * {@link ScanConfig} for each data scanning job.
   *
   * The builder can take some query specific information to do operators pushdown, store streaming
   * offsets, etc., and keep these information in the created {@link ScanConfig}.
   *
   * This is the first step of the data scan. All other methods in {@link ContinuousReadSupport}
   * needs to take {@link ScanConfig} as an input.
   */
  ScanConfigBuilder newScanConfigBuilder(Offset start);

  /**
   * Returns a factory, which produces one {@link ContinuousPartitionReader} for one
   * {@link InputPartition}.
   */
  ContinuousPartitionReaderFactory createContinuousReaderFactory(ScanConfig config);

  /**
   * Merge partitioned offsets coming from {@link ContinuousPartitionReader} instances
   * for each partition to a single global offset.
   */
  Offset mergeOffsets(PartitionOffset[] offsets);

  /**
   * The execution engine will call this method in every epoch to determine if new input
   * partitions need to be generated, which may be required if for example the underlying
   * source system has had partitions added or removed.
   *
   * If true, the query will be shut down and restarted with a new {@link ContinuousReadSupport}
   * instance.
   */
  default boolean needsReconfiguration(ScanConfig config) {
    return false;
  }
}
