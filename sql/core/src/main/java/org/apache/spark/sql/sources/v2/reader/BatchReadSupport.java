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

/**
 * An interface that defines how to scan the data from data source for batch processing.
 *
 * The execution engine will create an instance of this interface at the start of a batch query,
 * then call {@link #newScanConfigBuilder()} and create an instance of {@link ScanConfig}. The
 * {@link ScanConfigBuilder} can apply operator pushdown and keep the pushdown result in
 * {@link ScanConfig}. The {@link ScanConfig} will be used to create input partitions and reader
 * factory to process data from the data source.
 */
@InterfaceStability.Evolving
public interface BatchReadSupport extends ReadSupport {

  /**
   * Returns a builder of {@link ScanConfig}. The builder can take some query specific information
   * to do operators pushdown, and keep these information in the created {@link ScanConfig}.
   *
   * This is the first step of the data scan. All other methods in {@link BatchReadSupport} needs
   * to take {@link ScanConfig} as an input.
   */
  ScanConfigBuilder newScanConfigBuilder();

  /**
   * Returns a factory, which produces one {@link PartitionReader} for one {@link InputPartition}.
   */
  PartitionReaderFactory createReaderFactory(ScanConfig config);
}
