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

/**
 * A {@link InputStream} for a streaming query with continuous mode.
 */
@InterfaceStability.Evolving
public interface ContinuousInputStream extends InputStream {

  /**
   * Creates a {@link ContinuousScan} instance with a start offset, to scan the data from the start
   * offset with a end-less Spark job. The job will be terminated if {@link #needsReconfiguration()}
   * returns false, and the execution engine will call this method again, with a different start
   * offset, and launch a new end-less Spark job.
   */
  ContinuousScan createContinuousScan(Offset start);

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
   * If true, the query will be shut down and restarted with a new {@link ContinuousScan}
   * instance.
   */
  default boolean needsReconfiguration() {
    return false;
  }
}
