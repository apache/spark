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

package org.apache.spark.shuffle.api;

import java.io.IOException;

import org.apache.spark.annotation.Private;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;

/**
 * :: Private ::
 * A top-level writer that returns child writers for persisting the output of a map task,
 * and then commits all of the writes as one atomic operation.
 *
 * @since 3.0.0
 */
@Private
public interface ShuffleMapOutputWriter {

  /**
   * Creates a writer that can open an output stream to persist bytes targeted for a given reduce
   * partition id.
   * <p>
   * The chunk corresponds to bytes in the given reduce partition. This will not be called twice
   * for the same partition within any given map task. The partition identifier will be in the
   * range of precisely 0 (inclusive) to numPartitions (exclusive), where numPartitions was
   * provided upon the creation of this map output writer via
   * {@link ShuffleWriteSupport#createMapOutputWriter(
   * int, int, long, int, ShuffleWriteMetricsReporter)}.
   */
  ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException;

  /**
   * Commits the writes done by all partition writers returned by all calls to this object's
   * {@link #getPartitionWriter(int)}.
   * <p>
   * This should ensure that the writes conducted by this module's partition writers are
   * available to downstream reduce tasks. If this method throws any exception, this module's
   * {@link #abort(Throwable)} method will be invoked before propagating the exception.
   * <p>
   * This can also close any resources and clean up temporary state if necessary.
   */
  void commitAllPartitions() throws IOException;

  /**
   * Abort all of the writes done by any writers returned by {@link #getPartitionWriter(int)}.
   * <p>
   * This should invalidate the results of writing bytes. This can also close any resources and
   * clean up temporary state if necessary.
   */
  void abort(Throwable error) throws IOException;
}
