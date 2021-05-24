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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;

/**
 * The base interface representing a readable data stream in a Spark streaming query. It's
 * responsible to manage the offsets of the streaming source in the streaming query.
 * <p>
 * Data sources should implement concrete data stream interfaces:
 * {@link MicroBatchStream} and {@link ContinuousStream}.
 *
 * @since 3.0.0
 */
@Evolving
public interface SparkDataStream {

  /**
   * Returns the initial offset for a streaming query to start reading from. Note that the
   * streaming data source should not assume that it will start reading from its initial offset:
   * if Spark is restarting an existing query, it will restart from the check-pointed offset rather
   * than the initial one.
   */
  Offset initialOffset();

  /**
   * Deserialize a JSON string into an Offset of the implementation-defined offset type.
   *
   * @throws IllegalArgumentException if the JSON does not encode a valid offset for this reader
   */
  Offset deserializeOffset(String json);

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  void commit(Offset end);

  /**
   * Stop this source and free any resources it has allocated.
   */
  void stop();
}
