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

package org.apache.spark.streaming.util;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Interface representing a write ahead log (aka journal) that is used by Spark Streaming to
 * save the received data (by receivers) and associated metadata to a reliable storage, so that
 * they can be recovered after driver failures. See the Spark docs for more information on how
 * to plug in your own custom implementation of a write ahead log.
 */
@org.apache.spark.annotation.DeveloperApi
public interface WriteAheadLog {
  /**
   * Write the record to the log and return the segment information that is necessary to read
   * back the written record. The time is used to the index the record, such that it can be
   * cleaned later. Note that the written data must be durable and readable (using the
   * segment info) by the time this function returns.
   */
  WriteAheadLogSegment write(ByteBuffer record, long time);

  /**
   * Read a written record based on the given segment information.
   */
  ByteBuffer read(WriteAheadLogSegment segment);

  /**
   * Read and return an iterator of all the records that have written and not yet cleanup.
   */
  Iterator<ByteBuffer> readAll();

  /**
   * Cleanup all the records that are older than the given threshold time. It can wait for
   * the completion of the deletion.
   */
  void cleanup(long threshTime, boolean waitForCompletion);

  /**
   * Close this log and release any resources.
   */
  void close();
}
