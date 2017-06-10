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
 * :: DeveloperApi ::
 *
 * This abstract class represents a write ahead log (aka journal) that is used by Spark Streaming
 * to save the received data (by receivers) and associated metadata to a reliable storage, so that
 * they can be recovered after driver failures. See the Spark documentation for more information
 * on how to plug in your own custom implementation of a write ahead log.
 */
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLog {
  /**
   * Write the record to the log and return a record handle, which contains all the information
   * necessary to read back the written record. The time is used to the index the record,
   * such that it can be cleaned later. Note that implementations of this abstract class must
   * ensure that the written data is durable and readable (using the record handle) by the
   * time this function returns.
   */
  public abstract WriteAheadLogRecordHandle write(ByteBuffer record, long time);

  /**
   * Read a written record based on the given record handle.
   */
  public abstract ByteBuffer read(WriteAheadLogRecordHandle handle);

  /**
   * Read and return an iterator of all the records that have been written but not yet cleaned up.
   */
  public abstract Iterator<ByteBuffer> readAll();

  /**
   * Clean all the records that are older than the threshold time. It can wait for
   * the completion of the deletion.
   */
  public abstract void clean(long threshTime, boolean waitForCompletion);

  /**
   * Close this log and release any resources. It must be idempotent.
   */
  public abstract void close();
}
