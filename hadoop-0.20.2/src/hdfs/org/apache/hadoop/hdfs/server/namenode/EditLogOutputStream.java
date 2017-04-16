/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.Writable;

/**
 * A generic abstract class to support journaling of edits logs into 
 * a persistent storage.
 */
abstract class EditLogOutputStream extends OutputStream {
  // these are statistics counters
  private long numSync;        // number of sync(s) to disk
  private long totalTimeSync;  // total time to sync

  EditLogOutputStream() throws IOException {
    numSync = totalTimeSync = 0;
  }

  /**
   * Get this stream name.
   * 
   * @return name of the stream
   */
  abstract String getName();

  /** {@inheritDoc} */
  abstract public void write(int b) throws IOException;

  /**
   * Write edits log record into the stream.
   * The record is represented by operation name and
   * an array of Writable arguments.
   * 
   * @param op operation
   * @param writables array of Writable arguments
   * @throws IOException
   */
  abstract void write(byte op, Writable ... writables) throws IOException;

  /**
   * Create and initialize new edits log storage.
   * 
   * @throws IOException
   */
  abstract void create() throws IOException;

  /** {@inheritDoc} */
  abstract public void close() throws IOException;

  /**
   * All data that has been written to the stream so far will be flushed.
   * New data can be still written to the stream while flushing is performed.
   */
  abstract void setReadyToFlush() throws IOException;

  /**
   * Flush and sync all data that is ready to be flush 
   * {@link #setReadyToFlush()} into underlying persistent store.
   * @throws IOException
   */
  abstract protected void flushAndSync() throws IOException;

  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush() throws IOException {
    numSync++;
    long start = FSNamesystem.now();
    flushAndSync();
    long end = FSNamesystem.now();
    totalTimeSync += (end - start);
  }

  /**
   * Return the size of the current edits log.
   * Length is used to check when it is large enough to start a checkpoint.
   */
  abstract long length() throws IOException;

  /**
   * Return total time spent in {@link #flushAndSync()}
   */
  long getTotalSyncTime() {
    return totalTimeSync;
  }

  /**
   * Return number of calls to {@link #flushAndSync()}
   */
  long getNumSync() {
    return numSync;
  }
}
