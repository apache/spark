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

package org.apache.spark.shuffle.sort;

import java.io.IOException;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

/**
 * An external sorter that defines helper methods useful for shuffling.
 */
abstract class ShuffleSorter extends MemoryConsumer {

  protected ShuffleSorter(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    super(taskMemoryManager, pageSize, mode);
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public abstract long getPeakMemoryUsedBytes();

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public abstract void cleanupResources();

  /**
   * Write a record to the shuffle sorter.
   */
  public abstract void insertRecord(Object recordBase, long recordOffset, int length,
                                    int partitionId)
    throws IOException;

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  public abstract SpillInfo[] closeAndGetSpills() throws IOException;

  /**
   * Is a spill pending on the next insert?
   */
  public abstract boolean hasSpaceForInsertRecord();
}
