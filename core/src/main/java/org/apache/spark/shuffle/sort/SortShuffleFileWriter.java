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

import java.io.File;
import java.io.IOException;

import scala.Product2;
import scala.collection.Iterator;

import org.apache.spark.annotation.Private;
import org.apache.spark.TaskContext;
import org.apache.spark.storage.BlockId;

/**
 * Interface for objects that {@link SortShuffleWriter} uses to write its output files.
 */
@Private
public interface SortShuffleFileWriter<K, V> {

  void insertAll(Iterator<Product2<K, V>> records) throws IOException;

  /**
   * Write all the data added into this shuffle sorter into a file in the disk store. This is
   * called by the SortShuffleWriter and can go through an efficient path of just concatenating
   * binary files if we decided to avoid merge-sorting.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @param context a TaskContext for a running Spark task, for us to update shuffle metrics.
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  long[] writePartitionedFile(
      BlockId blockId,
      TaskContext context,
      File outputFile) throws IOException;

  void stop() throws IOException;
}
