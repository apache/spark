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

import org.apache.spark.SparkConf;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.UnifiedMemoryManager;
import org.apache.spark.storage.BlockManager;

public class ShuffleSorterFactory {
  static final ShuffleSorterFactory INSTANCE = new ShuffleSorterFactory();

  /**
   * Create an external shuffle sorter based on the spark configuration.
   *
   * @return ExternalSorter for inserting and sorting shuffle data.
   */
  public ShuffleSorter create(TaskMemoryManager memoryManager,
                              BlockManager blockManager,
                              TaskMetrics taskMetrics,
                              int initialSize,
                              int numPartitions,
                              SparkConf conf) {
    int numSorters = conf.getInt("spark.shuffle.async.num.sorter", 1);
    if (numSorters <= 1) {
      return createShuffleExternalSorter(
        memoryManager, blockManager, taskMetrics, initialSize, numPartitions, conf);
    } else {
      // Compute a maximum memory per sorter that is used to balance work between sorters.
      long maxMemory = UnifiedMemoryManager.getMaxMemory(conf);
      int maxExecutors = conf.getInt("spark.executor.instances", 1);
      long maxMemoryPerSorter = maxMemory / maxExecutors;

      return createMultiShuffleSorter(
        memoryManager, blockManager, taskMetrics, initialSize, numPartitions, conf,
        numSorters, maxMemoryPerSorter);
    }
  }

  public ShuffleSorter createShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskMetrics taskMetrics,
      int initialSize,
      int numPartitions,
      SparkConf conf) {
    return new ShuffleExternalSorter(
      memoryManager, blockManager, taskMetrics, initialSize, numPartitions, conf,
      taskMetrics.shuffleWriteMetrics(), false);
  }

  public ShuffleSorter createShuffleExternalSorter(
    TaskMemoryManager memoryManager,
    BlockManager blockManager,
    TaskMetrics taskMetrics,
    int initialSize,
    int numPartitions,
    SparkConf conf,
    boolean disableSelfSpill) {
    return new ShuffleExternalSorter(
      memoryManager, blockManager, taskMetrics, initialSize, numPartitions, conf,
      taskMetrics.shuffleWriteMetrics(), disableSelfSpill);
  }

  public ShuffleSorter createMultiShuffleSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskMetrics taskMetrics,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      int numSorters,
      long initialMaxMemoryPerSorter) {
    return new MultiShuffleSorter(
      memoryManager, blockManager, taskMetrics, initialSize, numPartitions, conf,
      numSorters, initialMaxMemoryPerSorter, this);
  }
}
