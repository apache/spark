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

package org.apache.spark.shuffle.sort.io;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;

public class LocalDiskShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private final Supplier<IndexShuffleBlockResolver> blockResolver;

  public LocalDiskShuffleExecutorComponents(SparkConf sparkConf) {
    this(
        sparkConf,
        Suppliers.memoize(() -> {
          if (SparkEnv.get() == null) {
            throw new IllegalStateException("SparkEnv must be initialized before using the" +
                " local disk executor components/");
          }
          return new IndexShuffleBlockResolver(sparkConf, SparkEnv.get().blockManager());
        }));
  }

  @VisibleForTesting
  public LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      IndexShuffleBlockResolver blockResolver) {
    this(sparkConf, () -> blockResolver);
  }

  private LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      Supplier<IndexShuffleBlockResolver> blockResolver) {
    this.sparkConf = sparkConf;
    this.blockResolver = blockResolver;
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    return new LocalDiskShuffleMapOutputWriter(
        shuffleId, mapTaskId, numPartitions, blockResolver.get(), sparkConf);
  }

  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    return Optional.of(
        new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver.get()));
  }
}
