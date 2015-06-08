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

package org.apache.spark.shuffle

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 */
private[spark] trait ShuffleManager {
  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      stageAttemptId: Int,
      context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]


  /**
   * Get all the files associated with the given shuffle.
   *
   * This method exists just so that general shuffle tests can make sure shuffle files are cleaned
   * up correctly.
   */
  @VisibleForTesting
  private[shuffle] def getShuffleFiles(
      handle: ShuffleHandle,
      mapId: Int,
      reduceId: Int,
      stageAttemptId: Int): Seq[File]


  /**
    * Remove a shuffle's metadata from the ShuffleManager.
    * @return true if the metadata removed successfully, otherwise false.
    */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit

  private[this] val shuffleToAttempts = new ConcurrentHashMap[Int, ConcurrentHashMap[Int, Int]]()

  /**
   * Register a stage attempt for the given shuffle, so we can clean up all attempts when
   * the shuffle is unregistered
   */
  protected def addShuffleAttempt(shuffleId: Int, stageAttemptId: Int): Unit = {
    shuffleToAttempts.putIfAbsent(shuffleId, new ConcurrentHashMap[Int, Int]())
    shuffleToAttempts.get(shuffleId).putIfAbsent(stageAttemptId, stageAttemptId)
  }

  /**
   * Get all stage attempts a shuffle, so they can all be cleaned up.
   *
   * Calling this also cleans up internal state which tracks attempts for each shuffle, so calling
   * this again for the same shuffleId will always yield an empty Iterable.
   */
  @VisibleForTesting
  private[shuffle] def stageAttemptsForShuffle(shuffleId: Int): Iterable[Int] = {
    val attempts = shuffleToAttempts.remove(shuffleId)
    if (attempts == null) {
      Iterable[Int]()
    } else {
      attempts.values().asScala
    }
  }
}
