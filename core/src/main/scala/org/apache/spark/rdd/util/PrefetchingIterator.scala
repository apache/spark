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

package org.apache.spark.rdd.util

import java.util.concurrent.locks.ReentrantLock

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

/**
 * Iterator that optionally prefetches next partition asynchronously
 */
class PrefetchingIterator[T : ClassTag](rdd: RDD[T],
                                        prefetch: Boolean)
  extends Iterator[Array[T]]
    with Serializable {

  private val partitionIterator = rdd.partitions.indices.iterator

  private val lock = new ReentrantLock()
  private val ready = lock.newCondition()

  private var nextResult: Array[T] = _
  private var fetchInProgress = false

  /**
   * In addition, it prefetches next element, if it exists
   */
  override def hasNext: Boolean = withLock(() => {
    if (fetchInProgress) true
    else if (nextResult == null) {
      if (partitionIterator.hasNext) {
        initPrefetch(partitionIterator.next())
        true
      } else false
    } else true
  })

  override def next(): Array[T] = withLock(() => {
    while (fetchInProgress) ready.await()

    val partitionArray = nextResult

    nextResult = null
    fetchInProgress = false

    if (prefetch && partitionIterator.hasNext) initPrefetch(partitionIterator.next())

    partitionArray
  })

  private def initPrefetch(p: Int): Unit = {
    fetchInProgress = true

    rdd.sparkContext.submitJob(
      rdd,
      (iter: Iterator[T]) => iter.toArray,
      Seq(p),
      (_, value: Array[T]) => rememberResultAndSignal(value),
      nextResult)
  }

  private def rememberResultAndSignal(value: Array[T]): Unit = withLock(() => {
    nextResult = value
    fetchInProgress = false

    ready.signal()
  })

  private def withLock[A](fn: () => A): A = {
    lock.lock()
    try fn()
    finally lock.unlock()
  }
}
