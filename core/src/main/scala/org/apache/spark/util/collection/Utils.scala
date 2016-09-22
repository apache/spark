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

package org.apache.spark.util.collection

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.google.common.collect.{Ordering => GuavaOrdering}

import org.apache.spark.{TaskContext}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{CompletionIterator, SizeEstimator}

/**
 * Utility functions for collections.
 */
private[spark] object Utils {

  /**
   * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
   * and maintains the ordering.
   */
  def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    ordering.leastOf(input.asJava, num).iterator.asScala
  }

  /**
   * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
   * and maintains the ordering.
   */
  def takeOrdered[T: ClassTag](input: Iterator[T], num: Int, ser: Serializer)
      (implicit ord: Ordering[T]): Iterator[T] = {
    val context = TaskContext.get()
    if (context == null || !input.hasNext) {
      return takeOrdered(input, num)(ord)
    }

    val iter = input.buffered
    var size = SizeEstimator.estimate(iter.head)
    if (size == 0) {
      size = 1024
    }

    val executorMemory = Runtime.getRuntime.maxMemory()
    val limit = (executorMemory / size) * 0.1

    if (num < limit) {
      takeOrdered(iter, num)(ord)
    } else {
      val sorter = new ExternalSorter[T, Any, Any](context, None, None, Some(ord), ser)
      sorter.insertAll(iter.map(x => (x, null)))
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
      CompletionIterator[T, Iterator[T]](sorter.iterator.map(_._1).take(num), sorter.stop())
    }
  }
}
