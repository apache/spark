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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.util.ThreadUtils

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

import org.apache.spark.{ComplexFutureAction, FutureAction, Logging}

/**
 * A set of asynchronous RDD actions available through an implicit conversion.
 */
class AsyncRDDActions[T: ClassTag](self: RDD[T]) extends Serializable with Logging {

  /**
   * Returns a future for counting the number of elements in the RDD.
   */
  def countAsync(): FutureAction[Long] = self.withScope {
    val totalCount = new AtomicLong
    self.context.submitJob(
      self,
      (iter: Iterator[T]) => {
        var result = 0L
        while (iter.hasNext) {
          result += 1L
          iter.next()
        }
        result
      },
      Range(0, self.partitions.length),
      (index: Int, data: Long) => totalCount.addAndGet(data),
      totalCount.get())
  }

  /**
   * Returns a future for retrieving all elements of this RDD.
   */
  def collectAsync(): FutureAction[Seq[T]] = self.withScope {
    val results = new Array[Array[T]](self.partitions.length)
    self.context.submitJob[T, Array[T], Seq[T]](self, _.toArray, Range(0, self.partitions.length),
      (index, data) => results(index) = data, results.flatten.toSeq)
  }

  /**
   * Returns a future for retrieving the first num elements of the RDD.
   */
  def takeAsync(num: Int): FutureAction[Seq[T]] = self.withScope {
    val f = new ComplexFutureAction[Seq[T]]

    f.run {
      // This is a blocking action so we should use "AsyncRDDActions.futureExecutionContext" which
      // is a cached thread pool.
      val results = new ArrayBuffer[T](num)
      val totalParts = self.partitions.length
      var partsScanned = 0
      while (results.size < num && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        var numPartsToTry = 1
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, quadruple and retry.
          // Otherwise, interpolate the number of partitions we need to try, but overestimate it
          // by 50%. We also cap the estimation in the end.
          if (results.size == 0) {
            numPartsToTry = partsScanned * 4
          } else {
            // the left side of max is >=1 whenever partsScanned >= 2
            numPartsToTry = Math.max(1,
              (1.5 * num * partsScanned / results.size).toInt - partsScanned)
            numPartsToTry = Math.min(numPartsToTry, partsScanned * 4)
          }
        }

        val left = num - results.size
        val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)

        val buf = new Array[Array[T]](p.size)
        f.runJob(self,
          (it: Iterator[T]) => it.take(left).toArray,
          p,
          (index: Int, data: Array[T]) => buf(index) = data,
          Unit)

        buf.foreach(results ++= _.take(num - results.size))
        partsScanned += numPartsToTry
      }
      results.toSeq
    }(AsyncRDDActions.futureExecutionContext)

    f
  }

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreachAsync(f: T => Unit): FutureAction[Unit] = self.withScope {
    val cleanF = self.context.clean(f)
    self.context.submitJob[T, Unit, Unit](self, _.foreach(cleanF), Range(0, self.partitions.length),
      (index, data) => Unit, Unit)
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartitionAsync(f: Iterator[T] => Unit): FutureAction[Unit] = self.withScope {
    self.context.submitJob[T, Unit, Unit](self, f, Range(0, self.partitions.length),
      (index, data) => Unit, Unit)
  }
}

private object AsyncRDDActions {
  val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncRDDActions-future", 128))
}
