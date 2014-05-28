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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

import org.apache.spark.{ComplexFutureAction, FutureAction, Logging}
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * A set of asynchronous RDD actions available through an implicit conversion.
 * Import `org.apache.spark.SparkContext._` at the top of your program to use these functions.
 */
@Experimental
class AsyncRDDActions[T: ClassTag](self: RDD[T]) extends Serializable with Logging {

  /**
   * Returns a future for counting the number of elements in the RDD.
   */
  def countAsync(): FutureAction[Long] = {
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
      Range(0, self.partitions.size),
      (index: Int, data: Long) => totalCount.addAndGet(data),
      totalCount.get())
  }

  /**
   * Returns a future for retrieving all elements of this RDD.
   */
  def collectAsync(): FutureAction[Seq[T]] = {
    val results = new Array[Array[T]](self.partitions.size)
    self.context.submitJob[T, Array[T], Seq[T]](self, _.toArray, Range(0, self.partitions.size),
      (index, data) => results(index) = data, results.flatten.toSeq)
  }

  /**
   * Returns a future for retrieving the first num elements of the RDD.
   */
  def takeAsync(num: Int): FutureAction[Seq[T]] = {
    val f = new ComplexFutureAction[Seq[T]]

    f.run {
      val results = new ArrayBuffer[T](num)
      val totalParts = self.partitions.length
      var partsScanned = 0
      while (results.size < num && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        var numPartsToTry = 1
        if (partsScanned > 0) {
          // If we didn't find any rows after the first iteration, just try all partitions next.
          // Otherwise, interpolate the number of partitions we need to try, but overestimate it
          // by 50%.
          if (results.size == 0) {
            numPartsToTry = totalParts - 1
          } else {
            numPartsToTry = (1.5 * num * partsScanned / results.size).toInt
          }
        }
        numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

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
    }

    f
  }

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreachAsync(f: T => Unit): FutureAction[Unit] = {
    self.context.submitJob[T, Unit, Unit](self, _.foreach(f), Range(0, self.partitions.size),
      (index, data) => Unit, Unit)
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartitionAsync(f: Iterator[T] => Unit): FutureAction[Unit] = {
    self.context.submitJob[T, Unit, Unit](self, f, Range(0, self.partitions.size),
      (index, data) => Unit, Unit)
  }
}
