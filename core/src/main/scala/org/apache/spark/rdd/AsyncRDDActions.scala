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
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

import org.apache.spark.{ComplexFutureAction, FutureAction, JobSubmitter}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{RDD_LIMIT_INITIAL_NUM_PARTITIONS, RDD_LIMIT_SCALE_UP_FACTOR}
import org.apache.spark.util.ThreadUtils

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
    val callSite = self.context.getCallSite
    val localProperties = self.context.getLocalProperties
    // Cached thread pool to handle aggregation of subtasks.
    implicit val executionContext = AsyncRDDActions.futureExecutionContext
    val results = new ArrayBuffer[T]
    val totalParts = self.partitions.length

    val scaleUpFactor = Math.max(self.conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)

    /*
      Recursively triggers jobs to scan partitions until either the requested
      number of elements are retrieved, or the partitions to scan are exhausted.
      This implementation is non-blocking, asynchronously handling the
      results of each job and triggering the next job using callbacks on futures.
     */
    def continue(partsScanned: Int)(implicit jobSubmitter: JobSubmitter): Future[Seq[T]] =
      if (results.size >= num || partsScanned >= totalParts) {
        Future.successful(results.toSeq)
      } else {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        var numPartsToTry = self.conf.get(RDD_LIMIT_INITIAL_NUM_PARTITIONS)
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, multiply by
          // limitScaleUpFactor and retry. Otherwise, interpolate the number of partitions we need
          // to try, but overestimate it by 50%. We also cap the estimation in the end.
          if (results.isEmpty) {
            numPartsToTry = partsScanned * scaleUpFactor
          } else {
            // the left side of max is >=1 whenever partsScanned >= 2
            numPartsToTry = Math.max(1,
              (1.5 * num * partsScanned / results.size).toInt - partsScanned)
            numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
          }
        }

        val left = num - results.size
        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)

        val buf = new Array[Array[T]](p.size)
        self.context.setCallSite(callSite)
        self.context.setLocalProperties(localProperties)
        val job = jobSubmitter.submitJob(self,
          (it: Iterator[T]) => it.take(left).toArray,
          p,
          (index: Int, data: Array[T]) => buf(index) = data,
          ())
        job.flatMap { _ =>
          buf.foreach(results ++= _.take(num - results.size))
          continue(partsScanned + p.size)
        }
      }

    new ComplexFutureAction[Seq[T]](continue(0)(_))
  }

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreachAsync(f: T => Unit): FutureAction[Unit] = self.withScope {
    val cleanF = self.context.clean(f)
    self.context.submitJob[T, Unit, Unit](self, _.foreach(cleanF), Range(0, self.partitions.length),
      (index, data) => (), ())
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartitionAsync(f: Iterator[T] => Unit): FutureAction[Unit] = self.withScope {
    self.context.submitJob[T, Unit, Unit](self, f, Range(0, self.partitions.length),
      (index, data) => (), ())
  }
}

private object AsyncRDDActions {
  val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncRDDActions-future", 128))
}
