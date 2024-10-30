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
package org.apache.spark.util

import java.util.concurrent.TimeoutException

import scala.concurrent.Awaitable
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.SparkException

private[spark] object SparkThreadUtils {
  // scalastyle:off awaitresult
  /**
   * Preferred alternative to `Await.result()`.
   *
   * This method wraps and re-throws any exceptions thrown by the underlying `Await` call, ensuring
   * that this thread's stack trace appears in logs.
   *
   * In addition, it calls `Awaitable.result` directly to avoid using `ForkJoinPool`'s
   * `BlockingContext`. Codes running in the user's thread may be in a thread of Scala ForkJoinPool.
   * As concurrent executions in ForkJoinPool may see some [[ThreadLocal]] value unexpectedly, this
   * method basically prevents ForkJoinPool from running other tasks in the current waiting thread.
   * In general, we should use this method because many places in Spark use [[ThreadLocal]] and it's
   * hard to debug when [[ThreadLocal]]s leak to other tasks.
   */
  @throws(classOf[SparkException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      awaitResultNoSparkExceptionConversion(awaitable, atMost)
    } catch {
      case e: SparkFatalException =>
        throw e.throwable
      // TimeoutException is thrown in the current thread, so not need to warp
      // the exception.
      case NonFatal(t)
        if !t.isInstanceOf[TimeoutException] =>
        throw new SparkException("Exception thrown in awaitResult: ", t)
    }
  }

  def awaitResultNoSparkExceptionConversion[T](awaitable: Awaitable[T], atMost: Duration): T = {
    // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
    // See SPARK-13747.
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    awaitable.result(atMost)(awaitPermission)
  }
  // scalastyle:on awaitresult
}
