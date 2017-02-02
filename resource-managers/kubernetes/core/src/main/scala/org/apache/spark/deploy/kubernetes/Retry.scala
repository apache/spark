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
package org.apache.spark.deploy.kubernetes

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

private[spark] object Retry extends Logging {

  private def retryableFuture[T]
      (attempt: Int, maxAttempts: Int, interval: Duration, retryMessage: Option[String])
      (f: => Future[T])
      (implicit executionContext: ExecutionContext): Future[T] = {
    f recoverWith {
      case error: Throwable =>
        if (attempt <= maxAttempts) {
          retryMessage.foreach { message =>
            logWarning(s"$message - attempt $attempt of $maxAttempts", error)
          }
          Thread.sleep(interval.toMillis)
          retryableFuture(attempt + 1, maxAttempts, interval, retryMessage)(f)
        } else {
          Future.failed(retryMessage.map(message =>
            new SparkException(s"$message - reached $maxAttempts attempts," +
              s" and aborting task.", error)
          ).getOrElse(error))
        }
    }
  }

  def retry[T]
      (times: Int, interval: Duration, retryMessage: Option[String] = None)
      (f: => T)
      (implicit executionContext: ExecutionContext): Future[T] = {
    retryableFuture(1, times, interval, retryMessage)(Future[T] { f })
  }
}
