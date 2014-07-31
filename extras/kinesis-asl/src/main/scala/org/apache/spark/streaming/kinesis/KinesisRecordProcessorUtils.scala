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
package org.apache.spark.streaming.kinesis

import scala.util.Random

import org.apache.spark.Logging

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException


/**
 * Helper for the KinesisRecordProcessor.
 */
private[kinesis] object KinesisRecordProcessorUtils extends Logging {
  /**
   * Retry the given amount of times with a random backoff time (millis) less than the
   *   given maxBackOffMillis
   *
   * @param expression expression to evalute
   * @param numRetriesLeft number of retries left
   * @param maxBackOffMillis: max millis between retries
   *
   * @return Evaluation of the given expression
   * @throws Unretryable exception, unexpected exception,
   *  or any exception that persists after numRetriesLeft reaches 0
   */
  @annotation.tailrec
  def retry[T](expression: => T, numRetriesLeft: Int, maxBackOffMillis: Int): T = {
    util.Try { expression } match {
      /** If the function succeeded, evaluate to x. */
      case util.Success(x) => x
      /** If the function failed, either retry or throw the exception */
      case util.Failure(e) => e match {
        /** Retry:  Throttling or other Retryable exception has occurred */
        case _: ThrottlingException | _: KinesisClientLibDependencyException if numRetriesLeft > 1
          => {
               val backOffMillis = Random.nextInt(maxBackOffMillis)
               Thread.sleep(backOffMillis)
               logError(s"Retryable Exception:  Random backOffMillis=${backOffMillis}", e)
               retry(expression, numRetriesLeft - 1, maxBackOffMillis)
             }
        /** Throw:  Shutdown has been requested by the Kinesis Client Library.*/
        case _: ShutdownException => {
          logError(s"ShutdownException:  Caught shutdown exception, skipping checkpoint.", e)
          throw e
        }
        /** Throw:  Non-retryable exception has occurred with the Kinesis Client Library */
        case _: InvalidStateException => {
          logError(s"InvalidStateException:  Cannot save checkpoint to the DynamoDB table used" +
              s" by the Amazon Kinesis Client Library.  Table likely doesn't exist.", e)
          throw e
        }
        /** Throw:  Unexpected exception has occurred */
        case _ => {
          logError(s"Unexpected, non-retryable exception.", e)
          throw e
        }
      }
    }
  }
}
