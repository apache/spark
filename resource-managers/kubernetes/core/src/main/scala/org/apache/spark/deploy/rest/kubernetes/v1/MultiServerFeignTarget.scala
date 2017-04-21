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
package org.apache.spark.deploy.rest.kubernetes.v1

import feign.{Request, RequestTemplate, RetryableException, Retryer, Target}
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.internal.Logging

private[kubernetes] class MultiServerFeignTarget[T : ClassTag](
    private val servers: Seq[String],
    private val maxRetriesPerServer: Int = 1,
    private val delayBetweenRetriesMillis: Int = 1000) extends Target[T] with Retryer with Logging {
  require(servers.nonEmpty, "Must provide at least one server URI.")

  private val threadLocalShuffledServers = new ThreadLocal[Seq[String]] {
    override def initialValue(): Seq[String] = Random.shuffle(servers)
  }
  private val threadLocalCurrentAttempt = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }

  override def `type`(): Class[T] = {
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  }

  /**
   * Cloning the target is done on every request, for use on the current
   * thread - thus it's important that clone returns a "fresh" target.
   */
  override def clone(): Retryer = {
    reset()
    this
  }

  override def name(): String = {
    s"${getClass.getSimpleName} with servers [${servers.mkString(",")}]"
  }

  override def apply(requestTemplate: RequestTemplate): Request = {
    if (!requestTemplate.url().startsWith("http")) {
      requestTemplate.insert(0, url())
    }
    requestTemplate.request()
  }

  override def url(): String = threadLocalShuffledServers.get.head

  override def continueOrPropagate(e: RetryableException): Unit = {
    threadLocalCurrentAttempt.set(threadLocalCurrentAttempt.get + 1)
    val currentAttempt = threadLocalCurrentAttempt.get
    if (threadLocalCurrentAttempt.get < maxRetriesPerServer) {
      logWarning(s"Attempt $currentAttempt of $maxRetriesPerServer failed for" +
        s" server ${url()}. Retrying request...", e)
      Thread.sleep(delayBetweenRetriesMillis)
    } else {
      val previousUrl = url()
      threadLocalShuffledServers.set(threadLocalShuffledServers.get.drop(1))
      if (threadLocalShuffledServers.get.isEmpty) {
        logError(s"Failed request to all servers $maxRetriesPerServer times.", e)
        throw e
      } else {
        logWarning(s"Failed request to $previousUrl $maxRetriesPerServer times." +
          s" Trying to access ${url()} instead.", e)
        threadLocalCurrentAttempt.set(0)
      }
    }
  }

  def reset(): Unit = {
    threadLocalShuffledServers.set(Random.shuffle(servers))
    threadLocalCurrentAttempt.set(0)
  }
}
