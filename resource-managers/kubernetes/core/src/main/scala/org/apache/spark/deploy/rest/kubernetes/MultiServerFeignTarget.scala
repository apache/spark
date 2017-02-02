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
package org.apache.spark.deploy.rest.kubernetes

import feign.{Request, RequestTemplate, RetryableException, Retryer, Target}
import scala.reflect.ClassTag
import scala.util.Random

private[kubernetes] class MultiServerFeignTarget[T : ClassTag](
    private val servers: Seq[String]) extends Target[T] with Retryer {
  require(servers.nonEmpty, "Must provide at least one server URI.")

  private val threadLocalShuffledServers = new ThreadLocal[Seq[String]] {
    override def initialValue(): Seq[String] = Random.shuffle(servers)
  }

  override def `type`(): Class[T] = {
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  }

  override def url(): String = threadLocalShuffledServers.get.head

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

  override def continueOrPropagate(e: RetryableException): Unit = {
    threadLocalShuffledServers.set(threadLocalShuffledServers.get.drop(1))
    if (threadLocalShuffledServers.get.isEmpty) {
      throw e
    }
  }

  def reset(): Unit = {
    threadLocalShuffledServers.set(Random.shuffle(servers))
  }
}
