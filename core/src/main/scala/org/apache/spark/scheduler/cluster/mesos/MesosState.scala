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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.state._
import scala.concurrent.duration.Duration
import java.util.concurrent._
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.util.Utils
import java.lang.Boolean
import java.util
import java.util.Collections
import org.apache.mesos.MesosNativeLibrary
import org.apache.spark.deploy.SparkCuratorUtil

/**
 * An abstraction over Mesos state abstraction. This to provide automatic retries and
 * serialization of objects, and translating state exceptions into Spark exceptions.
 */
private[spark] class MesosState(conf: SparkConf) {
  val defaultFetchTimeoutMs = conf.getLong("spark.mesos.state.fetch.timeout.ms", 2000)
  val defaultStoreTimeoutMs = conf.getLong("spark.mesos.state.store.timeout.ms", 5000)
  val retries = conf.getInt("spark.mesos.state.store.retries", 3)
  val quorum = conf.getInt("spark.mesos.state.quorum", 1)
  val path = conf.get("spark.mesos.state.path", "/.spark_mesos_dispatcher")
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000

  private val state = conf.get("spark.deploy.recoveryMode", "NONE").toUpperCase() match {
    case "NONE" => new InMemoryState()
    case "ZOOKEEPER" => {
      val servers = conf.get("spark.deploy.zookeeper.url")
      new ZooKeeperState(
        servers,
        ZK_SESSION_TIMEOUT_MILLIS,
        TimeUnit.MILLISECONDS,
        "/spark_mesos_dispatcher")
    }
  }

  assert(retries >= 0, s"Retries must be larger or equal than zero, retries: $retries")

  def fetch[T](name: String, timeout: Option[Duration] = None): Option[(Variable, T)] = {
    val finalTimeout =
      timeout.getOrElse(Duration.create(defaultFetchTimeoutMs, TimeUnit.MILLISECONDS))
    try {
      val variable = state.fetch(name).get(finalTimeout.toMillis, TimeUnit.MILLISECONDS)
      if (variable == null || variable.value().size == 0) {
        None
      } else {
        Option((variable, Utils.deserialize(variable.value()).asInstanceOf[T]))
      }
    } catch {
      case e: TimeoutException =>
        throw new SparkException(s"Timed out fetching $name, timeout: $finalTimeout")
      case e: ExecutionException =>
        throw new SparkException(s"Failed to fetch $name, error: $e")
      case e: CancellationException =>
        throw new SparkException("Fetch operation is discarded")
    }
  }

  def store[T](
      name: String,
      variable: Variable,
      value: T,
      timeout: Option[Duration] = None): Variable = {
    val finalTimeout =
      timeout.getOrElse(Duration.create(defaultStoreTimeoutMs, TimeUnit.MILLISECONDS))
    val newVariable = variable.mutate(Utils.serialize(value))
    val future = state.store(newVariable)
    var remainingRuns = retries + 1
    while (remainingRuns > 0) {
      try {
        future.get(finalTimeout.toMillis, TimeUnit.MILLISECONDS)
      } catch {
        case e: TimeoutException =>
          throw new SparkException(s"Timed out storing $name, timeout: $finalTimeout")
        case e: ExecutionException =>
          throw new SparkException(s"Failed to storing $name, error: $e")
        case e: CancellationException =>
          throw new SparkException("Store operation is discarded")
      }

      val status = future.get()
      if (status != null) {
        status
      }
      remainingRuns -= 1
    }

    throw new SparkException(s"Unable to store variable $name after $retries retries")
  }
}
