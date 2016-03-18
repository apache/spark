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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * An extension service that can be loaded into a Spark YARN scheduler.
 * A Service that can be started and stopped.
 *
 * 1. For implementations to be loadable by `SchedulerExtensionServices`,
 * they must provide an empty constructor.
 * 2. The `stop()` operation MUST be idempotent, and succeed even if `start()` was
 * never invoked.
 */
trait SchedulerExtensionService {

  /**
   * Start the extension service. This should be a no-op if
   * called more than once.
   * @param binding binding to the spark application and YARN
   */
  def start(binding: SchedulerExtensionServiceBinding): Unit

  /**
   * Stop the service
   * The `stop()` operation MUST be idempotent, and succeed even if `start()` was
   * never invoked.
   */
  def stop(): Unit
}

/**
 * Binding information for a [[SchedulerExtensionService]].
 *
 * The attempt ID will be set if the service is started within a YARN application master;
 * there is then a different attempt ID for every time that AM is restarted.
 * When the service binding is instantiated in client mode, there's no attempt ID, as it lacks
 * this information.
 * @param sparkContext current spark context
 * @param applicationId YARN application ID
 * @param attemptId YARN attemptID. This will always be unset in client mode, and always set in
 *                  cluster mode.
 */
case class SchedulerExtensionServiceBinding(
    sparkContext: SparkContext,
    applicationId: ApplicationId,
    attemptId: Option[ApplicationAttemptId] = None)

/**
 * Container for [[SchedulerExtensionService]] instances.
 *
 * Loads Extension Services from the configuration property
 * `"spark.yarn.services"`, instantiates and starts them.
 * When stopped, it stops all child entries.
 *
 * The order in which child extension services are started and stopped
 * is undefined.
 */
private[spark] class SchedulerExtensionServices extends SchedulerExtensionService
    with Logging {
  private var serviceOption: Option[String] = None
  private var services: List[SchedulerExtensionService] = Nil
  private val started = new AtomicBoolean(false)
  private var binding: SchedulerExtensionServiceBinding = _

  /**
   * Binding operation will load the named services and call bind on them too; the
   * entire set of services are then ready for `init()` and `start()` calls.
   *
   * @param binding binding to the spark application and YARN
   */
  def start(binding: SchedulerExtensionServiceBinding): Unit = {
    if (started.getAndSet(true)) {
      logWarning("Ignoring re-entrant start operation")
      return
    }
    require(binding.sparkContext != null, "Null context parameter")
    require(binding.applicationId != null, "Null appId parameter")
    this.binding = binding
    val sparkContext = binding.sparkContext
    val appId = binding.applicationId
    val attemptId = binding.attemptId
    logInfo(s"Starting Yarn extension services with app $appId and attemptId $attemptId")

    services = sparkContext.conf.get(SCHEDULER_SERVICES).map { sClass =>
      val instance = Utils.classForName(sClass)
        .newInstance()
        .asInstanceOf[SchedulerExtensionService]
      // bind this service
      instance.start(binding)
      logInfo(s"Service $sClass started")
      instance
    }.toList
  }

  /**
   * Get the list of services.
   *
   * @return a list of services; Nil until the service is started
   */
  def getServices: List[SchedulerExtensionService] = services

  /**
   * Stop the services; idempotent.
   *
   */
  override def stop(): Unit = {
    if (started.getAndSet(false)) {
      logInfo(s"Stopping $this")
      services.foreach { s =>
        Utils.tryLogNonFatalError(s.stop())
      }
    }
  }

  override def toString(): String = s"""SchedulerExtensionServices
    |(serviceOption=$serviceOption,
    | services=$services,
    | started=$started)""".stripMargin
}
