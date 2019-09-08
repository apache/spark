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

package org.apache.spark.sql.hive.thriftserver

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.Service._

/**
 * Construct the service.
 *
 * @param name
 * service name
 */
abstract class AbstractService(val name: String) extends Service with Logging {

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private var state: STATE = NOTINITED
  /**
   * Service start time. Will be zero until the service is started.
   */
  private var startTime = 0L
  /**
   * The configuration. Will be null until the service is initialized.
   */
  private var hiveConf: HiveConf = null
  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  final private val listeners = new util.ArrayList[ServiceStateChangeListener]

  def getServiceState: Service.STATE = synchronized {
    state
  }

  /**
   * {@inheritDoc }
   *
   * @throws IllegalStateException
   * if the current service state does not permit
   * this action
   */
  def init(hiveConf: HiveConf): Unit = {
    ensureCurrentState(NOTINITED)
    this.hiveConf = hiveConf
    changeState(INITED)
    logInfo("Service:" + getName + " is inited.")
  }

  def start(): Unit = {
    startTime = System.currentTimeMillis
    ensureCurrentState(INITED)
    changeState(STARTED)
    logInfo("Service:" + getName + " is started.")
  }

  def stop(): Unit = {
    if ((state eq STOPPED) ||
      (state eq INITED) ||
      (state eq NOTINITED)) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return
    }
    ensureCurrentState(STARTED)
    changeState(STOPPED)
    logInfo("Service:" + getName + " is stopped.")
  }

  def register(l: ServiceStateChangeListener): Unit = {
    listeners.add(l)
  }

  def unregister(l: ServiceStateChangeListener): Unit = {
    listeners.remove(l)
  }

  def getName: String = name

  def getHiveConf: HiveConf = hiveConf

  def getStartTime: Long = startTime

  /**
   * Verify that a service is in a given state.
   *
   * @param currentState
   * the desired state
   * @throws IllegalStateException
   * if the service state is different from
   * the desired state
   */
  private def ensureCurrentState(currentState: STATE): Unit = {
    ServiceOperations.ensureCurrentState(state, currentState)
  }

  /**
   * Change to a new state and notify all listeners.
   * This is a private method that is only invoked from synchronized methods,
   * which avoid having to clone the listener list. It does imply that
   * the state change listener methods should be short lived, as they
   * will delay the state transition.
   *
   * @param newState
   * new service state
   */
  private def changeState(newState: STATE): Unit = {
    state = newState
    // notify listeners
    for (l <- listeners.asScala) {
      l.stateChanged(this)
    }
  }
}

