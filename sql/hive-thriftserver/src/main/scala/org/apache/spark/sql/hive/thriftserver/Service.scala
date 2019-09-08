/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import org.apache.hadoop.hive.conf.HiveConf

trait Service {
  /**
   * Initialize the service.
   *
   * The transition must be from {@link STATE#NOTINITED} to {@link STATE#INITED} unless the
   * operation failed and an exception was raised.
   *
   * @param conf
   * the configuration of the service
   */
  def init(conf: HiveConf): Unit


  /**
   * Start the service.
   *
   * The transition should be from {@link STATE#INITED} to {@link STATE#STARTED} unless the
   * operation failed and an exception was raised.
   */
  def start(): Unit

  /**
   * Stop the service.
   *
   * This operation must be designed to complete regardless of the initial state
   * of the service, including the state of all its internal fields.
   */
  def stop(): Unit

  /**
   * Register an instance of the service state change events.
   *
   * @param listener
   * a new listener
   */
  def register(listener: ServiceStateChangeListener): Unit

  /**
   * Unregister a previously instance of the service state change events.
   *
   * @param listener
   * the listener to unregister.
   */
  def unregister(listener: ServiceStateChangeListener): Unit

  /**
   * Get the name of this service.
   *
   * @return the service name
   */
  def getName: String

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   *
   * @return the current configuration, unless a specific implementation chooses
   *         otherwise.
   */
  def getHiveConf: HiveConf

  /**
   * Get the current service state
   *
   * @return the state of the service
   */
  def getServiceState: Service.STATE

  /**
   * Get the service start time
   *
   * @return the start time of the service. This will be zero if the service
   *         has not yet been started.
   */
  def getStartTime: Long
}

object Service {

  trait STATE

  case object NOTINITED extends STATE

  case object INITED extends STATE

  case object STARTED extends STATE

  case object STOPPED extends STATE

}