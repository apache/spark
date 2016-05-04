/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Service.
 *
 */
public interface Service {

  /**
   * Service states
   */
  enum STATE {
    /** Constructed but not initialized */
    NOTINITED,

    /** Initialized but not started or stopped */
    INITED,

    /** started and not stopped */
    STARTED,

    /** stopped. No further state transitions are permitted */
    STOPPED
  }

  /**
   * Initialize the service.
   *
   * The transition must be from {@link STATE#NOTINITED} to {@link STATE#INITED} unless the
   * operation failed and an exception was raised.
   *
   * @param config
   *          the configuration of the service
   */
  void init(HiveConf conf);


  /**
   * Start the service.
   *
   * The transition should be from {@link STATE#INITED} to {@link STATE#STARTED} unless the
   * operation failed and an exception was raised.
   */
  void start();

  /**
   * Stop the service.
   *
   * This operation must be designed to complete regardless of the initial state
   * of the service, including the state of all its internal fields.
   */
  void stop();

  /**
   * Register an instance of the service state change events.
   *
   * @param listener
   *          a new listener
   */
  void register(ServiceStateChangeListener listener);

  /**
   * Unregister a previously instance of the service state change events.
   *
   * @param listener
   *          the listener to unregister.
   */
  void unregister(ServiceStateChangeListener listener);

  /**
   * Get the name of this service.
   *
   * @return the service name
   */
  String getName();

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   *
   * @return the current configuration, unless a specific implementation chooses
   *         otherwise.
   */
  HiveConf getHiveConf();

  /**
   * Get the current service state
   *
   * @return the state of the service
   */
  STATE getServiceState();

  /**
   * Get the service start time
   *
   * @return the start time of the service. This will be zero if the service
   *         has not yet been started.
   */
  long getStartTime();

}
