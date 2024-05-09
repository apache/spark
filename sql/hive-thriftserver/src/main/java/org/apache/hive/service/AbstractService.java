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

package org.apache.hive.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

/**
 * AbstractService.
 *
 */
public abstract class AbstractService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractService.class);

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private Service.STATE state = STATE.NOTINITED;

  /**
   * Service name.
   */
  private final String name;
  /**
   * Service start time. Will be zero until the service is started.
   */
  private long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private HiveConf hiveConf;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final List<ServiceStateChangeListener> listeners =
      new ArrayList<ServiceStateChangeListener>();

  /**
   * Construct the service.
   *
   * @param name
   *          service name
   */
  public AbstractService(String name) {
    this.name = name;
  }

  @Override
  public synchronized Service.STATE getServiceState() {
    return state;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    ensureCurrentState(STATE.NOTINITED);
    this.hiveConf = hiveConf;
    changeState(STATE.INITED);
    LOG.info("Service:{} is inited.", MDC.of(LogKeys.SERVICE_NAME$.MODULE$, getName()));
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void start() {
    startTime = System.currentTimeMillis();
    ensureCurrentState(STATE.INITED);
    changeState(STATE.STARTED);
    LOG.info("Service:{} is started.", MDC.of(LogKeys.SERVICE_NAME$.MODULE$, getName()));
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void stop() {
    if (state == STATE.STOPPED ||
        state == STATE.INITED ||
        state == STATE.NOTINITED) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return;
    }
    ensureCurrentState(STATE.STARTED);
    changeState(STATE.STOPPED);
    LOG.info("Service:{} is stopped.", MDC.of(LogKeys.SERVICE_NAME$.MODULE$, getName()));
  }

  @Override
  public synchronized void register(ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public synchronized void unregister(ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized HiveConf getHiveConf() {
    return hiveConf;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * Verify that a service is in a given state.
   *
   * @param currentState
   *          the desired state
   * @throws IllegalStateException
   *           if the service state is different from
   *           the desired state
   */
  private void ensureCurrentState(Service.STATE currentState) {
    ServiceOperations.ensureCurrentState(state, currentState);
  }

  /**
   * Change to a new state and notify all listeners.
   * This is a private method that is only invoked from synchronized methods,
   * which avoid having to clone the listener list. It does imply that
   * the state change listener methods should be short lived, as they
   * will delay the state transition.
   *
   * @param newState
   *          new service state
   */
  private void changeState(Service.STATE newState) {
    state = newState;
    // notify listeners
    for (ServiceStateChangeListener l : listeners) {
      l.stateChanged(this);
    }
  }

}
