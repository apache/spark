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
 * FilterService.
 *
 */
public class FilterService implements Service {


  private final Service service;
  private final long startTime = System.currentTimeMillis();

  public FilterService(Service service) {
    this.service = service;
  }

  @Override
  public void init(HiveConf config) {
    service.init(config);
  }

  @Override
  public void start() {
    service.start();
  }

  @Override
  public void stop() {
    service.stop();
  }


  @Override
  public void register(ServiceStateChangeListener listener) {
    service.register(listener);
  }

  @Override
  public void unregister(ServiceStateChangeListener listener) {
    service.unregister(listener);
  }

  @Override
  public String getName() {
    return service.getName();
  }

  @Override
  public HiveConf getHiveConf() {
    return service.getHiveConf();
  }

  @Override
  public STATE getServiceState() {
    return service.getServiceState();
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

}
