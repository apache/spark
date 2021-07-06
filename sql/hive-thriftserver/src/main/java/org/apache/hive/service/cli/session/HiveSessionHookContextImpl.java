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

package org.apache.hive.service.cli.session;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 *
 * HiveSessionHookContextImpl.
 * Session hook context implementation which is created by session  manager
 * and passed to hook invocation.
 */
public class HiveSessionHookContextImpl implements HiveSessionHookContext {

  private final HiveSession hiveSession;

  HiveSessionHookContextImpl(HiveSession hiveSession) {
    this.hiveSession = hiveSession;
  }

  @Override
  public HiveConf getSessionConf() {
    return hiveSession.getHiveConf();
  }


  @Override
  public String getSessionUser() {
    return hiveSession.getUserName();
  }

  @Override
  public String getSessionHandle() {
    return hiveSession.getSessionHandle().toString();
  }
}
