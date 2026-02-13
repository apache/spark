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

package org.apache.spark.network.shuffle;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores the applications which have recovery disabled.
 */
public final class AppsWithRecoveryDisabled {

  private static final AppsWithRecoveryDisabled INSTANCE = new AppsWithRecoveryDisabled();

  private final Set<String> appsWithRecoveryDisabled = Collections.newSetFromMap(
      new ConcurrentHashMap<>());

  private AppsWithRecoveryDisabled() {
  }

  /**
   * Add an application for which recovery is disabled.
   * @param appId application id
   */
  public static void disableRecoveryOfApp(String appId) {
    INSTANCE.appsWithRecoveryDisabled.add(Objects.requireNonNull(appId));
  }

  /**
   * Returns whether an application is enabled for recovery or not.
   * @param appId application id
   * @return true if the application is enabled for recovery; false otherwise.
   */
  public static boolean isRecoveryEnabledForApp(String appId) {
    return !INSTANCE.appsWithRecoveryDisabled.contains(Objects.requireNonNull(appId));
  }

  /**
   * Removes the application from the store.
   * @param appId application id
   */
  public static void removeApp(String appId) {
    INSTANCE.appsWithRecoveryDisabled.remove(Objects.requireNonNull(appId));
  }
}
