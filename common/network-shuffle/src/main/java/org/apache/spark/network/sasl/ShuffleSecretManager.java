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

package org.apache.spark.network.sasl;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.JavaUtils;

/**
 * A class that manages shuffle secret used by the external shuffle service.
 */
public class ShuffleSecretManager implements SecretKeyHolder {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleSecretManager.class);

  private final ConcurrentHashMap<String, String> shuffleSecretMap;

  // Spark user used for authenticating SASL connections
  // Note that this must match the value in org.apache.spark.SecurityManager
  private static final String SPARK_SASL_USER = "sparkSaslUser";

  public ShuffleSecretManager() {
    shuffleSecretMap = new ConcurrentHashMap<>();
  }

  /**
   * Register an application with its secret.
   * Executors need to first authenticate themselves with the same secret before
   * fetching shuffle files written by other executors in this application.
   */
  public void registerApp(String appId, String shuffleSecret) {
    // Always put the new secret information to make sure it's the most up to date.
    // Otherwise we have to specifically look at the application attempt in addition
    // to the applicationId since the secrets change between application attempts on yarn.
    shuffleSecretMap.put(appId, shuffleSecret);
    logger.info("Registered shuffle secret for application {}", appId);
  }

  /**
   * Register an application with its secret specified as a byte buffer.
   */
  public void registerApp(String appId, ByteBuffer shuffleSecret) {
    registerApp(appId, JavaUtils.bytesToString(shuffleSecret));
  }

  /**
   * Unregister an application along with its secret.
   * This is called when the application terminates.
   */
  public void unregisterApp(String appId) {
    shuffleSecretMap.remove(appId);
    logger.info("Unregistered shuffle secret for application {}", appId);
  }

  /**
   * Return the Spark user for authenticating SASL connections.
   */
  @Override
  public String getSaslUser(String appId) {
    return SPARK_SASL_USER;
  }

  /**
   * Return the secret key registered with the given application.
   * This key is used to authenticate the executors before they can fetch shuffle files
   * written by this application from the external shuffle service. If the specified
   * application is not registered, return null.
   */
  @Override
  public String getSecretKey(String appId) {
    return shuffleSecretMap.get(appId);
  }
}
