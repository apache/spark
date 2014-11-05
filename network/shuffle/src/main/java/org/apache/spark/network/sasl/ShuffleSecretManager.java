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

import java.lang.Override;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.sasl.SecretKeyHolder;

/**
 * A class that manages shuffle secret used by the external shuffle service.
 */
public class ShuffleSecretManager implements SecretKeyHolder {
  private final Logger logger = LoggerFactory.getLogger(ShuffleSecretManager.class);
  private final ConcurrentHashMap<String, String> shuffleSecretMap;

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  // Spark user used for authenticating SASL connections
  // Note that this must match the value in org.apache.spark.SecurityManager
  private static final String SPARK_SASL_USER = "sparkSaslUser";

  /**
   * Convert the given string to a byte buffer. The resulting buffer can be converted back to
   * the same string through {@link #bytesToString(ByteBuffer)}. This is used if the external
   * shuffle service represents shuffle secrets as bytes buffers instead of strings.
   */
  public static ByteBuffer stringToBytes(String s) {
    return ByteBuffer.wrap(s.getBytes(UTF8_CHARSET));
  }

  /**
   * Convert the given byte buffer to a string. The resulting string can be converted back to
   * the same byte buffer through {@link #stringToBytes(String)}. This is used if the external
   * shuffle service represents shuffle secrets as bytes buffers instead of strings.
   */
  public static String bytesToString(ByteBuffer b) {
    return new String(b.array(), UTF8_CHARSET);
  }

  public ShuffleSecretManager() {
    shuffleSecretMap = new ConcurrentHashMap<String, String>();
  }

  /**
   * Register an application with its secret.
   * Executors need to first authenticate themselves with the same secret before
   * fetching shuffle files written by other executors in this application.
   */
  public void registerApp(String appId, String shuffleSecret) {
    if (!shuffleSecretMap.contains(appId)) {
      shuffleSecretMap.put(appId, shuffleSecret);
      logger.info("Registered shuffle secret for application {}", appId);
    } else {
      logger.debug("Application {} already registered", appId);
    }
  }

  /**
   * Register an application with its secret specified as a byte buffer.
   */
  public void registerApp(String appId, ByteBuffer shuffleSecret) {
    registerApp(appId, bytesToString(shuffleSecret));
  }

  /**
   * Unregister an application along with its secret.
   * This is called when the application terminates.
   */
  public void unregisterApp(String appId) {
    if (shuffleSecretMap.contains(appId)) {
      shuffleSecretMap.remove(appId);
      logger.info("Unregistered shuffle secret for application {}", appId);
    } else {
      logger.warn("Attempted to unregister application {} when it is not registered", appId);
    }
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
