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

package org.apache.spark.sql.jdbc

import java.sql.{Connection, Driver}

import org.apache.spark.annotation.{DeveloperApi, Unstable}

/**
 * ::DeveloperApi::
 * Connection provider which opens connection toward various databases (database specific instance
 * needed). If any authentication required then it's the provider's responsibility to set all
 * the parameters.
 * Important to mention connection providers within a JVM used from multiple threads so adding
 * internal state is not advised. If any state added then it must be synchronized properly.
 *
 * @since 3.1.0
 */
@DeveloperApi
@Unstable
abstract class JdbcConnectionProvider {
  /**
   * Name of the service to provide JDBC connections. This name should be unique. Spark will
   * internally use this name to differentiate JDBC connection providers.
   */
  val name: String

  /**
   * Checks if this connection provider instance can handle the connection initiated by the driver.
   * There must be exactly one active connection provider which can handle the connection for a
   * specific driver. If this requirement doesn't met then `IllegalArgumentException`
   * will be thrown by the provider framework.
   *
   * @param driver  Java driver which initiates the connection
   * @param options Driver options which initiates the connection
   * @return True if the connection provider can handle the driver with the given options.
   */
  def canHandle(driver: Driver, options: Map[String, String]): Boolean

  /**
   * Opens connection to the database. Since global JVM security configuration change may be
   * needed this API is called synchronized by `SecurityConfigurationLock` to avoid race when
   * `modifiesSecurityContext` returns true for the given driver with the given options.
   *
   * @param driver  Java driver which initiates the connection
   * @param options Driver options which initiates the connection
   * @return a `Connection` object that represents a connection to the URL
   */
  def getConnection(driver: Driver, options: Map[String, String]): Connection

  /**
   * Checks if this connection provider instance needs to modify global security configuration to
   * handle authentication and thus should synchronize access to the security configuration while
   * the given driver is initiating a connection with the given options.
   *
   * @param driver  Java driver which initiates the connection
   * @param options Driver options which initiates the connection
   * @return True if the connection provider will need to modify the security configuration when
   * initiating a connection with the given driver with the given options.
   *
   * @since 3.1.3
   */
  def modifiesSecurityContext(driver: Driver, options: Map[String, String]): Boolean
}
