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

package org.apache.spark.sql.execution.datasources.jdbc.connection

import java.sql.{Connection, Driver}

import org.apache.spark.sql.jdbc.JdbcConnectionProvider

private class IntentionallyFaultyConnectionProvider extends JdbcConnectionProvider {
  IntentionallyFaultyConnectionProvider.constructed = true
  throw new IllegalArgumentException("Intentional Exception")
  override val name: String = "IntentionallyFaultyConnectionProvider"
  override def canHandle(driver: Driver, options: Map[String, String]): Boolean = true
  override def getConnection(driver: Driver, options: Map[String, String]): Connection = null
  override def modifiesSecurityContext(
    driver: Driver,
    options: Map[String, String]
  ): Boolean = false
}

private object IntentionallyFaultyConnectionProvider {
  var constructed = false
}
