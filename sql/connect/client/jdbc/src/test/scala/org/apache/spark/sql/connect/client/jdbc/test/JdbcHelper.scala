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

package org.apache.spark.sql.connect.client.jdbc.test

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import scala.util.Using

import org.apache.spark.sql.connect.client.jdbc.SparkConnectDriver

trait JdbcHelper {

  def jdbcUrl: String

  // explicitly load the class to make it known to the DriverManager
  classOf[SparkConnectDriver].getClassLoader

  def withConnection[T](f: Connection => T): T = {
    Using.resource(DriverManager.getConnection(jdbcUrl)) { conn => f(conn) }
  }

  def withStatement[T](f: Statement => T): T = withConnection { conn =>
    Using.resource(conn.createStatement()) { stmt => f(stmt) }
  }

  def withExecuteQuery(query: String)(f: ResultSet => Unit): Unit = {
    withStatement { stmt => withExecuteQuery(stmt, query)(f) }
  }

  def withExecuteQuery(stmt: Statement, query: String)(f: ResultSet => Unit): Unit = {
    Using.resource { stmt.executeQuery(query) } { rs => f(rs) }
  }
}
