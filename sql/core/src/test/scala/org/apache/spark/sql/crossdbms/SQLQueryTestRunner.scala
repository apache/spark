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

package org.apache.spark.sql.crossdbms

/**
 * Trait for classes that can run SQL queries for testing.
 */
trait SQLQueryTestRunner {

  /**
   * Runs a given query and returns a Seq[String] that represents the query result output.
   */
  def runQuery(query: String): Seq[String]

  /**
   * Perform clean up, such as dropping tables and closing the database connection.
   */
  def cleanUp(): Unit
}

/**
 * A runner that takes a JDBC connection and uses it to execute queries.
 */
private[sql] case class JdbcSQLQueryTestRunner(connection: JdbcConnection)
  extends SQLQueryTestRunner {

  def runQuery(query: String): Seq[String] = connection.runQuery(query)

  def cleanUp(): Unit = connection.close()
}