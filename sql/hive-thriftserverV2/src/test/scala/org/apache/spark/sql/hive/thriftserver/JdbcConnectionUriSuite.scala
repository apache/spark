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

package org.apache.spark.sql.hive.thriftserver

import java.sql.DriverManager

import org.apache.hive.jdbc.HiveDriver

import org.apache.spark.util.Utils

class JdbcConnectionUriSuite extends HiveThriftServer2Test {
  Utils.classForName(classOf[HiveDriver].getCanonicalName)

  override def mode: ServerMode.Value = ServerMode.binary

  val JDBC_TEST_DATABASE = "jdbc_test_database"
  val USER = System.getProperty("user.name")
  val PASSWORD = ""

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val jdbcUri = s"jdbc:hive2://localhost:$serverPort/"
    val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
    val statement = connection.createStatement()
    statement.execute(s"CREATE DATABASE $JDBC_TEST_DATABASE")
    connection.close()
  }

  override protected def afterAll(): Unit = {
    try {
      val jdbcUri = s"jdbc:hive2://localhost:$serverPort/"
      val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
      val statement = connection.createStatement()
      statement.execute(s"DROP DATABASE $JDBC_TEST_DATABASE")
      connection.close()
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-17819 Support default database in connection URIs") {
    val jdbcUri = s"jdbc:hive2://localhost:$serverPort/$JDBC_TEST_DATABASE"
    val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
    val statement = connection.createStatement()
    try {
      val resultSet = statement.executeQuery("select current_database()")
      resultSet.next()
      assert(resultSet.getString(1) === JDBC_TEST_DATABASE)
    } finally {
      statement.close()
      connection.close()
    }
  }
}
