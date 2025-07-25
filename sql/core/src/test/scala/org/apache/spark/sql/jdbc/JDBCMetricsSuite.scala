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

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCMetricsSuite extends QueryTest with SharedSparkSession {

  val url = "jdbc:h2:mem:testdb0"
  var conn: java.sql.Connection = null
  val properties = new Properties()
  properties.setProperty("user", "testUser")
  properties.setProperty("password", "testPass")

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement("drop table if exists test.people").executeUpdate()
    conn.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('dany', 1)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('alex', 3)").executeUpdate()
    conn.commit()
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW people
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))
  }

  override def afterAll(): Unit = {
    conn.close()
    super.afterAll()
  }

  test("Test logging of schema fetch time") {
    val df = sql("SELECT * FROM people")
    val leaves = df.queryExecution.executedPlan.collectLeaves().head.metrics
    val testKey = "remoteSchemaFetchTime"
    val optionMetric = leaves.get(testKey)
    assert(optionMetric != null)
    if(optionMetric.isDefined) {
      val metric = optionMetric.get
      assert(metric.value >= 0)
    }
  }

  test("Select *") {
    assert(sql("SELECT * FROM people").collect().length === 3)
  }
}
