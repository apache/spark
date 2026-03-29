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

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCMetricsSuite extends QueryTest with SharedSparkSession {

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  val properties = new Properties()
  properties.setProperty("user", "testUser")
  properties.setProperty("password", "testPass")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
      .set("spark.sql.catalog.h2.url", url)
      .set("spark.sql.catalog.h2.driver", "org.h2.Driver")
      .set("spark.sql.catalog.h2.pushDownAggregate", "true")
      .set("spark.sql.catalog.h2.pushDownLimit", "true")
      .set("spark.sql.catalog.h2.pushDownOffset", "true")
      .set("spark.sql.catalog.h2.pushDownJoin", "true")

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      val statement: Statement = conn.createStatement()
      statement.addBatch("create schema test")
      statement.addBatch(
        "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)")
      statement.addBatch("insert into test.people values ('dany', 1)")
      statement.addBatch("insert into test.people values ('mary', 2)")
      statement.addBatch("insert into test.people values ('alex', 3)")
      statement.executeBatch()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  private def hasMetricKeyValue(metrics: Map[String, SQLMetric], metricKey: String): Unit = {
    val optionMetric = metrics.get(metricKey)
    assert(optionMetric.isDefined)
    if(optionMetric.isDefined) {
      val metric = optionMetric.get
      assert(metric.value >= 0)
    }
  }

  test("schema fetch time metric: JDBC v1") {
    val df = spark.read
      .format("jdbc")
      .option("url", url)
      .option("query", "SELECT * FROM TEST.PEOPLE")
      .load()
    hasMetricKeyValue(
      df.queryExecution.executedPlan.collectLeaves().head.metrics,
      "remoteSchemaFetchTime")
  }

  test("schema fetch time metric: JDBC v2") {
    val df = sql("SELECT * FROM h2.TEST.PEOPLE")
    hasMetricKeyValue(
      df.queryExecution.executedPlan.collectLeaves().head.metrics,
      "remoteSchemaFetchTime")
  }

  test("schema fetch time metric: DataFrameReader jdbc") {
    val df = spark.read.jdbc(url, "TEST.PEOPLE", Array[String](), new Properties())
    hasMetricKeyValue(
      df.queryExecution.executedPlan.collectLeaves().head.metrics,
      "remoteSchemaFetchTime")
  }
}
