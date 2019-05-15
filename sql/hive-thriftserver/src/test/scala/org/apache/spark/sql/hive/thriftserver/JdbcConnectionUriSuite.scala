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
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JDBCPartition, JdbcPartitionSaveFailureException, JdbcUtils}
import org.apache.spark.util.Utils

class JdbcConnectionUriSuite extends HiveThriftServer2Test {
  Utils.classForName(classOf[HiveDriver].getCanonicalName)

  override def mode: ServerMode.Value = ServerMode.binary

  val JDBC_TEST_DATABASE = "jdbc_test_database"
  val USER = System.getProperty("user.name")
  val PASSWORD = ""

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val jdbcUri = s"jdbc:hive2://localhost:$serverPort/"
    val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
    val statement = connection.createStatement()
    statement.execute(s"CREATE DATABASE $JDBC_TEST_DATABASE")
    connection.close()
    spark = SparkSession.builder().enableHiveSupport().getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      val jdbcUri = s"jdbc:hive2://localhost:$serverPort/"
      val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
      val statement = connection.createStatement()
      statement.execute(s"DROP DATABASE $JDBC_TEST_DATABASE")
      connection.close()
      if (spark != null) {
        spark.close()
      }
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

  test("SPARK-27716 test transactional save table") {
    val jdbcUri = s"jdbc:hive2://localhost:$serverPort/$JDBC_TEST_DATABASE"
    val params = Map("url" -> jdbcUri, "dbtable" -> "test")
    val options = new JdbcOptionsInWrite(params)

    val mockRdd = new RDD[Row](spark.sparkContext, null) {
      var partitions_ : Array[Partition] = Array(new JDBCPartition("id", 0))
      override def getPartitions: Array[Partition] = partitions_
      override def compute(split: Partition, context: TaskContext): Iterator[Row] = null
      override def foreachPartition(f: Iterator[Row] => Unit): Unit = {}
    }

    val rdd = spark.sparkContext.parallelize(params.toSeq)
    val df = spark.createDataFrame(rdd)
    val schema = df.schema

    val mockdf = mock(classOf[DataFrame])
    when(mockdf.sparkSession).thenReturn(spark)
    when(mockdf.schema).thenReturn(schema)
    when(mockdf.rdd).thenReturn(mockRdd)
    // Because the part num is 1, but accumulator's value is 0.
    intercept[JdbcPartitionSaveFailureException](JdbcUtils.transactionalSaveTable(
      mockdf, Some(schema), false, options))

    val showTables = "show tables"
    val connection = DriverManager.getConnection(jdbcUri, USER, PASSWORD)
    val statement = connection.createStatement()
    // This should consistent with the suffix of temp table name.
    val tempTableSuffix = "temp"
    try {
      val result = statement.executeQuery(showTables)
      while (result.next()) {
        val tbl = result.getString(1)
        // The transactional save table is failed, so the options.table has't been created and
        // the temp table has been dropped.
        assert(tbl != options.table && !tbl.endsWith(tempTableSuffix))
      }
    } finally {
      statement.close()
      connection.close()
    }
  }
}
