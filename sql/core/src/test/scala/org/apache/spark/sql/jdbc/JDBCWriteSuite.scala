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

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.Row
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._

class JDBCWriteSuite extends FunSuite with BeforeAndAfter {
  val url = "jdbc:h2:mem:testdb2"
  var conn: java.sql.Connection = null

  before {
    Class.forName("org.h2.Driver")
    conn = DriverManager.getConnection(url)
    conn.prepareStatement("create schema test").executeUpdate()
  }

  after {
    conn.close()
  }

  val sc = TestSQLContext.sparkContext

  val arr2x2 = Array[Row](Row.apply("dave", 42), Row.apply("mary", 222))
  val arr1x2 = Array[Row](Row.apply("fred", 3))
  val schema2 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) :: Nil)

  val arr2x3 = Array[Row](Row.apply("dave", 42, 1), Row.apply("mary", 222, 2))
  val schema3 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) ::
      StructField("seq", IntegerType) :: Nil)

  test("Basic CREATE") {
    val df = TestSQLContext.createDataFrame(sc.parallelize(arr2x2), schema2)

    df.createJDBCTable(url, "TEST.BASICCREATETEST", false)
    assert(2 == TestSQLContext.jdbc(url, "TEST.BASICCREATETEST").count)
    assert(2 == TestSQLContext.jdbc(url, "TEST.BASICCREATETEST").collect()(0).length)
  }

  test("CREATE with overwrite") {
    val df = TestSQLContext.createDataFrame(sc.parallelize(arr2x3), schema3)
    val df2 = TestSQLContext.createDataFrame(sc.parallelize(arr1x2), schema2)

    df.createJDBCTable(url, "TEST.DROPTEST", false)
    assert(2 == TestSQLContext.jdbc(url, "TEST.DROPTEST").count)
    assert(3 == TestSQLContext.jdbc(url, "TEST.DROPTEST").collect()(0).length)

    df2.createJDBCTable(url, "TEST.DROPTEST", true)
    assert(1 == TestSQLContext.jdbc(url, "TEST.DROPTEST").count)
    assert(2 == TestSQLContext.jdbc(url, "TEST.DROPTEST").collect()(0).length)
  }

  test("CREATE then INSERT to append") {
    val df = TestSQLContext.createDataFrame(sc.parallelize(arr2x2), schema2)
    val df2 = TestSQLContext.createDataFrame(sc.parallelize(arr1x2), schema2)

    df.createJDBCTable(url, "TEST.APPENDTEST", false)
    df2.insertIntoJDBC(url, "TEST.APPENDTEST", false)
    assert(3 == TestSQLContext.jdbc(url, "TEST.APPENDTEST").count)
    assert(2 == TestSQLContext.jdbc(url, "TEST.APPENDTEST").collect()(0).length)
  }

  test("CREATE then INSERT to truncate") {
    val df = TestSQLContext.createDataFrame(sc.parallelize(arr2x2), schema2)
    val df2 = TestSQLContext.createDataFrame(sc.parallelize(arr1x2), schema2)

    df.createJDBCTable(url, "TEST.TRUNCATETEST", false)
    df2.insertIntoJDBC(url, "TEST.TRUNCATETEST", true)
    assert(1 == TestSQLContext.jdbc(url, "TEST.TRUNCATETEST").count)
    assert(2 == TestSQLContext.jdbc(url, "TEST.TRUNCATETEST").collect()(0).length)
  }

  test("Incompatible INSERT to append") {
    val df = TestSQLContext.createDataFrame(sc.parallelize(arr2x2), schema2)
    val df2 = TestSQLContext.createDataFrame(sc.parallelize(arr2x3), schema3)

    df.createJDBCTable(url, "TEST.INCOMPATIBLETEST", false)
    intercept[org.apache.spark.SparkException] {
      df2.insertIntoJDBC(url, "TEST.INCOMPATIBLETEST", true)
    }
  }

}
