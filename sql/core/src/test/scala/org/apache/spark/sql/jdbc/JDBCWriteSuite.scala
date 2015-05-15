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

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.Row
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._

class JDBCWriteSuite extends FunSuite with BeforeAndAfter {
  val url = "jdbc:h2:mem:testdb2"
  var conn: java.sql.Connection = null
  val url1 = "jdbc:h2:mem:testdb3"
  var conn1: java.sql.Connection = null
  val properties = new Properties()
  properties.setProperty("user", "testUser")
  properties.setProperty("password", "testPass")
  properties.setProperty("rowId", "false")
    
  before {
    Class.forName("org.h2.Driver")
    conn = DriverManager.getConnection(url)
    conn.prepareStatement("create schema test").executeUpdate()
   
    conn1 = DriverManager.getConnection(url1, properties)
    conn1.prepareStatement("create schema test").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people").executeUpdate()
    conn1.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people1").executeUpdate()
    conn1.prepareStatement(
      "create table test.people1 (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.commit()
     
    TestSQLContext.sql(
      s"""
        |CREATE TEMPORARY TABLE PEOPLE
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
    
    TestSQLContext.sql(
      s"""
        |CREATE TEMPORARY TABLE PEOPLE1
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE1', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))  
  }

  after {
    conn.close()
    conn1.close()
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

    df.createJDBCTable(url1, "TEST.DROPTEST", false, properties)
    assert(2 == TestSQLContext.jdbc(url1, "TEST.DROPTEST", properties).count)
    assert(3 == TestSQLContext.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)

    df2.createJDBCTable(url1, "TEST.DROPTEST", true, properties)
    assert(1 == TestSQLContext.jdbc(url1, "TEST.DROPTEST", properties).count)
    assert(2 == TestSQLContext.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)
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

    df.createJDBCTable(url1, "TEST.TRUNCATETEST", false, properties)
    df2.insertIntoJDBC(url1, "TEST.TRUNCATETEST", true, properties)
    assert(1 == TestSQLContext.jdbc(url1, "TEST.TRUNCATETEST", properties).count)
    assert(2 == TestSQLContext.jdbc(url1, "TEST.TRUNCATETEST", properties).collect()(0).length)
  }

  test("Incompatible INSERT to append") {
    val df = TestSQLContext.createDataFrame(sc.parallelize(arr2x2), schema2)
    val df2 = TestSQLContext.createDataFrame(sc.parallelize(arr2x3), schema3)

    df.createJDBCTable(url, "TEST.INCOMPATIBLETEST", false)
    intercept[org.apache.spark.SparkException] {
      df2.insertIntoJDBC(url, "TEST.INCOMPATIBLETEST", true)
    }
  }
  
  test("INSERT to JDBC Datasource") {
    TestSQLContext.sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 == TestSQLContext.jdbc(url1, "TEST.PEOPLE1", properties).count)
    assert(2 == TestSQLContext.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }
  
  test("INSERT to JDBC Datasource with overwrite") {
    TestSQLContext.sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    TestSQLContext.sql("INSERT OVERWRITE TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 == TestSQLContext.jdbc(url1, "TEST.PEOPLE1", properties).count)
    assert(2 == TestSQLContext.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  } 
}
