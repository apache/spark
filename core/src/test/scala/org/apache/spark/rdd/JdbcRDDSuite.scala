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

package org.apache.spark.rdd

import java.sql._

import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}
import org.apache.spark.util.Utils

class JdbcRDDSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;create=true")
    try {

      try {
        val create = conn.createStatement
        create.execute("""
          CREATE TABLE FOO(
            ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
            DATA INTEGER
          )""")
        create.close()
        val insert = conn.prepareStatement("INSERT INTO FOO(DATA) VALUES(?)")
        (1 to 100).foreach { i =>
          insert.setInt(1, i * 2)
          insert.executeUpdate
        }
        insert.close()
      } catch {
        case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
      }

      try {
        val create = conn.createStatement
        create.execute("CREATE TABLE BIGINT_TEST(ID BIGINT NOT NULL, DATA INTEGER)")
        create.close()
        val insert = conn.prepareStatement("INSERT INTO BIGINT_TEST VALUES(?,?)")
        (1 to 100).foreach { i =>
          insert.setLong(1, 100000000000000000L +  4000000000000000L * i)
          insert.setInt(2, i)
          insert.executeUpdate
        }
        insert.close()
      } catch {
        case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
      }

    } finally {
      conn.close()
    }
  }

  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcRDD(
      sc,
      () => { DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb") },
      "SELECT DATA FROM FOO WHERE ? <= ID AND ID <= ?",
      1, 100, 3,
      (r: ResultSet) => { r.getInt(1) } ).cache()

    assert(rdd.count === 100)
    assert(rdd.reduce(_ + _) === 10100)
  }

  test("large id overflow") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcRDD(
      sc,
      () => { DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb") },
      "SELECT DATA FROM BIGINT_TEST WHERE ? <= ID AND ID <= ?",
      1131544775L, 567279358897692673L, 20,
      (r: ResultSet) => { r.getInt(1) } ).cache()
    assert(rdd.count === 100)
    assert(rdd.reduce(_ + _) === 5050)
  }

  override def afterAll(): Unit = {
    try {
      DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;shutdown=true")
    } catch {
      case se: SQLException if se.getSQLState == "08006" =>
        // Normal single database shutdown
        // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
    }
    super.afterAll()
  }
}
