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

class JdbcPartitioningRDDSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  before {
    Utils.classForName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection("jdbc:derby:target/JdbcPartitioningRDDSuiteDb;create=true")
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
    } finally {
      conn.close()
    }
  }

  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcPartitioningRDD[Int, Int](
      sc,
      () => { DriverManager.getConnection("jdbc:derby:target/JdbcPartitioningRDDSuiteDb") },
      "SELECT DATA FROM FOO WHERE ID = ?",
      1 to 100,
      insertPlaceholders = { case (stmt, args) =>
        stmt.setInt(1, args)
      },
      mapRow = (r: ResultSet) => { r.getInt(1) } ).cache()

    assert(rdd.count === 100)
    assert(rdd.getPartitions.size == 100)
    assert(rdd.reduce(_ + _) === 10100)
  }

  test("using SetPlaceholder") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcPartitioningRDD[Seq[Int], Int](
      sc,
      () => { DriverManager.getConnection("jdbc:derby:target/JdbcPartitioningRDDSuiteDb") },
      s"SELECT DATA FROM FOO WHERE ID IN (${JdbcPartitioningRDD.SetPlaceholder})",
      (1 to 100).grouped(5).toList,
      insertPlaceholders = {
        case (stmt, args) =>
          args.zipWithIndex.foreach { case (id, idx) =>
            stmt.setInt(idx+1, id)
          }
      },
      setupPlaceholders = { case (sql, args) => JdbcPartitioningRDD.insertStatementSetPlaceholders(sql, args.size) },
      mapRow = (r: ResultSet) => { r.getInt(1) } ).cache()
    assert(rdd.count === 100)
    assert(rdd.getPartitions.size == 20)
    assert(rdd.reduce(_ + _) === 10100)
  }

  test("compound arguments with multiple SetPlaceholders") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcPartitioningRDD[(Seq[Int], Seq[Int]), Int](
      sc,
      () => { DriverManager.getConnection("jdbc:derby:target/JdbcPartitioningRDDSuiteDb") },
      s"SELECT DATA FROM FOO WHERE ID IN (${JdbcPartitioningRDD.SetPlaceholder}) AND ID IN (${JdbcPartitioningRDD.SetPlaceholder})",
      (1 to 100).toList.grouped(5).zip((1 to 100).toList.grouped(5)).toList,
      insertPlaceholders = {
        case (stmt, (ids1, ids2)) =>
          ids1.++(ids2).zipWithIndex.foreach { case (id, idx) =>
            stmt.setLong(idx+1, id)
          }
      },
      setupPlaceholders = { case (sql, (ids1, ids2)) =>
        val ids1Replaced = JdbcPartitioningRDD.insertStatementSetPlaceholders(sql, ids1.size)
        JdbcPartitioningRDD.insertStatementSetPlaceholders(ids1Replaced, ids2.size)
      },
      mapRow = (r: ResultSet) => { r.getInt(1) } ).cache()
    assert(rdd.count === 100)
    assert(rdd.getPartitions.size == 20)
    assert(rdd.reduce(_ + _) === 10100)
  }

  after {
    try {
      DriverManager.getConnection("jdbc:derby:target/JdbcPartitioningRDDSuiteDb;shutdown=true")
    } catch {
      case se: SQLException if se.getSQLState == "08006" =>
      // Normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
    }
  }
}
