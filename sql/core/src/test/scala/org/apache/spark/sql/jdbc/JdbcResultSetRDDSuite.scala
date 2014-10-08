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

import java.sql._

import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.TestSQLContext._

class JdbcResultSetRDDSuite extends QueryTest with BeforeAndAfter {

  before {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection("jdbc:derby:target/JdbcSchemaRDDSuiteDb;create=true")
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
    } finally {
      conn.close()
    }
  }

  test("basic functionality") {
    val jdbcResultSetRDD = jdbcResultSet("jdbc:derby:target/JdbcSchemaRDDSuiteDb", "SELECT DATA FROM FOO")
    jdbcResultSetRDD.registerTempTable("foo")

    checkAnswer(
      sql("select count(*) from foo"),
      100
    )
    checkAnswer(
      sql("select sum(DATA) from foo"),
      10100
    )
  }

  after {
    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true")
    } catch {
      case se: SQLException if se.getSQLState == "XJ015" =>
        // normal shutdown
    }
  }
}
