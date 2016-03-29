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

package org.apache.spark.sql.execution.command

import java.io.File

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.test.SharedSQLContext

class DDLSuite extends QueryTest with SharedSQLContext {

  /**
   * Drops database `databaseName` after calling `f`.
   */
  private def withDatabase(dbNames: String*)(f: => Unit): Unit = {
    try f finally {
      dbNames.foreach { name =>
        sqlContext.sql(s"DROP DATABASE IF EXISTS $name CASCADE")
      }
    }
  }

  test("Create/Drop Database") {
    val catalog = sqlContext.sessionState.catalog

    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      withDatabase(dbName) {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)

        sql(s"CREATE DATABASE $dbName")
        val db1 = catalog.getDatabase(dbNameWithoutBackTicks)
        assert(db1 == CatalogDatabase(
          dbNameWithoutBackTicks,
          "",
          System.getProperty("java.io.tmpdir") + File.separator + s"$dbNameWithoutBackTicks.db",
          Map.empty))
        sql(s"DROP DATABASE $dbName CASCADE")
        assert(!catalog.databaseExists(dbNameWithoutBackTicks))
      }
    }
  }

  test("Create Database - database already exists") {
    val catalog = sqlContext.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      val dbNameWithoutBackTicks = cleanIdentifier(dbName)
      withDatabase(dbName) {
        sql(s"CREATE DATABASE $dbName")
        val db1 = catalog.getDatabase(dbNameWithoutBackTicks)
        assert(db1 == CatalogDatabase(
          dbNameWithoutBackTicks,
          "",
          System.getProperty("java.io.tmpdir") + File.separator + s"$dbNameWithoutBackTicks.db",
          Map.empty))

        val message = intercept[AnalysisException] {
          sql(s"CREATE DATABASE $dbName")
        }.getMessage
        assert(message.contains(s"Database '$dbNameWithoutBackTicks' already exists."))
      }
    }
  }

  test("Alter/Describe Database") {
    val catalog = sqlContext.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      withDatabase(dbName) {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)
        val location =
          System.getProperty("java.io.tmpdir") + File.separator + s"$dbNameWithoutBackTicks.db"
        sql(s"CREATE DATABASE $dbName")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", location) ::
            Row("Properties", "") :: Nil)

        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", location) ::
            Row("Properties", "((a,a), (b,b), (c,c))") :: Nil)

        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('d'='d')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", location) ::
            Row("Properties", "((a,a), (b,b), (c,c), (d,d))") :: Nil)
      }
    }
  }

  test("Drop/Alter/Describe Database - database does not exists") {
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      val dbNameWithoutBackTicks = cleanIdentifier(dbName)
      assert(!sqlContext.sessionState.catalog.databaseExists(dbNameWithoutBackTicks))

      var message = intercept[AnalysisException] {
        sql(s"DROP DATABASE $dbName")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' does not exist"))

      message = intercept[AnalysisException] {
        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('d'='d')")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' does not exist"))

      message = intercept[AnalysisException] {
        sql(s"DESCRIBE DATABASE EXTENDED $dbName")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' does not exist"))

      sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  // TODO: ADD a testcase for Drop Database in Restric when we can create tables in SQLContext
}
