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

package org.apache.spark.sql.connect

import java.util
import java.util.Collections

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.{TableChange, TableWritePrivilege}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Connect-mode equivalent of the repeated-sql() tests added to DataSourceV2DataFrameSuite in the
 * classic path.
 *
 * In Connect, every sql() call creates a fresh plan that is re-analyzed on the server, so it
 * always sees the latest data, schema, and table identity.
 */
class DataSourceV2RepeatedSQLConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

  private def assertRows(actual: Array[Row], expected: Seq[Row]): Unit = {
    val actualStrs = actual.map(_.toString()).toSet
    val expectedStrs = expected.map(_.toString()).toSet
    assert(
      actualStrs == expectedStrs,
      s"Expected ${expected.mkString(", ")} but got ${actual.mkString(", ")}")
  }

  // Scenario 1: external writes

  test("[connect] repeated sql() reflects session write") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100), Row(2, 200)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] repeated sql() reflects external write") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

      // external writer adds (2, 200)
      val serverSession = getServerSession(s)
      val cat = serverSession.sessionState.catalogManager
        .catalog("testcat")
        .asInstanceOf[InMemoryTableCatalog]
      val schema2 = StructType.fromDDL("id INT, salary INT")
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(Array(new BufferedRows(Seq.empty, schema2).withRow(InternalRow(2, 200))))

      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100), Row(2, 200)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 2: external schema changes

  test("[connect] repeated sql() reflects session schema change") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

      s.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
      s.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100, null), Row(2, 200, -1)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] repeated sql() reflects external schema change") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

      // external schema change + data write via catalog API
      val serverSession = getServerSession(s)
      val cat = serverSession.sessionState.catalogManager
        .catalog("testcat")
        .asInstanceOf[InMemoryTableCatalog]
      val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
      cat.alterTable(ident, addCol)

      val schema3 = StructType.fromDDL("id INT, salary INT, new_col INT")
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(
        Array(new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100, null), Row(2, 200, -1)))

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 3: drop and recreate table

  test("[connect] repeated sql() reflects session drop/recreate") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

      s.sql(s"DROP TABLE $T").collect()
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq.empty)

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] repeated sql() reflects external drop/recreate") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

      // external drop and recreate via catalog API
      val serverSession = getServerSession(s)
      val cat = serverSession.sessionState.catalogManager
        .catalog("testcat")
        .asInstanceOf[InMemoryTableCatalog]
      cat.dropTable(ident)
      cat.createTable(
        ident,
        Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)),
        Array.empty,
        Collections.emptyMap[String, String])

      assertRows(s.sql(s"SELECT * FROM $T").collect(), Seq.empty)

      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }
}
