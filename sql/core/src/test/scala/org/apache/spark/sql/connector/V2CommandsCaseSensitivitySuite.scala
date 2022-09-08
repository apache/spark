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

package org.apache.spark.sql.connector

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, CreateTablePartitioningValidationSuite, ResolvedTable, TestRelation2, TestTable2, UnresolvedFieldName, UnresolvedFieldPosition, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, AlterTableCommand, CreateTableAsSelect, DropColumns, LogicalPlan, QualifiedColType, RenameColumn, ReplaceColumns, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.datasources.PreprocessTableCreation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}

class V2CommandsCaseSensitivitySuite extends SharedSparkSession with AnalysisTest {
  import CreateTablePartitioningValidationSuite._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private val table = ResolvedTable(
    catalog,
    Identifier.of(Array(), "table_name"),
    TestTable2,
    schema.toAttributes)

  override protected def extendedAnalysisRules: Seq[Rule[LogicalPlan]] = {
    Seq(PreprocessTableCreation(spark))
  }

  test("CreateTableAsSelect: using top level field for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("ID", "iD").foreach { ref =>
          val tableSpec = TableSpec(Map.empty, None, Map.empty,
            None, None, None, false)
          val plan = CreateTableAsSelect(
            UnresolvedIdentifier(Array("table_name")),
            Expressions.identity(ref) :: Nil,
            TestRelation2,
            tableSpec,
            Map.empty,
            ignoreIfExists = false)

          if (caseSensitive) {
            assertAnalysisError(plan, Seq("Couldn't find column", ref), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("CreateTableAsSelect: using nested column for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("POINT.X", "point.X", "poInt.x", "poInt.X").foreach { ref =>
          val tableSpec = TableSpec(Map.empty, None, Map.empty,
            None, None, None, false)
          val plan = CreateTableAsSelect(
            UnresolvedIdentifier(Array("table_name")),
            Expressions.bucket(4, ref) :: Nil,
            TestRelation2,
            tableSpec,
            Map.empty,
            ignoreIfExists = false)

          if (caseSensitive) {
            val field = ref.split("\\.")
            assertAnalysisError(plan, Seq("Couldn't find column", field.head), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("ReplaceTableAsSelect: using top level field for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("ID", "iD").foreach { ref =>
          val tableSpec = TableSpec(Map.empty, None, Map.empty,
            None, None, None, false)
          val plan = ReplaceTableAsSelect(
            UnresolvedIdentifier(Array("table_name")),
            Expressions.identity(ref) :: Nil,
            TestRelation2,
            tableSpec,
            Map.empty,
            orCreate = true)

          if (caseSensitive) {
            assertAnalysisError(plan, Seq("Couldn't find column", ref), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("ReplaceTableAsSelect: using nested column for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("POINT.X", "point.X", "poInt.x", "poInt.X").foreach { ref =>
          val tableSpec = TableSpec(Map.empty, None, Map.empty,
            None, None, None, false)
          val plan = ReplaceTableAsSelect(
            UnresolvedIdentifier(Array("table_name")),
            Expressions.bucket(4, ref) :: Nil,
            TestRelation2,
            tableSpec,
            Map.empty,
            orCreate = true)

          if (caseSensitive) {
            val field = ref.split("\\.")
            assertAnalysisError(plan, Seq("Couldn't find column", field.head), caseSensitive)
          } else {
            assertAnalysisSuccess(plan, caseSensitive)
          }
        }
      }
    }
  }

  test("AlterTable: add column - nested") {
    Seq("POINT.Z", "poInt.z", "poInt.Z").foreach { ref =>
      val field = ref.split("\\.")
      alterTableTest(
        AddColumns(
          table,
          Seq(QualifiedColType(
            Some(UnresolvedFieldName(field.init)), field.last, LongType, true, None, None, None))),
        Seq("Missing field " + field.head)
      )
    }
  }

  test("AlterTable: add column resolution - positional") {
    Seq("ID", "iD").foreach { ref =>
      alterTableTest(
        AddColumns(
          table,
          Seq(QualifiedColType(
            None,
            "f",
            LongType,
            true,
            None,
            Some(UnresolvedFieldPosition(ColumnPosition.after(ref))),
            None))),
        Seq("reference column", ref)
      )
    }
  }

  test("AlterTable: add column resolution - column position referencing new column") {
    alterTableTest(
      AddColumns(
        table,
        Seq(QualifiedColType(
          None,
          "x",
          LongType,
          true,
          None,
          Some(UnresolvedFieldPosition(ColumnPosition.after("id"))),
          None),
        QualifiedColType(
          None,
          "y",
          LongType,
          true,
          None,
          Some(UnresolvedFieldPosition(ColumnPosition.after("X"))),
          None))),
      Seq("Couldn't find the reference column for AFTER X at root")
    )
  }

  test("AlterTable: add column resolution - nested positional") {
    Seq("X", "Y").foreach { ref =>
      alterTableTest(
        AddColumns(
          table,
          Seq(QualifiedColType(
            Some(UnresolvedFieldName(Seq("point"))),
            "z",
            LongType,
            true,
            None,
            Some(UnresolvedFieldPosition(ColumnPosition.after(ref))),
            None))),
        Seq("reference column", ref)
      )
    }
  }

  test("AlterTable: add column resolution - column position referencing new nested column") {
    alterTableTest(
      AddColumns(
        table,
        Seq(QualifiedColType(
          Some(UnresolvedFieldName(Seq("point"))),
          "z",
          LongType,
          true,
          None,
          None,
          None),
        QualifiedColType(
          Some(UnresolvedFieldName(Seq("point"))),
          "zz",
          LongType,
          true,
          None,
          Some(UnresolvedFieldPosition(ColumnPosition.after("Z"))),
          None))),
      Seq("Couldn't find the reference column for AFTER Z at point")
    )
  }

  test("SPARK-36372: Adding duplicate columns should not be allowed") {
    alterTableTest(
      AddColumns(
        table,
        Seq(QualifiedColType(
          Some(UnresolvedFieldName(Seq("point"))),
          "z",
          LongType,
          true,
          None,
          None,
          None),
        QualifiedColType(
          Some(UnresolvedFieldName(Seq("point"))),
          "Z",
          LongType,
          true,
          None,
          None,
          None))),
      Seq("Found duplicate column(s) in the user specified columns: `point.z`"),
      expectErrorOnCaseSensitive = false)
  }

  test("SPARK-36381: Check column name exist case sensitive and insensitive when add column") {
    alterTableTest(
      AddColumns(
        table,
        Seq(QualifiedColType(
          None,
          "ID",
          LongType,
          true,
          None,
          Some(UnresolvedFieldPosition(ColumnPosition.after("id"))),
          None))),
      Seq("Cannot add column, because ID already exists in root"),
      expectErrorOnCaseSensitive = false)
  }

  test("SPARK-36381: Check column name exist case sensitive and insensitive when rename column") {
    alterTableTest(
      RenameColumn(table, UnresolvedFieldName(Array("id")), "DATA"),
      Seq("Cannot rename column, because DATA already exists in root"),
      expectErrorOnCaseSensitive = false)
  }

  test("AlterTable: drop column resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      Seq(true, false).foreach { ifExists =>
        val expectedErrors = if (ifExists) {
          Seq.empty[String]
        } else {
          Seq("Missing field " + ref.quoted)
        }
        val alter = DropColumns(table, Seq(UnresolvedFieldName(ref)), ifExists)
        if (ifExists) {
          // using IF EXISTS will silence all errors for missing columns
          assertAnalysisSuccess(alter, caseSensitive = true)
          assertAnalysisSuccess(alter, caseSensitive = false)
        } else {
          alterTableTest(alter, expectedErrors, expectErrorOnCaseSensitive = true)
        }
      }
    }
  }

  test("AlterTable: rename column resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        RenameColumn(table, UnresolvedFieldName(ref), "newName"),
        Seq("Missing field " + ref.quoted)
      )
    }
  }

  test("AlterTable: drop column nullability resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        AlterColumn(table, UnresolvedFieldName(ref), None, Some(true), None, None, None),
        Seq("Missing field " + ref.quoted)
      )
    }
  }

  test("AlterTable: change column type resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        AlterColumn(table, UnresolvedFieldName(ref), Some(StringType), None, None, None, None),
        Seq("Missing field " + ref.quoted)
      )
    }
  }

  test("AlterTable: change column comment resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        AlterColumn(table, UnresolvedFieldName(ref), None, None, Some("comment"), None, None),
        Seq("Missing field " + ref.quoted)
      )
    }
  }

  test("SPARK-36449: Replacing columns with duplicate name should not be allowed") {
    alterTableTest(
      ReplaceColumns(
        table,
        Seq(QualifiedColType(None, "f", LongType, true, None, None, None),
          QualifiedColType(None, "F", LongType, true, None, None, None))),
      Seq("Found duplicate column(s) in the user specified columns: `f`"),
      expectErrorOnCaseSensitive = false)
  }

  private def alterTableTest(
      alter: => AlterTableCommand,
      error: Seq[String],
      expectErrorOnCaseSensitive: Boolean = true): Unit = {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val expectError = if (expectErrorOnCaseSensitive) caseSensitive else !caseSensitive
        if (expectError) {
          assertAnalysisError(alter, error, caseSensitive)
        } else {
          assertAnalysisSuccess(alter, caseSensitive)
        }
      }
    }
  }
}
