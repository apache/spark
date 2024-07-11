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
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, AlterTableCommand, CreateTableAsSelect, DropColumns, LogicalPlan, OptionList, QualifiedColType, RenameColumn, ReplaceColumns, ReplaceTableAsSelect, UnresolvedTableSpec}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.datasources.PreprocessTableCreation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.util.ArrayImplicits._

class V2CommandsCaseSensitivitySuite
  extends SharedSparkSession
  with AnalysisTest
  with QueryErrorsBase {

  import CreateTablePartitioningValidationSuite._

  private val table = ResolvedTable(
    catalog,
    Identifier.of(Array(), "table_name"),
    TestTable2,
    toAttributes(schema))

  override protected def extendedAnalysisRules: Seq[Rule[LogicalPlan]] = {
    Seq(PreprocessTableCreation(spark.sessionState.catalog))
  }

  test("CreateTableAsSelect: using top level field for partitioning") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        Seq("ID", "iD").foreach { ref =>
          val tableSpec =
            UnresolvedTableSpec(Map.empty, None, OptionList(Seq.empty), None, None, None, false)
          val plan = CreateTableAsSelect(
            UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
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
          val tableSpec =
            UnresolvedTableSpec(Map.empty, None, OptionList(Seq.empty), None, None, None, false)
          val plan = CreateTableAsSelect(
            UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
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
          val tableSpec =
            UnresolvedTableSpec(Map.empty, None, OptionList(Seq.empty), None, None, None, false)
          val plan = ReplaceTableAsSelect(
            UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
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
          val tableSpec =
            UnresolvedTableSpec(Map.empty, None, OptionList(Seq.empty), None, None, None, false)
          val plan = ReplaceTableAsSelect(
            UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
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
            Some(UnresolvedFieldName(field.init.toImmutableArraySeq)),
            field.last, LongType, true, None, None, None))),
        expectedErrorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        expectedMessageParameters = Map(
          "objectName" -> s"`${field.head}`",
          "proposal" -> "`id`, `data`, `point`")
      )
    }
  }

  test("AlterTable: add column resolution - positional") {
    Seq("ID", "iD").foreach { ref =>
      val alter = AddColumns(
          table,
          Seq(QualifiedColType(
            None,
            "f",
            LongType,
            true,
            None,
            Some(UnresolvedFieldPosition(ColumnPosition.after(ref))),
            None)))
      Seq(true, false).foreach { caseSensitive =>
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
          assertAnalysisErrorClass(
            inputPlan = alter,
            expectedErrorClass = "FIELD_NOT_FOUND",
            expectedMessageParameters = Map("fieldName" -> "`f`", "fields" -> "id, data, point")
          )
        }
      }
    }
  }

  test("AlterTable: add column resolution - column position referencing new column") {
    val alter = AddColumns(
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
          None)))
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisErrorClass(
          inputPlan = alter,
          expectedErrorClass = "FIELD_NOT_FOUND",
          expectedMessageParameters = Map("fieldName" -> "`y`", "fields" -> "id, data, point, x")
        )
      }
    }
  }

  test("AlterTable: add column resolution - nested positional") {
    Seq("X", "Y").foreach { ref =>
      val alter = AddColumns(
          table,
          Seq(QualifiedColType(
            Some(UnresolvedFieldName(Seq("point"))),
            "z",
            LongType,
            true,
            None,
            Some(UnresolvedFieldPosition(ColumnPosition.after(ref))),
            None)))
      Seq(true, false).foreach { caseSensitive =>
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
          assertAnalysisErrorClass(
            inputPlan = alter,
            expectedErrorClass = "FIELD_NOT_FOUND",
            expectedMessageParameters = Map("fieldName" -> "`z`", "fields" -> "x, y")
          )
        }
      }
    }
  }

  test("AlterTable: add column resolution - column position referencing new nested column") {
    val alter = AddColumns(
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
          None)))
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisErrorClass(
          inputPlan = alter,
          expectedErrorClass = "FIELD_NOT_FOUND",
          expectedMessageParameters = Map("fieldName" -> "`zz`", "fields" -> "x, y, z")
        )
      }
    }
  }

  test("SPARK-36372: Adding duplicate columns should not be allowed") {
    assertAnalysisErrorClass(
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
      "COLUMN_ALREADY_EXISTS",
      Map("columnName" -> toSQLId("point.z")),
      caseSensitive = false)
  }

  test("SPARK-36381: Check column name exist case sensitive and insensitive when add column") {
    alterTableErrorClass(
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
      "FIELD_ALREADY_EXISTS",
      Map(
        "op" -> "add",
        "fieldNames" -> "`ID`",
        "struct" -> "\"STRUCT<id: BIGINT, data: STRING, point: STRUCT<x: DOUBLE, y: DOUBLE>>\""),
      expectErrorOnCaseSensitive = false)
  }

  test("SPARK-36381: Check column name exist case sensitive and insensitive when rename column") {
    alterTableErrorClass(
      RenameColumn(table, UnresolvedFieldName(Array("id").toImmutableArraySeq), "DATA"),
      "FIELD_ALREADY_EXISTS",
      Map(
        "op" -> "rename",
        "fieldNames" -> "`DATA`",
        "struct" -> "\"STRUCT<id: BIGINT, data: STRING, point: STRUCT<x: DOUBLE, y: DOUBLE>>\""),
      expectErrorOnCaseSensitive = false)
  }

  test("AlterTable: drop column resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      Seq(true, false).foreach { ifExists =>
        val alter = DropColumns(table, Seq(UnresolvedFieldName(ref.toImmutableArraySeq)), ifExists)
        if (ifExists) {
          // using IF EXISTS will silence all errors for missing columns
          assertAnalysisSuccess(alter, caseSensitive = true)
          assertAnalysisSuccess(alter, caseSensitive = false)
        } else {
          alterTableTest(
            alter = alter,
            expectedErrorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            expectedMessageParameters = Map(
              "objectName" -> s"${toSQLId(ref.toImmutableArraySeq)}",
              "proposal" -> "`id`, `data`, `point`"
            ),
            expectErrorOnCaseSensitive = true)
        }
      }
    }
  }

  test("AlterTable: rename column resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        alter = RenameColumn(table, UnresolvedFieldName(ref.toImmutableArraySeq), "newName"),
        expectedErrorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        expectedMessageParameters = Map(
          "objectName" -> s"${toSQLId(ref.toImmutableArraySeq)}",
          "proposal" -> "`id`, `data`, `point`")
      )
    }
  }

  test("AlterTable: drop column nullability resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        AlterColumn(table, UnresolvedFieldName(ref.toImmutableArraySeq),
          None, Some(true), None, None, None),
        expectedErrorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        expectedMessageParameters = Map(
          "objectName" -> s"${toSQLId(ref.toImmutableArraySeq)}",
          "proposal" -> "`id`, `data`, `point`")
      )
    }
  }

  test("AlterTable: change column type resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        AlterColumn(table, UnresolvedFieldName(ref.toImmutableArraySeq),
          Some(StringType), None, None, None, None),
        expectedErrorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        expectedMessageParameters = Map(
          "objectName" -> s"${toSQLId(ref.toImmutableArraySeq)}",
          "proposal" -> "`id`, `data`, `point`")
      )
    }
  }

  test("AlterTable: change column comment resolution") {
    Seq(Array("ID"), Array("point", "X"), Array("POINT", "X"), Array("POINT", "x")).foreach { ref =>
      alterTableTest(
        AlterColumn(table, UnresolvedFieldName(ref.toImmutableArraySeq),
          None, None, Some("comment"), None, None),
        expectedErrorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        expectedMessageParameters = Map(
          "objectName" -> s"${toSQLId(ref.toImmutableArraySeq)}",
          "proposal" -> "`id`, `data`, `point`")
      )
    }
  }

  test("SPARK-36449: Replacing columns with duplicate name should not be allowed") {
    assertAnalysisErrorClass(
      ReplaceColumns(
        table,
        Seq(QualifiedColType(None, "f", LongType, true, None, None, None),
          QualifiedColType(None, "F", LongType, true, None, None, None))),
      "COLUMN_ALREADY_EXISTS",
      Map("columnName" -> toSQLId("f")),
      caseSensitive = false)
  }

  private def alterTableTest(
      alter: => AlterTableCommand,
      expectedErrorClass: String,
      expectedMessageParameters: Map[String, String],
      expectErrorOnCaseSensitive: Boolean = true): Unit = {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val expectError = if (expectErrorOnCaseSensitive) caseSensitive else !caseSensitive
        if (expectError) {
          assertAnalysisErrorClass(
            alter, expectedErrorClass, expectedMessageParameters, caseSensitive = caseSensitive)
        } else {
          assertAnalysisSuccess(alter, caseSensitive)
        }
      }
    }
  }

  private def alterTableErrorClass(
      alter: => AlterTableCommand,
      errorClass: String,
      messageParameters: Map[String, String],
      expectErrorOnCaseSensitive: Boolean = true): Unit = {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val expectError = if (expectErrorOnCaseSensitive) caseSensitive else !caseSensitive
        if (expectError) {
          assertAnalysisErrorClass(
            alter, errorClass, messageParameters, caseSensitive = caseSensitive)
        } else {
          assertAnalysisSuccess(alter, caseSensitive)
        }
      }
    }
  }
}
