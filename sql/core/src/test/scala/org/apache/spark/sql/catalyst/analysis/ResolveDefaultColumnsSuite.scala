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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.write.SupportsCustomSchemaWrite
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ResolveDefaultColumnsSuite extends QueryTest with SharedSparkSession {
  val rule = ResolveDefaultColumns(null)
  // This is the internal storage for the timestamp 2020-12-31 00:00:00.0.
  val literal = Literal(1609401600000000L, TimestampType)
  val table = UnresolvedInlineTable(
    names = Seq("attr1"),
    rows = Seq(Seq(literal)))
  val localRelation = ResolveInlineTables(table).asInstanceOf[LocalRelation]

  def asLocalRelation(result: LogicalPlan): LocalRelation = result match {
    case r: LocalRelation => r
    case _ => fail(s"invalid result operator type: $result")
  }

  test("SPARK-43018: Add DEFAULTs for INSERT from VALUES list with user-defined columns") {
    // Call the 'addMissingDefaultValuesForInsertFromInlineTable' method with one user-specified
    // column. We add a default value of NULL to the row as a result.
    val insertTableSchemaWithoutPartitionColumns = StructType(Seq(
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)))
    val (result: LogicalPlan, _: Boolean) =
      rule.addMissingDefaultValuesForInsertFromInlineTable(
        localRelation, insertTableSchemaWithoutPartitionColumns, numUserSpecifiedColumns = 1)
    val relation = asLocalRelation(result)
    assert(relation.output.map(_.name) == Seq("c1", "c2"))
    val data: Seq[Seq[Any]] = relation.data.map { row =>
      row.toSeq(StructType(relation.output.map(col => StructField(col.name, col.dataType))))
    }
    assert(data == Seq(Seq(literal.value, null)))
  }

  test("SPARK-43018: Add no DEFAULTs for INSERT from VALUES list with no user-defined columns") {
    // Call the 'addMissingDefaultValuesForInsertFromInlineTable' method with zero user-specified
    // columns. The table is unchanged because there are no default columns to add in this case.
    val insertTableSchemaWithoutPartitionColumns = StructType(Seq(
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)))
    val (result: LogicalPlan, _: Boolean) =
      rule.addMissingDefaultValuesForInsertFromInlineTable(
        localRelation, insertTableSchemaWithoutPartitionColumns, numUserSpecifiedColumns = 0)
    assert(asLocalRelation(result) == localRelation)
  }

  test("SPARK-43018: INSERT timestamp values into a table with column DEFAULTs") {
    withTable("t") {
      sql("create table t(id int, ts timestamp) using parquet")
      sql("insert into t (ts) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select null, timestamp'2020-12-31'").collect().head)
    }
  }

  test("SPARK-43085: Column DEFAULT assignment for target tables with multi-part names") {
    withDatabase("demos") {
      sql("create database demos")
      withTable("demos.test_ts") {
        sql("create table demos.test_ts (id int, ts timestamp) using parquet")
        sql("insert into demos.test_ts(ts) values (timestamp'2023-01-01')")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select null, timestamp'2023-01-01'"))
      }
      withTable("demos.test_ts") {
        sql("create table demos.test_ts (id int, ts timestamp) using parquet")
        sql("use database demos")
        sql("insert into test_ts(ts) values (timestamp'2023-01-01')")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select null, timestamp'2023-01-01'"))
      }
    }
  }

  test("SPARK-43313: Column default values with implicit coercion from provided values") {
    withDatabase("demos") {
      sql("create database demos")
      withTable("demos.test_ts") {
        // If the provided default value is a literal of a wider type than the target column, but
        // the literal value fits within the narrower type, just coerce it for convenience.
        sql(
          """create table demos.test_ts (
            |a int default 42L,
            |b timestamp_ntz default '2022-01-02',
            |c date default '2022-01-03',
            |f float default 0D
            |) using parquet""".stripMargin)
        sql("insert into demos.test_ts(a) values (default)")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select 42, timestamp_ntz'2022-01-02', date'2022-01-03', 0f"))
        // If the provided default value is a literal of a different type than the target column
        // such that no coercion is possible, throw an error.
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a int default 'abc') using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"INT\"",
            "defaultValue" -> "'abc'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a timestamp default '2022-01-02') using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"TIMESTAMP\"",
            "defaultValue" -> "'2022-01-02'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a boolean default 'true') using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"BOOLEAN\"",
            "defaultValue" -> "'true'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a int default true) using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"INT\"",
            "defaultValue" -> "true",
            "actualType" -> "\"BOOLEAN\""))
      }
    }
  }

  /**
   * This is a new relation type that defines the 'customSchemaForInserts' method.
   * Its implementation drops the last table column as it represents an internal pseudocolumn.
   */
  case class TableWithCustomInsertSchema(output: Seq[Attribute], numMetadataColumns: Int)
    extends Table with SupportsCustomSchemaWrite {
    override def name: String = "t"
    override def schema: StructType = StructType.fromAttributes(output)
    override def capabilities(): java.util.Set[TableCapability] =
      new java.util.HashSet[TableCapability]()
    override def customSchemaForInserts: StructType =
      StructType(schema.fields.dropRight(numMetadataColumns))
  }

  /** Helper method to generate a DSV2 relation using the above table type. */
  private def relationWithCustomInsertSchema(
      output: Seq[AttributeReference], numMetadataColumns: Int): DataSourceV2Relation = {
    DataSourceV2Relation(
      TableWithCustomInsertSchema(output, numMetadataColumns),
      output,
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty)
  }

  test("SPARK-43313: Add missing default values for MERGE INSERT actions") {
    val testRelation = SubqueryAlias(
      "testRelation",
      relationWithCustomInsertSchema(Seq(
        AttributeReference(
          "a",
          StringType,
          true,
          new MetadataBuilder()
            .putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, "'a'")
            .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, "'a'")
            .build())(),
        AttributeReference(
          "b",
          StringType,
          true,
          new MetadataBuilder()
            .putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, "'b'")
            .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, "'b'")
            .build())(),
        AttributeReference(
          "c",
          StringType,
          true,
          new MetadataBuilder()
            .putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, "'c'")
            .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, "'c'")
            .build())(),
        AttributeReference(
          "pseudocolumn",
          StringType,
          true,
          new MetadataBuilder()
            .putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, "'pseudocolumn'")
            .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, "'pseudocolumn'")
            .build())()),
        numMetadataColumns = 1))
    val testRelation2 =
      SubqueryAlias(
        "testRelation2",
        relationWithCustomInsertSchema(Seq(
          AttributeReference("d", StringType)(),
          AttributeReference("e", StringType)(),
          AttributeReference("f", StringType)()),
        numMetadataColumns = 0))
    val mergePlan = MergeIntoTable(
      targetTable = testRelation,
      sourceTable = testRelation2,
      mergeCondition = EqualTo(testRelation.output.head, testRelation2.output.head),
      matchedActions = Seq(DeleteAction(None)),
      notMatchedActions = Seq(
        InsertAction(
          condition = None,
          assignments = Seq(
            Assignment(
              key = UnresolvedAttribute("a"),
              value = UnresolvedAttribute("DEFAULT")),
            Assignment(
              key = UnresolvedAttribute(Seq("testRelation", "b")),
              value = Literal("xyz"))))),
      notMatchedBySourceActions = Seq(DeleteAction(None)))
    // Run the 'addMissingDefaultValuesForMergeAction' method of the 'ResolveDefaultColumns' rule
    // on an MERGE INSERT action with two assignments, one to the target table's column 'a' and
    // another to the target table's column 'b'.
    val columnNamesWithDefaults = Seq("a", "b", "c")
    val actualMergeAction =
      rule.apply(mergePlan).asInstanceOf[MergeIntoTable].notMatchedActions.head
    val expectedMergeAction =
      InsertAction(
        condition = None,
        assignments = Seq(
          Assignment(key = UnresolvedAttribute("a"), value = Literal("a")),
          Assignment(key = UnresolvedAttribute(Seq("testRelation", "b")), value = Literal("xyz")),
          Assignment(key = UnresolvedAttribute("c"), value = Literal("c"))))
    assert(expectedMergeAction == actualMergeAction)
    // Run the same method on another MERGE DELETE action. There is no change because this method
    // only operates on MERGE INSERT actions.
    assert(rule.addMissingDefaultValuesForMergeAction(
      mergePlan.matchedActions.head, mergePlan, columnNamesWithDefaults) ==
      mergePlan.matchedActions.head)
  }
}
