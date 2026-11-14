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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NamedRelation, RowLevelAssignmentResolution, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, LogicalPlan, MergeAction, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.InMemoryBaseTable
import org.apache.spark.sql.execution.datasources.v2.InsertOnlyMergeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.tags.DataSourceV2Test

/**
 * Tests MERGE on tables that accept any schema. Spark leaves resolving such a MERGE to the rules
 * that the table injects, so these tests inject a rule that resolves the MERGE and aligns its
 * assignments with the output of its target table, as Spark does for other tables.
 *
 * The MERGE statements reference a metadata column of the target table, so AddMetadataColumns
 * adds the metadata columns to the target table, and the aligned assignments include them. The
 * target table has no alias because MergeIntoTable.skipSchemaResolution looks through only one
 * alias, so Spark itself aligns the assignments of a MERGE whose target table has an alias.
 */
abstract class AcceptAnySchemaMergeIntoTableSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf.set(
    SPARK_SESSION_EXTENSIONS.key,
    classOf[ResolveMergeIntoTableAcceptingAnySchemaExtensions].getName)

  protected def operationTableProps: Map[String, String]

  // the metadata columns are nullable, so that INSERT actions can be aligned with them
  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = operationTableProps ++
      Map("accept-any-schema" -> "true", "nullable-metadata-columns" -> "true")
    props.asJava
  }

  protected def expectedRowIdSchema: Option[StructType]

  protected def expectedWriteLog: Seq[WriteLogEntry]

  test("SPARK-61725: merge into a table accepting any schema handles metadata columns correctly") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      Seq(3, 4, 5, 6).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString
           |USING source s
           |ON test_table.pk = s.pk
           |WHEN MATCHED AND test_table._partition = 'hr' THEN
           | UPDATE SET salary = salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
           |WHEN NOT MATCHED BY SOURCE AND pk = 1 THEN
           | UPDATE SET salary = -1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // update
          Row(2, 200, "software"), // unchanged
          Row(3, 301, "hr"), // update
          Row(4, 401, "hr"), // update
          Row(5, 501, "hr"), // update
          Row(6, 0, "new"))) // insert

      checkLastWriteInfo(
        expectedRowSchema = table.schema,
        expectedRowIdSchema = expectedRowIdSchema,
        expectedMetadataSchema =
          Some(StructType(Array(PARTITION_FIELD_NULLABLE, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(expectedWriteLog: _*)
    }
  }

  test("SPARK-61725: insert-only merge into a table accepting any schema writes data columns") {
    withTempView("source") {
      Seq(1, 2, 3).toDF("pk").createOrReplaceTempView("source")

      // a single NOT MATCHED action and multiple NOT MATCHED actions are rewritten differently
      Seq(
        "WHEN NOT MATCHED THEN INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')",
        """WHEN NOT MATCHED AND s.pk = 2 THEN INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
          |WHEN NOT MATCHED THEN INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
          |""".stripMargin
      ).foreach { clauses =>
        withTable(tableNameAsString) {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |""".stripMargin)

          val executedPlan = executeAndKeepPlan {
            sql(
              s"""MERGE INTO $tableNameAsString
                 |USING source s
                 |ON test_table.pk = s.pk AND test_table._partition = 'hr'
                 |$clauses
                 |""".stripMargin)
          }

          val write = executedPlan.collectFirst {
            case e: InsertOnlyMergeExec => e.write
          }.getOrElse(fail("expected an InsertOnlyMergeExec in the executed plan"))
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#Append]
          assert(append.info.schema.fieldNames === Array("pk", "salary", "dep"))

          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr"), // unchanged (matched)
              Row(2, 200, "software"), // unchanged
              Row(2, 0, "new"), // insert (not matched in hr)
              Row(3, 0, "new"))) // insert
        }
      }
    }
  }

  test("SPARK-61725: merge into a table accepting any schema rejects metadata column assignments") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      Seq(1, 3).toDF("pk").createOrReplaceTempView("source")

      Seq(
        "WHEN MATCHED THEN UPDATE SET salary = -1, index = 5" -> "index",
        // the aligned assignment of the nullable metadata column `index` to null is removed
        "WHEN NOT MATCHED THEN INSERT (pk, salary, dep, _partition) VALUES (s.pk, 0, 'new', 'x')" ->
          "_partition"
      ).foreach { case (clause, metadataCol) =>
        val mergeStmt = s"MERGE INTO $tableNameAsString USING source s " +
          s"ON test_table.pk = s.pk AND test_table._partition = 'hr' $clause"
        val e = intercept[AnalysisException](sql(mergeStmt))
        assert(e.getCondition == "DATATYPE_MISMATCH.INVALID_ROW_LEVEL_OPERATION_ASSIGNMENTS")
        assert(e.getMessage.contains(s"Cannot assign to metadata column '$metadataCol'"))

        withSQLConf(SQLConf.LEGACY_IGNORE_METADATA_COLUMN_ASSIGNMENTS.key -> "true") {
          sql(mergeStmt)
        }
      }

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // update
          Row(2, 200, "software"), // unchanged
          Row(3, 0, "new"))) // insert
    }
  }
}

@DataSourceV2Test
class GroupBasedAcceptAnySchemaMergeIntoTableSuite extends AcceptAnySchemaMergeIntoTableSuiteBase {

  override protected def operationTableProps: Map[String, String] = Map.empty

  override protected def expectedRowIdSchema: Option[StructType] = None

  override protected def expectedWriteLog: Seq[WriteLogEntry] = Seq(
    writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(1, -1, "hr")),
    writeWithMetadataLogEntry(metadata = Row("software", 0), data = Row(2, 200, "software")),
    writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(3, 301, "hr")),
    writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(4, 401, "hr")),
    writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(5, 501, "hr")),
    writeLogEntry(data = Row(6, 0, "new")))
}

@DataSourceV2Test
class DeltaBasedAcceptAnySchemaMergeIntoTableSuite extends AcceptAnySchemaMergeIntoTableSuiteBase {

  override protected def operationTableProps: Map[String, String] =
    Map("supports-deltas" -> "true")

  override protected def expectedRowIdSchema: Option[StructType] = Some(StructType(Array(PK_FIELD)))

  override protected def expectedWriteLog: Seq[WriteLogEntry] = Seq(
    updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(1, -1, "hr")),
    updateWriteLogEntry(id = 3, metadata = Row("hr", null), data = Row(3, 301, "hr")),
    updateWriteLogEntry(id = 4, metadata = Row("hr", null), data = Row(4, 401, "hr")),
    updateWriteLogEntry(id = 5, metadata = Row("hr", null), data = Row(5, 501, "hr")),
    insertWriteLogEntry(data = Row(6, 0, "new")))
}

@DataSourceV2Test
class DeltaBasedAcceptAnySchemaMergeIntoTableUpdateAsDeleteAndInsertSuite
  extends AcceptAnySchemaMergeIntoTableSuiteBase {

  override protected def operationTableProps: Map[String, String] =
    Map("supports-deltas" -> "true", "split-updates" -> "true")

  override protected def expectedRowIdSchema: Option[StructType] = Some(StructType(Array(PK_FIELD)))

  override protected def expectedWriteLog: Seq[WriteLogEntry] = Seq(
    deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
    reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(1, -1, "hr")),
    deleteWriteLogEntry(id = 3, metadata = Row("hr", null)),
    reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(3, 301, "hr")),
    deleteWriteLogEntry(id = 4, metadata = Row("hr", null)),
    reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(4, 401, "hr")),
    deleteWriteLogEntry(id = 5, metadata = Row("hr", null)),
    reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(5, 501, "hr")),
    insertWriteLogEntry(data = Row(6, 0, "new")))
}

/**
 * Resolves a MERGE on a table that accepts any schema and aligns its assignments with the output
 * of its target table, as ResolveReferences and ResolveRowLevelCommandAssignments do for other
 * tables. It supports only what the tests above use: no `*` in actions and no nested fields.
 */
object ResolveMergeIntoTableAcceptingAnySchema extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case m: MergeIntoTable if acceptsAnySchema(m) && m.childrenResolved && !m.resolved =>
      val inBoth = (nameParts: Seq[String]) => m.resolveChildren(nameParts, conf.resolver)
      val inTarget = (nameParts: Seq[String]) => m.targetTable.resolve(nameParts, conf.resolver)
      val inSource = (nameParts: Seq[String]) => m.sourceTable.resolve(nameParts, conf.resolver)

      def resolve(expr: Expression, in: Seq[String] => Option[NamedExpression]): Expression = {
        expr.transformUp { case u: UnresolvedAttribute => in(u.nameParts).getOrElse(u) }
      }

      def resolveAction(
          action: MergeAction,
          in: Seq[String] => Option[NamedExpression]): MergeAction = {
        def resolveAssignments(assignments: Seq[Assignment]): Seq[Assignment] = {
          assignments.map(a => Assignment(resolve(a.key, inTarget), resolve(a.value, in)))
        }
        action match {
          case DeleteAction(cond) =>
            DeleteAction(cond.map(resolve(_, in)))
          case UpdateAction(cond, assignments, fromStar) =>
            UpdateAction(cond.map(resolve(_, in)), resolveAssignments(assignments), fromStar)
          case InsertAction(cond, assignments) =>
            InsertAction(cond.map(resolve(_, in)), resolveAssignments(assignments))
          case other =>
            other
        }
      }

      m.copy(
        mergeCondition = resolve(m.mergeCondition, inBoth),
        matchedActions = m.matchedActions.map(resolveAction(_, inBoth)),
        notMatchedActions = m.notMatchedActions.map(resolveAction(_, inSource)),
        notMatchedBySourceActions = m.notMatchedBySourceActions.map(resolveAction(_, inTarget)))

    case m: MergeIntoTable if m.skipSchemaResolution && m.resolved && !m.aligned =>
      def align(actions: Seq[MergeAction]): Seq[MergeAction] = {
        RowLevelAssignmentResolution.alignActions(
          m.targetTable.output,
          actions,
          coerceNestedTypes = false,
          SchemaAlignmentConfig.DEFAULT.ansiStoreAssignmentCastCheck())
      }
      m.copy(
        matchedActions = align(m.matchedActions),
        notMatchedActions = align(m.notMatchedActions),
        notMatchedBySourceActions = align(m.notMatchedBySourceActions))
  }

  // ResolveReferences looks through all aliases of the target table
  private def acceptsAnySchema(m: MergeIntoTable): Boolean = {
    EliminateSubqueryAliases(m.targetTable) match {
      case r: NamedRelation => r.skipSchemaResolution
      case _ => false
    }
  }
}

class ResolveMergeIntoTableAcceptingAnySchemaExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(_ => ResolveMergeIntoTableAcceptingAnySchema)
  }
}
