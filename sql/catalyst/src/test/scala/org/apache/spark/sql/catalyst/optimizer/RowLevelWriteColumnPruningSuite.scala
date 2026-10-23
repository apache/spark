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

package org.apache.spark.sql.catalyst.optimizer

import java.util

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualNullSafe, Expression, If, Literal, Not}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Copy, Keep, Update}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.{Column, Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests the [[ColumnPruning]] case that prunes the query columns of a [[RowLevelWrite]] that its
 * projections don't read and moves the projections to the ordinals of the pruned query.
 */
class RowLevelWriteColumnPruningSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Column Pruning", FixedPoint(10),
      ColumnPruning, CollapseProject, RemoveNoopOperators) :: Nil
  }

  // t(pk, id, dep, salary, bonus, extra) with the metadata column _partition
  private val read = LocalRelation(
    $"pk".int.notNull, $"id".int, $"dep".string, $"salary".int, $"bonus".int, $"extra".int,
    $"_partition".string.notNull)

  // the write and original table relations; the pruning never inspects them
  private val tableRelation: DataSourceV2Relation = {
    val table = new Table {
      override def name(): String = "t"
      override def columns(): Array[Column] = Array(Column.create("pk", IntegerType))
      override def capabilities(): util.Set[TableCapability] = util.Set.of[TableCapability]()
    }
    DataSourceV2Relation.create(table, None, None, CaseInsensitiveStringMap.empty())
  }

  private val dataColumns = Seq("pk", "id", "dep", "salary", "bonus", "extra")

  // the data columns of a write that receives pk and salary
  private val writeColumns = Seq("pk", "salary")
  private val queryColumns = dataColumns :+ "_partition"

  private def column(plan: LogicalPlan, name: String): Attribute = {
    plan.output.find(_.name == name).get
  }

  private def cond(plan: LogicalPlan): Expression = column(plan, "dep") === "hr"

  // the value of every data column after `SET salary = salary + bonus`
  private def updatedValue(plan: LogicalPlan, name: String): Expression = name match {
    case "salary" => column(plan, "salary") + column(plan, "bonus")
    case _ => column(plan, name)
  }

  private def projection(query: LogicalPlan, names: String*): ProjectingInternalRow = {
    val ordinals = names.map(name => query.output.indexWhere(_.name == name)).toIndexedSeq
    val fields = ordinals.map { i =>
      val attr = query.output(i)
      StructField(attr.name, attr.dataType, attr.nullable)
    }
    ProjectingInternalRow(StructType(fields), ordinals)
  }

  // the copy-on-write UPDATE query built by RewriteUpdateTable.buildReplaceDataPlan
  private def updateQuery(plan: LogicalPlan): LogicalPlan = {
    val operation = If(cond(plan), Literal(UPDATE_OPERATION), Literal(COPY_OPERATION))
    val values = queryColumns.map { name =>
      if (name == "_partition") {
        column(plan, name)
      } else {
        Alias(If(cond(plan), updatedValue(plan, name), column(plan, name)), name)()
      }
    }
    Project(Alias(operation, OPERATION_COLUMN)() +: values, plan)
  }

  // the updated rows for the matched rows, as the subquery and delta rewrites project them
  private def updatedRows(matched: LogicalPlan): LogicalPlan = Project(
    Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)() +: queryColumns.map { name =>
      if (name == "_partition") column(matched, name)
      else Alias(updatedValue(matched, name), name)()
    },
    matched)

  private def replaceData(query: LogicalPlan, rowNames: Seq[String]): ReplaceData = {
    val projections = ReplaceDataProjections(
      projection(query, rowNames: _*),
      Some(projection(query, "_partition")))
    ReplaceData(tableRelation, cond(read), query, tableRelation, projections)
  }

  private def writeDelta(query: LogicalPlan): WriteDelta = {
    val projections = WriteDeltaProjections(
      Some(projection(query, writeColumns: _*)),
      projection(query, "pk"),
      Some(projection(query, "_partition")))
    WriteDelta(tableRelation, cond(read), query, tableRelation, projections)
  }

  private def scannedColumns(plan: LogicalPlan, relation: LogicalPlan): Set[String] = {
    plan.collect { case node => node.references }.flatten
      .filter(relation.outputSet.contains).map(_.name).toSet
  }

  // the query columns each projection of the write reads
  private def projectedColumns(write: RowLevelWrite): Seq[Seq[Attribute]] = {
    val projections = write match {
      case r: ReplaceData => r.projections.all
      case d: WriteDelta => d.projections.all
    }
    projections.map(_.colOrdinals.map(write.query.output))
  }

  private def checkPruned(optimized: RowLevelWrite, write: RowLevelWrite): Unit = {
    assert(optimized.query.output.map(_.name) ==
      OPERATION_COLUMN +: writeColumns :+ "_partition")
    assert(projectedColumns(optimized) == projectedColumns(write))
    comparePlans(Optimize.execute(optimized), optimized, checkAnalysis = false)
  }

  test("ReplaceData: prune the columns the write does not read") {
    val write = replaceData(updateQuery(read), writeColumns)
    val optimized = Optimize.execute(write).asInstanceOf[ReplaceData]

    checkPruned(optimized, write)
    assert(scannedColumns(optimized, read) == Set("pk", "dep", "salary", "bonus", "_partition"))
  }

  test("ReplaceData: move the projections to the ordinals of the pruned query") {
    val write = replaceData(updateQuery(read), writeColumns)
    assert(write.projections.rowProjection.colOrdinals == Seq(1, 4))
    assert(write.projections.metadataProjection.get.colOrdinals == Seq(7))

    val optimized = Optimize.execute(write).asInstanceOf[ReplaceData]

    assert(optimized.projections.rowProjection.colOrdinals == Seq(1, 2))
    assert(optimized.projections.metadataProjection.get.colOrdinals == Seq(3))
    assert(optimized.projections.rowProjection.schema == write.projections.rowProjection.schema)
  }

  test("ReplaceData: a CHECK filter on the query keeps its columns") {
    val updated = updateQuery(read)
    val write = replaceData(Filter(column(updated, "extra") > 0, updated), writeColumns)
    val optimized = Optimize.execute(write).asInstanceOf[ReplaceData]

    checkPruned(optimized, write)
    assert(optimized.query.exists(_.isInstanceOf[Filter]))
    assert(scannedColumns(optimized, read) ==
      Set("pk", "dep", "salary", "bonus", "extra", "_partition"))
  }

  test("ReplaceData: prune both children of the Union built for subquery conditions") {
    val updated = updatedRows(Filter(cond(read), read))
    val read2 = read.newInstance()
    val remaining = Filter(Not(EqualNullSafe(cond(read2), Literal.TrueLiteral)), read2)
    val copied = Project(
      Alias(Literal(COPY_OPERATION), OPERATION_COLUMN)() +: queryColumns.map(column(remaining, _)),
      remaining)
    val write = replaceData(Union(updated, copied), writeColumns)
    val optimized = Optimize.execute(write).asInstanceOf[ReplaceData]

    checkPruned(optimized, write)
    val union = optimized.query.collectFirst { case u: Union => u }.get
    union.children.foreach { child =>
      assert(child.output.map(_.name) == OPERATION_COLUMN +: writeColumns :+ "_partition")
    }
    assert(scannedColumns(optimized, read) == Set("pk", "dep", "salary", "bonus", "_partition"))
    assert(scannedColumns(optimized, read2) == Set("pk", "dep", "salary", "_partition"))
  }

  test("WriteDelta: prune the columns the write does not read") {
    val write = writeDelta(updatedRows(Filter(cond(read), read)))
    val optimized = Optimize.execute(write).asInstanceOf[WriteDelta]

    checkPruned(optimized, write)
    assert(scannedColumns(optimized, read) == Set("pk", "dep", "salary", "bonus", "_partition"))
  }

  test("WriteDelta: prune the Expand built for delete and reinsert") {
    val matched = Filter(cond(read), read)
    val deleteOutput = Literal(DELETE_OPERATION) +: queryColumns.map { name =>
      if (name == "pk" || name == "_partition") column(matched, name)
      else Literal(null, column(matched, name).dataType)
    }
    val reinsertOutput = Literal(REINSERT_OPERATION) +: queryColumns.map(updatedValue(matched, _))
    val expandOutput = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)() +:
      queryColumns.map(column(matched, _).newInstance())
    val query = Expand(Seq(deleteOutput, reinsertOutput), expandOutput, matched)
    val write = writeDelta(query)
    val optimized = Optimize.execute(write).asInstanceOf[WriteDelta]

    checkPruned(optimized, write)
    val expand = optimized.query.collectFirst { case e: Expand => e }.get
    assert(expand.output.map(_.name) == OPERATION_COLUMN +: writeColumns :+ "_partition")
    assert(expand.projections.forall(_.size == 4))
    assert(scannedColumns(optimized, read) == Set("pk", "dep", "salary", "bonus", "_partition"))
  }

  test("a projection that reads outside the query output is an internal error") {
    val query = updateQuery(read)
    val field = StructField("x", IntegerType)
    Seq(query.output.length, -1).foreach { ordinal =>
      val projections = ReplaceDataProjections(
        ProjectingInternalRow(StructType(Seq(field)), IndexedSeq(ordinal)), None)
      val write = ReplaceData(tableRelation, cond(read), query, tableRelation, projections)

      checkError(
        exception = intercept[SparkException](write.references),
        condition = "INTERNAL_ERROR",
        parameters = Map("message" ->
          s"A projection of ReplaceData reads ordinal $ordinal of a query with 8 columns."))
    }
  }

  test("no change when the projections read every query column") {
    // DELETE and an UPDATE that is not a column update read every data column
    val update = replaceData(updateQuery(read), dataColumns)
    comparePlans(Optimize.execute(update), update, checkAnalysis = false)

    val delete = replaceData(
      Project(Alias(Literal(COPY_OPERATION), OPERATION_COLUMN)() +: read.output,
        Filter(Not(EqualNullSafe(cond(read), Literal.TrueLiteral)), read)),
      dataColumns)
    comparePlans(Optimize.execute(delete), delete, checkAnalysis = false)

    // MERGE reads every target column through MergeRows
    val source = LocalRelation($"spk".int, $"ssalary".int, $"present".boolean)
    val Seq(spk, ssalary, present) = source.output
    val joined = Join(read, source, LeftOuter, Some(column(read, "pk") === spk), JoinHint.NONE)
    def target(name: String): Expression = column(read, name)
    val updateOutput = Literal(UPDATE_OPERATION) +:
      dataColumns.map(name => if (name == "salary") ssalary else target(name)) :+
      target("_partition")
    val copyOutput = Literal(COPY_OPERATION) +: dataColumns.map(target) :+ target("_partition")
    val mergeOutput = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)() +:
      read.output.map(_.newInstance())
    val mergeRows = MergeRows(
      isSourceRowPresent = present,
      isTargetRowPresent = Literal.TrueLiteral,
      matchedInstructions = Seq(Keep(Update, Literal.TrueLiteral, updateOutput)),
      notMatchedInstructions = Nil,
      notMatchedBySourceInstructions = Seq(Keep(Copy, Literal.TrueLiteral, copyOutput)),
      checkCardinality = false,
      output = mergeOutput,
      child = joined)
    val merge = replaceData(mergeRows, dataColumns)
    val optimizedMerge = Optimize.execute(merge).asInstanceOf[ReplaceData]
    assert(optimizedMerge.query.isInstanceOf[MergeRows])
    assert(optimizedMerge.query.output == mergeRows.output)
    assert(optimizedMerge.projections == merge.projections)
  }
}
