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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.analysis.TestRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EqualNullSafe, Expression, If, Literal, Not}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Tests the [[ColumnPruning]] case that, for a [[RowLevelWrite]] whose write relation's column
 * names differ from the table's, prunes the query columns its projections don't read and moves the
 * projections to the ordinals of the pruned query.
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

  private val dataColumns = Seq("pk", "id", "dep", "salary", "bonus", "extra")

  // the data columns of a write that receives pk and salary
  private val writeColumns = Seq("pk", "salary")

  // the original table, and the write relation that the rewrite narrows to `writeColumns`
  private val tableRelation = TestRelation(relationColumns(dataColumns))
  private val narrowRelation = TestRelation(relationColumns(writeColumns))
  private val queryColumns = dataColumns :+ "_partition"

  // the columns of `read` that `SET salary = salary + bonus WHERE dep = 'hr'` needs
  private val updateScanColumns = Set("pk", "dep", "salary", "bonus", "_partition")

  private def column(plan: LogicalPlan, name: String): Attribute = {
    plan.output.find(_.name == name).getOrElse(fail(s"no column $name in ${plan.output}"))
  }

  private def relationColumns(names: Seq[String]): Seq[AttributeReference] =
    names.map(column(read, _).asInstanceOf[AttributeReference])

  private def cond(plan: LogicalPlan): Expression = column(plan, "dep") === "hr"

  // the value of every data column after `SET salary = salary + bonus`
  private def updatedValue(plan: LogicalPlan, name: String): Expression = name match {
    case "salary" => column(plan, "salary") + column(plan, "bonus")
    case _ => column(plan, name)
  }

  private def projection(query: LogicalPlan, names: String*): ProjectingInternalRow = {
    val ordinals = names.map(name => query.output.indexWhere(_.name == name)).toIndexedSeq
    ProjectingInternalRow(DataTypeUtils.fromAttributes(ordinals.map(query.output)), ordinals)
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
    ReplaceData(narrowRelation, cond(read), query, tableRelation, projections)
  }

  private def writeDelta(query: LogicalPlan): WriteDelta = {
    val projections = WriteDeltaProjections(
      Some(projection(query, writeColumns: _*)),
      projection(query, "pk"),
      Some(projection(query, "_partition")))
    WriteDelta(narrowRelation, cond(read), query, tableRelation, projections)
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
    assert(scannedColumns(optimized, read) == updateScanColumns)
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
    val union = optimized.query.collectFirst { case u: Union => u }
      .getOrElse(fail(s"no Union in ${optimized.query}"))
    union.children.foreach { child =>
      assert(child.output.map(_.name) == OPERATION_COLUMN +: writeColumns :+ "_partition")
    }
    assert(scannedColumns(optimized, read) == updateScanColumns)
    assert(scannedColumns(optimized, read2) == Set("pk", "dep", "salary", "_partition"))
  }

  test("WriteDelta: prune the columns the write does not read") {
    val write = writeDelta(updatedRows(Filter(cond(read), read)))
    val optimized = Optimize.execute(write).asInstanceOf[WriteDelta]

    checkPruned(optimized, write)
    assert(scannedColumns(optimized, read) == updateScanColumns)
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
    val expand = optimized.query.collectFirst { case e: Expand => e }
      .getOrElse(fail(s"no Expand in ${optimized.query}"))
    assert(expand.output.map(_.name) == OPERATION_COLUMN +: writeColumns :+ "_partition")
    assert(expand.projections.forall(_.size == 4))
    assert(scannedColumns(optimized, read) == updateScanColumns)
  }

  test("a projection that reads outside the query output is an internal error") {
    val query = updateQuery(read)
    val field = StructField("x", IntegerType)
    Seq(query.output.length, -1).foreach { ordinal =>
      val projections = ReplaceDataProjections(
        ProjectingInternalRow(StructType(Seq(field)), IndexedSeq(ordinal)), None)
      val write = ReplaceData(tableRelation, cond(read), query, tableRelation, projections)
      val message = s"A projection of ReplaceData reads ordinal $ordinal of a query with " +
        s"${query.output.length} columns."

      // the plan can still be printed, to find the rule that left the ordinal behind
      assert(write.references == AttributeSet(query.output.head))
      assert(write.treeString.contains("ReplaceData"))
      checkError(
        exception = intercept[SparkException](write.checkProjectionOrdinals()),
        condition = "INTERNAL_ERROR",
        parameters = Map("message" -> message))
      checkError(
        exception = intercept[SparkException](write.withPrunedQuery(query)),
        condition = "INTERNAL_ERROR",
        parameters = Map("message" -> message))
    }
  }

  test("withPrunedQuery finds each read column by exprId, in any column order") {
    val query = updateQuery(read)
    val write = replaceData(query, writeColumns)
    val operation = query.output.head
    val kept = write.references.toSeq.filterNot(_ == operation).sortBy(_.name).reverse

    val pruned = write.withPrunedQuery(Project(operation +: kept, query)).asInstanceOf[ReplaceData]

    val output = pruned.query.output
    assert(pruned.projections.rowProjection.colOrdinals.map(output(_).name) == writeColumns)
    assert(pruned.projections.metadataProjection.get.colOrdinals.map(output(_).name) ==
      Seq("_partition"))
  }

  test("withPrunedQuery rejects a query that drops a read column or moves the operation column") {
    val query = updateQuery(read)
    val write = replaceData(query, writeColumns)
    val operation = query.output.head
    val salary = column(query, "salary")
    Seq(
      Project(query.output.filterNot(_ == salary), query) ->
        s"The pruned query of ReplaceData drops $salary, which a projection reads.",
      Project(query.output.tail :+ operation, query) ->
        "The pruned query of ReplaceData must start with the operation column."
    ).foreach { case (newQuery, message) =>
      checkError(
        exception = intercept[SparkException](write.withPrunedQuery(newQuery)),
        condition = "INTERNAL_ERROR",
        parameters = Map("message" -> message))
    }
  }

  test("no change when the write relation is not narrowed") {
    // a full-width write without a metadata projection keeps `_partition`, which only the
    // condition reads and its distribution can use
    val query = updateQuery(read)
    val projections = ReplaceDataProjections(projection(query, dataColumns: _*), None)
    // a rule may rebuild the write relation with the same names and new exprIds
    val rebuiltRelation = TestRelation(tableRelation.output.map(_.newInstance()))
    Seq(tableRelation, rebuiltRelation).foreach { writeRelation =>
      val write = ReplaceData(writeRelation, cond(read), query, tableRelation, projections)
      comparePlans(Optimize.execute(write), write, checkAnalysis = false)
    }
  }
}
