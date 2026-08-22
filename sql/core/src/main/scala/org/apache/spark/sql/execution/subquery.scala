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

package org.apache.spark.sql.execution

import scala.collection.immutable.TreeSet

import org.apache.spark.QueryContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, ExprId, InSet, ListQuery, Literal, PlanExpression, Predicate, SupportQueryContext}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{LeafLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[BaseSubqueryExec] {
  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit

  /** Updates the expression with a new plan. */
  override def withNewPlan(plan: BaseSubqueryExec): ExecSubqueryExpression
}

object ExecSubqueryExpression {
  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.exists {
      case _: ExecSubqueryExpression => true
      case _ => false
    }
  }
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression with LeafLike[Expression] with SupportQueryContext {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): ScalarSubquery = copy(plan = query)
  def initQueryContext(): Option[QueryContext] = Some(origin.context)

  override lazy val canonicalized: Expression = {
    ScalarSubquery(plan.canonicalized.asInstanceOf[BaseSubqueryExec], ExprId(0))
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      throw QueryExecutionErrors.multipleRowScalarSubqueryError(getContextOrNull())
    }
    if (rows.length == 1) {
      assert(rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      result = rows(0).get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    toLiteral.doGenCode(ctx, ev)
  }

  def toLiteral: Literal = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType)
  }
}

/**
 * The physical node of in-subquery. When this is used for Dynamic Partition Pruning, as the pruning
 * happens at the driver side, we don't broadcast subquery result.
 */
case class InSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    isDynamicPruning: Boolean = true,
    private var resultBroadcast: Broadcast[Array[Any]] = null,
    @transient private var result: Array[Any] = null)
  extends ExecSubqueryExpression with UnaryLike[Expression] with Predicate {

  @transient private lazy val inSet = InSet(child, result.toSet)

  // Mirror the logical InSubquery.nullable: nullable when any output column is nullable
  // (null in any column position produces UNKNOWN on a miss) or when any LHS field is nullable.
  // For multi-column IN the LHS is a CreateNamedStruct whose top-level nullable is always false
  // even when individual field expressions are nullable (SPARK-58481). Both PlanSubqueries and
  // PlanAdaptiveSubqueries wrap multi-column LHS values in CreateNamedStruct, so matching on it
  // here is precise for the current producers. The fallback to child.nullable is safe
  // for the single-column case where child is the bare LHS expression.
  // LEGACY_IN_SUBQUERY_NULLABILITY suppresses only RHS-derived nullability; LHS field nullability
  // is preserved in both modes so that NOT IN on a nullable LHS field always propagates UNKNOWN.
  override def nullable: Boolean = {
    val lhsNullable = child match {
      case cns: CreateNamedStruct => cns.valExprs.exists(_.nullable)
      case _ => child.nullable
    }
    val rhsNullable = !SQLConf.get.getConf(SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY) &&
      plan.output.exists(_.nullable)
    lhsNullable || rhsNullable
  }
  override def toString: String = s"$child IN ${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): InSubqueryExec =
    copy(plan = plan, result = null)
  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(IN_SUBQUERY_EXEC)

  def updateResult(): Unit = {
    val (rows, unavailable) = ProjectedBroadcastValueSubqueryExec.resultOf(plan) match {
      case Some(BroadcastValueResult.Available(values)) => (values, false)
      case Some(BroadcastValueResult.Unavailable) => (Array.empty[InternalRow], true)
      case None => (plan.executeCollect(), false)
    }
    result = if (unavailable) {
      assert(isDynamicPruning,
        "An unavailable projected broadcast value domain is only supported for " +
          "dynamic partition pruning.")
      InSubqueryExecResultState.unavailableResult
    } else if (plan.output.length > 1) {
      rows.asInstanceOf[Array[Any]]
    } else {
      rows.map(_.get(0, child.dataType))
    }
    if (!isDynamicPruning && !isResultUnavailable) {
      resultBroadcast = plan.session.sparkContext.broadcast(result)
    }
  }

  // This is used only by DPP where we don't need broadcast the result.
  def values(): Option[Array[Any]] = if (isResultUnavailable) None else Option(result)

  private[sql] def isResultUnavailable: Boolean =
    InSubqueryExecResultState.isUnavailable(result)

  private def prepareResult(): Unit = {
    require(result != null || resultBroadcast != null, s"$this has not finished")
    if (result == null && resultBroadcast != null) {
      result = resultBroadcast.value
    }
  }

  // Invariant schema/ordering data for the multi-column evaluator, computed once after the result
  // is available. @transient so that serialization (result=null) does not trigger evaluation.
  @transient private lazy val multiColFieldTypes: Array[DataType] =
    plan.output.map(_.dataType).toArray
  @transient private lazy val multiColFieldOrderings: Array[Ordering[Any]] =
    multiColFieldTypes.map(TypeUtils.getInterpretedOrdering)
  // Struct-level ordering used to index fully non-null result rows in a TreeSet.
  @transient private lazy val multiColRowOrdering: Ordering[InternalRow] =
    TypeUtils.getInterpretedOrdering(child.dataType).asInstanceOf[Ordering[InternalRow]]

  // Split collected rows into a sorted set of fully non-null rows (O(log n) membership test)
  // and an array of rows that contain at least one null field (must be scanned linearly).
  // Built once; the TreeSet uses the struct-level Catalyst ordering. See SPARK-58481.
  @transient private lazy val (multiColNonNullSet, multiColNullRows) = {
    val withNull = Array.newBuilder[InternalRow]
    val nonNull = TreeSet.newBuilder[InternalRow](multiColRowOrdering)
    result.foreach { r =>
      val row = r.asInstanceOf[InternalRow]
      if (row.anyNull) withNull += row else nonNull += row
    }
    (nonNull.result(), withNull.result())
  }

  // Three-valued IN semantics for multi-column subqueries.
  // Result rows are InternalRow objects; InSet's TreeSet uses Catalyst ordering, but membership
  // cannot distinguish a definitively-false candidate from an indeterminate one.
  //
  // When the LHS struct has no null fields:
  //   Fast path: O(log n) TreeSet lookup against fully non-null result rows for TRUE.
  //   Slow path: linear scan over null-containing result rows only for potential UNKNOWN.
  //
  // When the LHS struct has at least one null field, the fast path cannot be used (a null LHS
  // field produces UNKNOWN against any non-null RHS row whose non-null fields all match). In
  // that case we scan all result rows linearly.
  //
  // Per-candidate three-valued logic: TRUE if every field matches; UNKNOWN if no field is
  // definitively unequal but at least one comparison involves null; FALSE otherwise.
  private def evalMultiColumn(inputRow: InternalRow): Any = {
    val value = child.eval(inputRow)
    if (value == null) return null
    val inputStruct = value.asInstanceOf[InternalRow]
    val fieldTypes = multiColFieldTypes
    val orderings = multiColFieldOrderings
    val numFields = fieldTypes.length

    if (!inputStruct.anyNull) {
      // Fast path: indexed lookup among fully non-null candidates.
      if (multiColNonNullSet.contains(inputStruct)) return true
      // Slow path: scan null-containing candidates for potential UNKNOWN.
      // Stop early once hasUnknown is set: the indexed lookup already ruled out TRUE,
      // and every row here contains NULL, so no later candidate can improve UNKNOWN to TRUE.
      var hasUnknown = false
      var i = 0
      while (i < multiColNullRows.length && !hasUnknown) {
        val candidate = multiColNullRows(i)
        var fieldIdx = 0
        var candidateIsUnknown = false
        var candidateIsFalse = false
        while (fieldIdx < numFields && !candidateIsFalse) {
          val candidateField = candidate.get(fieldIdx, fieldTypes(fieldIdx))
          if (candidateField == null) {
            candidateIsUnknown = true
          } else if (orderings(fieldIdx).compare(
              inputStruct.get(fieldIdx, fieldTypes(fieldIdx)), candidateField) != 0) {
            candidateIsFalse = true
          }
          fieldIdx += 1
        }
        if (!candidateIsFalse && candidateIsUnknown) hasUnknown = true
        i += 1
      }
      if (hasUnknown) null else false
    } else {
      // LHS has at least one null field: must scan all result rows because a null LHS field
      // produces UNKNOWN against any non-null RHS row whose other fields all match.
      var hasUnknown = false
      // Scan null-containing result rows first.
      var i = 0
      while (i < multiColNullRows.length && !hasUnknown) {
        val candidate = multiColNullRows(i)
        var fieldIdx = 0
        var candidateIsUnknown = false
        var candidateIsFalse = false
        while (fieldIdx < numFields && !candidateIsFalse) {
          val inputField = inputStruct.get(fieldIdx, fieldTypes(fieldIdx))
          val candidateField = candidate.get(fieldIdx, fieldTypes(fieldIdx))
          if (candidateField == null || inputField == null) {
            candidateIsUnknown = true
          } else if (orderings(fieldIdx).compare(inputField, candidateField) != 0) {
            candidateIsFalse = true
          }
          fieldIdx += 1
        }
        if (!candidateIsFalse && candidateIsUnknown) hasUnknown = true
        i += 1
      }
      // Scan non-null rows: a null LHS comparison is UNKNOWN unless a non-null field differs.
      val nonNullIter = multiColNonNullSet.iterator
      while (nonNullIter.hasNext && !hasUnknown) {
        val candidate = nonNullIter.next()
        var fieldIdx = 0
        var candidateIsUnknown = false
        var candidateIsFalse = false
        while (fieldIdx < numFields && !candidateIsFalse) {
          val inputField = inputStruct.get(fieldIdx, fieldTypes(fieldIdx))
          if (inputField == null) {
            candidateIsUnknown = true
          } else if (orderings(fieldIdx).compare(
              inputField, candidate.get(fieldIdx, fieldTypes(fieldIdx))) != 0) {
            candidateIsFalse = true
          }
          fieldIdx += 1
        }
        if (!candidateIsFalse && candidateIsUnknown) hasUnknown = true
      }
      if (hasUnknown) null else false
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    if (isResultUnavailable) {
      true
    } else if (plan.output.length > 1) {
      evalMultiColumn(input)
    } else {
      inSet.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()
    if (isResultUnavailable) {
      Literal.TrueLiteral.doGenCode(ctx, ev)
    } else if (plan.output.length > 1) {
      // Multi-column: per-candidate three-valued comparison cannot be expressed with InSet's
      // generated code.  Fall back to the interpreted path via eval().
      // Register any Nondeterministic descendants (e.g. rand() in the LHS) for partition-level
      // initialization, mirroring CodegenFallback's protocol.
      val resultIdx = ctx.references.length
      ctx.references += this
      child.foreach {
        case n: expressions.Nondeterministic =>
          val idx = ctx.references.length
          ctx.references += n
          ctx.addPartitionInitializationStatement(
            s"((${classOf[expressions.Nondeterministic].getName}) references[$idx])" +
              s".initialize(partitionIndex);")
        case _ =>
      }
      val tmp = ctx.freshName("inSubqueryTmp")
      ev.copy(code =
        code"""
          Object $tmp =
            ((org.apache.spark.sql.execution.InSubqueryExec) references[$resultIdx])
              .eval(${ctx.INPUT_ROW});
          boolean ${ev.isNull} = ($tmp == null);
          boolean ${ev.value} = !${ev.isNull} && (Boolean)$tmp;
        """)
    } else {
      inSet.doGenCode(ctx, ev)
    }
  }

  override lazy val canonicalized: InSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null,
      result = null)
  }

  override protected def withNewChildInternal(newChild: Expression): InSubqueryExec =
    copy(child = newChild)
}

private[execution] object InSubqueryExecResultState {
  private case object UnavailableMarker

  def unavailableResult: Array[Any] = Array(UnavailableMarker)

  def isUnavailable(result: Array[Any]): Boolean = {
    result != null && result.length == 1 &&
      (result(0).asInstanceOf[AnyRef] eq UnavailableMarker)
  }
}

/**
 * Plans subqueries that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY)) {
      case subquery: expressions.ScalarSubquery =>
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, subquery.plan)
        ScalarSubquery(
          SubqueryExec.createForScalarSubquery(
            s"scalar-subquery#${subquery.exprId.id}", executedPlan),
          subquery.exprId)
      case expressions.InSubquery(values, ListQuery(query, _, exprId, _, _, _)) =>
        val expr = if (values.length == 1) {
          values.head
        } else {
          CreateNamedStruct(
            values.zipWithIndex.flatMap { case (v, index) =>
              Seq(Literal(s"col_$index"), v)
            }
          )
        }
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, query)
        InSubqueryExec(expr, SubqueryExec(s"subquery#${exprId.id}", executedPlan),
          exprId, isDynamicPruning = false)
    }
  }
}
