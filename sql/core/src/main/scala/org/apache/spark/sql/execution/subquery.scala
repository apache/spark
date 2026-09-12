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
  // Captured at construction time to mirror InSet's empty-result short-circuit (SPARK-44550).
  private val legacyNullInEmptyBehavior = SQLConf.get.legacyNullInEmptyBehavior

  // Mirror the logical InSubquery.nullable: nullable when any output column is nullable
  // (a nullable RHS field can produce UNKNOWN on a miss) or when any LHS field is nullable.
  // Only genuinely multi-value IN (values.length > 1) is wrapped in a generated CreateNamedStruct
  // by PlanSubqueries/PlanAdaptiveSubqueries; for values.length == 1 the single expression is
  // passed through unchanged (e.g. an AttributeReference for a struct column stays as-is and is
  // NOT wrapped in CreateNamedStruct). The plan.output.length > 1 guard therefore matches exactly
  // the multi-value case and child is a CreateNamedStruct; for plan.output.length == 1 child is
  // whatever the user wrote, so we fall through to child.nullable directly.
  // LEGACY_IN_SUBQUERY_NULLABILITY suppresses only RHS-derived nullability; LHS field nullability
  // is preserved in both modes so a nullable LHS field can contribute UNKNOWN when no field
  // differs.
  override def nullable: Boolean = {
    val lhsNullable = if (plan.output.length > 1) {
      child match {
        case cns: CreateNamedStruct => cns.valExprs.exists(_.nullable)
        case _ => child.nullable
      }
    } else {
      child.nullable
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

  // Memoized lightweight evaluator instance. @transient so it is rebuilt (from the
  // serializable fields) when InSubqueryExec is deserialized in an executor; constructed
  // only for the multi-column path after prepareResult() has populated result.
  // The child expression is transformed to replace every prepared ScalarSubquery with its
  // already-evaluated Literal, so that no nested BaseSubqueryExec crosses the serialization
  // boundary with generated task closures. See SPARK-58481.
  @transient private[sql] lazy val multiColEvaluator: MultiColumnInSubqueryEvaluator = {
    // On the SQL execution path, updateResult stores rows.asInstanceOf[Array[Any]] where rows is
    // Array[InternalRow], so result is always Array[InternalRow] at runtime. Cast directly to
    // avoid allocating an extra reference array.
    val rows = result.asInstanceOf[Array[InternalRow]]
    val fTypes = plan.output.map(_.dataType).toArray
    // Replace each prepared ScalarSubquery in the LHS with its literal result so the captured
    // child expression carries no BaseSubqueryExec references into executor task closures.
    // Ordering is safe: SparkPlan.executeQuery calls waitForSubqueries() -- which calls
    // updateResult() on every ExecSubqueryExpression found in this plan's expressions, including
    // nested ScalarSubquery nodes inside child -- before doExecute()/doGenCode() runs and
    // accesses this lazy val. So every s.toLiteral call below finds a fully-evaluated result.
    val literalizedChild = child.transformUp {
      case s: ScalarSubquery => s.toLiteral
    }
    new MultiColumnInSubqueryEvaluator(literalizedChild, fTypes, rows, legacyNullInEmptyBehavior)
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    if (isResultUnavailable) {
      true
    } else if (plan.output.length > 1) {
      multiColEvaluator.eval(input)
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
      // generated code. Fall back to the interpreted path via the lightweight evaluator.
      // Register the evaluator (not the full InSubqueryExec) so that task closures do not
      // carry plan (BaseSubqueryExec) or any other driver-side SparkPlan state.
      val evalIdx = ctx.references.length
      ctx.references += multiColEvaluator
      // Register any Nondeterministic descendants for partition-level initialization,
      // mirroring CodegenFallback's protocol. Walk the evaluator's literalized child so that
      // initialize() is called on the same expression instances eval() uses -- if we walked
      // the original child instead, a ScalarSubquery that was replaced by a Literal would
      // leave its Nondeterministic descendants unregistered (harmless here, since ScalarSubquery
      // results are constants, but walking the same child preserves the identity guarantee).
      multiColEvaluator.child.foreach {
        case n: expressions.Nondeterministic =>
          val idx = ctx.references.length
          ctx.references += n
          ctx.addPartitionInitializationStatement(
            s"((${classOf[expressions.Nondeterministic].getName}) references[$idx])" +
              s".initialize(partitionIndex);")
        case _ =>
      }
      val evalClass = classOf[MultiColumnInSubqueryEvaluator].getName
      val tmp = ctx.freshName("inSubqueryTmp")
      ev.copy(code =
        code"""
          Object $tmp =
            (($evalClass) references[$evalIdx]).eval(${ctx.INPUT_ROW});
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

/**
 * Lightweight, serializable evaluator for multi-column IN subqueries.
 * Holds only what executors need: the LHS expression, collected result rows, field metadata,
 * and the legacy empty-list behavior flag. Excludes plan (BaseSubqueryExec) so that
 * task-closure serialization does not carry the driver's physical plan subtree.
 * All @transient lazy fields are rebuilt from the serialized fieldTypes and resultRows
 * after deserialization, mirroring InSubqueryExec's own pattern. See SPARK-58481.
 */
private[sql] class MultiColumnInSubqueryEvaluator(
    val child: Expression,
    val fieldTypes: Array[DataType],
    val resultRows: Array[InternalRow],
    val legacyNullInEmptyBehavior: Boolean) extends Serializable {

  @transient private lazy val fieldOrderings: Array[Ordering[Any]] =
    fieldTypes.map(TypeUtils.getInterpretedOrdering)

  @transient private lazy val rowOrdering: Ordering[InternalRow] =
    TypeUtils.getInterpretedOrdering(child.dataType).asInstanceOf[Ordering[InternalRow]]

  // Split collected rows using the struct-level Catalyst ordering so that duplicates are
  // deduplicated. Fully non-null rows go into nonNullSet (TreeSet) for O(log n)
  // membership tests. Null-containing rows are deduplicated via a temporary TreeSet and
  // then stored as nullRows (Array) for linear scanning; each distinct null-containing row
  // is thus scanned at most once per outer row regardless of RHS duplicate multiplicity.
  @transient private lazy val (nonNullSet, nullRows) = {
    val withNull = TreeSet.newBuilder[InternalRow](rowOrdering)
    val nonNull = TreeSet.newBuilder[InternalRow](rowOrdering)
    resultRows.foreach { r => if (r.anyNull) withNull += r else nonNull += r }
    (nonNull.result(), withNull.result().toArray)
  }

  // Three-valued IN semantics for multi-column subqueries.
  // Result rows are InternalRow objects; InSet's TreeSet uses Catalyst ordering, but
  // membership cannot distinguish a definitively-false candidate from an indeterminate one.
  //
  // When the LHS struct has no null fields:
  //   Fast path: O(log n) TreeSet lookup against fully non-null result rows for TRUE.
  //   Slow path: linear scan over null-containing result rows only for potential UNKNOWN.
  //
  // When the LHS struct has at least one null field, the fast path cannot be used (a null
  // LHS field produces UNKNOWN against any non-null RHS row whose non-null fields all
  // match). Both sets of result rows are scanned linearly, stopping once UNKNOWN is set.
  //
  // Per-candidate three-valued logic: TRUE if every field matches; UNKNOWN if no field is
  // definitively unequal but at least one comparison involves null; FALSE otherwise.
  def eval(inputRow: InternalRow): Any = {
    // When legacyNullInEmptyBehavior=false: IN (empty set) is always FALSE without evaluating
    // the LHS (ANSI mode defaults to false, but an explicit true overrides even ANSI).
    // When legacyNullInEmptyBehavior=true: IN (empty set) returns NULL for a null LHS and
    // FALSE otherwise, so the LHS must be evaluated. Mirror InSet.eval's guard: skip
    // child.eval only when legacyNullInEmptyBehavior is false (SPARK-44550).
    if (resultRows.isEmpty && !legacyNullInEmptyBehavior) return false
    val value = child.eval(inputRow)
    if (value == null) return null
    // Once the LHS is confirmed non-null and the result is empty, IN is always FALSE.
    if (resultRows.isEmpty) return false
    val inputStruct = value.asInstanceOf[InternalRow]
    val fTypes = fieldTypes
    val orderings = fieldOrderings
    val numFields = fTypes.length
    // Cache lazy accessors in locals to avoid re-entering them on every loop iteration.
    val nRows = nullRows
    val nnSet = nonNullSet

    if (!inputStruct.anyNull) {
      // Fast path: indexed lookup among fully non-null candidates.
      if (nnSet.contains(inputStruct)) return true
      // No null-containing candidates: no path to UNKNOWN, result is FALSE.
      if (nRows.isEmpty) return false
      // Materialize LHS fields once before the candidate scans to avoid repeated get().
      val inputFields = Array.tabulate(numFields)(i => inputStruct.get(i, fTypes(i)))
      // Slow path: scan null-containing candidates for potential UNKNOWN.
      // Stop early once hasUnknown is set: the indexed lookup already ruled out TRUE,
      // and every row here contains NULL, so no later candidate can improve to TRUE.
      var hasUnknown = false
      var i = 0
      while (i < nRows.length && !hasUnknown) {
        val candidate = nRows(i)
        var fieldIdx = 0
        var candidateIsUnknown = false
        var candidateIsFalse = false
        while (fieldIdx < numFields && !candidateIsFalse) {
          val candidateField = candidate.get(fieldIdx, fTypes(fieldIdx))
          if (candidateField == null) {
            candidateIsUnknown = true
          } else if (orderings(fieldIdx).compare(inputFields(fieldIdx), candidateField) != 0) {
            candidateIsFalse = true
          }
          fieldIdx += 1
        }
        if (!candidateIsFalse && candidateIsUnknown) hasUnknown = true
        i += 1
      }
      if (hasUnknown) null else false
    } else {
      // LHS has at least one null field: must scan both result sets, stopping once UNKNOWN
      // is established (a null LHS field can produce UNKNOWN against any non-null RHS row
      // whose other fields all match).
      // No candidates at all: result is FALSE (no match possible).
      if (nRows.isEmpty && nnSet.isEmpty) return false
      // Materialize LHS fields once before the scans to avoid repeated get() calls.
      val inputFields = Array.tabulate(numFields)(i => inputStruct.get(i, fTypes(i)))
      var hasUnknown = false
      // Scan null-containing result rows first.
      var i = 0
      while (i < nRows.length && !hasUnknown) {
        val candidate = nRows(i)
        var fieldIdx = 0
        var candidateIsUnknown = false
        var candidateIsFalse = false
        while (fieldIdx < numFields && !candidateIsFalse) {
          val inputField = inputFields(fieldIdx)
          if (inputField == null) {
            // LHS field is null: UNKNOWN regardless of the RHS value; skip candidate.get.
            candidateIsUnknown = true
          } else {
            val candidateField = candidate.get(fieldIdx, fTypes(fieldIdx))
            if (candidateField == null) {
              candidateIsUnknown = true
            } else if (orderings(fieldIdx).compare(inputField, candidateField) != 0) {
              candidateIsFalse = true
            }
          }
          fieldIdx += 1
        }
        if (!candidateIsFalse && candidateIsUnknown) hasUnknown = true
        i += 1
      }
      // Scan non-null rows: a null LHS comparison is UNKNOWN unless a non-null field differs.
      val nonNullIter = nnSet.iterator
      while (nonNullIter.hasNext && !hasUnknown) {
        val candidate = nonNullIter.next()
        var fieldIdx = 0
        var candidateIsUnknown = false
        var candidateIsFalse = false
        while (fieldIdx < numFields && !candidateIsFalse) {
          val inputField = inputFields(fieldIdx)
          if (inputField == null) {
            candidateIsUnknown = true
          } else if (orderings(fieldIdx).compare(
              inputField, candidate.get(fieldIdx, fTypes(fieldIdx))) != 0) {
            candidateIsFalse = true
          }
          fieldIdx += 1
        }
        if (!candidateIsFalse && candidateIsUnknown) hasUnknown = true
      }
      if (hasUnknown) null else false
    }
  }
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
