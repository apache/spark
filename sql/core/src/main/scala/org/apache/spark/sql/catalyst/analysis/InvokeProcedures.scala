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

import scala.jdk.CollectionConverters.IteratorHasAsScala

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GenericInternalRow, Grouping, GroupingID, SubqueryExpression, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.plans.logical.{Call, CommandResult, LocalRelation, LogicalPlan, OneRowRelation, PlanHelper, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.util.ArrayImplicits._

class InvokeProcedures(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (InvokeProcedures.suppressed.get()) plan else invokeProcedures(plan)
  }

  private def invokeProcedures(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // A CALL with an IN or correlated EXISTS subquery, or an aggregate, window function,
    // grouping function or generator, in an argument is left for the analyzer to reject. The
    // optimizer would rewrite the subquery into a join under the `Call`, or could fold away the
    // other expressions.
    case c: Call if c.resolved && c.bound && c.execute && c.checkArgTypes().isSuccess &&
        !c.args.exists(SubqueryExpression.hasInOrCorrelatedExistsSubquery) &&
        PlanHelper.specialExpressionsInUnsupportedOperator(c).isEmpty &&
        !c.args.exists(hasGroupingOrWindowFunctionWithoutOver) =>
      checkSubqueries(c)
      val procedure = c.procedure match {
        case ResolvedBoundProcedure(_, _, p) => p
      }
      session.sessionState.optimizer.execute(c) match {
        case Call(_: ResolvedBoundProcedure, args, _) =>
          invoke(procedure, args, c)
        case optimized =>
          // A rule such as `ExtractPythonUDFs` can add an operator under the `Call` to evaluate an
          // argument. Such an argument reads the output of that operator and does not fold.
          optimized match {
            case Call(_, args, _) => checkFoldable(procedure, args)
            case _ =>
          }
          // Even if every argument folds, invoking the procedure would ignore the operator under
          // the `Call`, so the plan is still unexpected.
          throw SparkException.internalError("Unexpected plan for optimized CALL statement")
      }
  }

  // The two forms that CheckAnalysis rejects with UNSUPPORTED_GROUPING_EXPRESSION and
  // WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE. A window function that is a whole argument is not one.
  private def hasGroupingOrWindowFunctionWithoutOver(arg: Expression): Boolean = arg.exists {
    case _: Grouping | _: GroupingID => true
    case e => !e.isInstanceOf[WindowExpression] && e.children.exists(_.isInstanceOf[WindowFunction])
  }

  /**
   * Checks the subqueries in the arguments as `CheckAnalysis` checks them in a SELECT. It never
   * checks them in a CALL, as this rule replaces the `Call` with its result. Unchecked, an invalid
   * subquery can make the optimizer fail, or lose its invalid part and let the procedure run.
   * A SELECT, unlike a `Call`, allows an uncorrelated EXISTS subquery, which works in a CALL as
   * the optimizer rewrites it into a scalar subquery.
   */
  private def checkSubqueries(call: Call): Unit = {
    val subqueries = call.args.flatMap(_.collect { case s: SubqueryExpression => s })
    if (subqueries.nonEmpty) {
      session.sessionState.analyzer.checkAnalysis(
        Project(subqueries.map(Alias(_, "subquery")()), OneRowRelation()))
    }
  }

  private def invoke(procedure: BoundProcedure, args: Seq[Expression], call: Call): LogicalPlan = {
    val input = toInternalRow(procedure, args)
    val scanIterator = procedure.call(input)
    val relations = scanIterator.asScala.map(toRelation).toSeq
    // Wrap the result in a CommandResult so SparkConnect
    // recognizes it as an eagerly executed SQL Command.
    relations match {
      case Nil =>
        CommandResult(
          Seq.empty,
          call,
          LocalTableScanExec(Seq.empty, Seq.empty, None),
          Seq.empty)
      case Seq(relation: LocalRelation) =>
        CommandResult(
          relation.output,
          call,
          LocalTableScanExec(relation.output, relation.data, None),
          relation.data)
      case _ =>
        throw SparkException.internalError(s"Multi-result procedures are temporarily not supported")
    }
  }

  private def toRelation(scan: Scan): LogicalPlan = scan match {
    case s: LocalScan =>
      val attrs = DataTypeUtils.toAttributes(s.readSchema)
      val data = s.rows.toImmutableArraySeq
      LocalRelation(attrs, data)
    case _ =>
      throw SparkException.internalError(
        s"Only local scans are temporarily supported as procedure output: ${scan.getClass.getName}")
  }

  private def toInternalRow(procedure: BoundProcedure, args: Seq[Expression]): InternalRow = {
    checkFoldable(procedure, args)
    val values = args.map(_.eval()).toArray
    new GenericInternalRow(values)
  }

  private def checkFoldable(procedure: BoundProcedure, args: Seq[Expression]): Unit = {
    // An argument that does not fold even after optimization, such as a nondeterministic
    // expression or a scalar subquery the optimizer cannot inline, cannot be evaluated here.
    args.zip(procedure.parameters).foreach { case (arg, param) =>
      if (!arg.foldable) {
        throw QueryCompilationErrors.nonFoldableArgumentError(
          procedure.name, param.name, param.dataType)
      }
    }
  }
}

object InvokeProcedures {
  private val suppressed: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)

  /**
   * Run `f` with CALL left unexecuted by analysis on this thread, including
   * CALL inside EXECUTE IMMEDIATE. For callers that analyze statements without
   * running them.
   */
  private[sql] def withoutInvocation[T](f: => T): T = {
    val previous = suppressed.get()
    suppressed.set(true)
    try f finally suppressed.set(previous)
  }
}
