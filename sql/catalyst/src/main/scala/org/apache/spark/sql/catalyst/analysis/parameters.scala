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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Literal, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARAMETER, PARAMETERIZED_QUERY, TreePattern, UNRESOLVED_WITH}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.DataType

sealed trait Parameter extends LeafExpression with Unevaluable {
  override lazy val resolved: Boolean = false

  private def unboundError(methodName: String): Nothing = {
    throw SparkException.internalError(
      s"Cannot call `$methodName()` of the unbound parameter `$name`.")
  }
  override def dataType: DataType = unboundError("dataType")
  override def nullable: Boolean = unboundError("nullable")

  final override val nodePatterns: Seq[TreePattern] = Seq(PARAMETER)

  def name: String
}

/**
 * The expression represents a named parameter that should be replaced by a literal.
 *
 * @param name The identifier of the parameter without the marker.
 */
case class NamedParameter(name: String) extends Parameter

/**
 * The expression represents a positional parameter that should be replaced by a literal.
 *
 * @param pos An unique position of the parameter in a SQL query text.
 */
case class PosParameter(pos: Int) extends Parameter {
  override def name: String = s"_$pos"
}

/**
 * The logical plan representing a parameterized query. It will be removed during analysis after
 * the parameters are bind.
 */
abstract class ParameterizedQuery(child: LogicalPlan) extends UnresolvedUnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(PARAMETERIZED_QUERY)
}

/**
 * The logical plan representing a parameterized query with named parameters.
 *
 * @param child The parameterized logical plan.
 * @param args The map of parameter names to its literal values.
 */
case class NameParameterizedQuery(child: LogicalPlan, args: Map[String, Expression])
  extends ParameterizedQuery(child) {
  assert(args.nonEmpty)
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

/**
 * The logical plan representing a parameterized query with positional parameters.
 *
 * @param child The parameterized logical plan.
 * @param args The literal values of positional parameters.
 */
case class PosParameterizedQuery(child: LogicalPlan, args: Array[Expression])
  extends ParameterizedQuery(child) {
  assert(args.nonEmpty)
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

/**
 * Finds all named parameters in `ParameterizedQuery` and substitutes them by literals from the
 * user-specified arguments.
 */
object BindParameters extends Rule[LogicalPlan] with QueryErrorsBase {
  private def checkArgs(args: Iterable[(String, Expression)]): Unit = {
    args.find(!_._2.isInstanceOf[Literal]).foreach { case (name, expr) =>
      expr.failAnalysis(
        errorClass = "INVALID_SQL_ARG",
        messageParameters = Map("name" -> name))
    }
  }

  private def bind(p: LogicalPlan)(f: PartialFunction[Expression, Expression]): LogicalPlan = {
    p.resolveExpressionsWithPruning(_.containsPattern(PARAMETER)) (f orElse {
      case sub: SubqueryExpression => sub.withNewPlan(bind(sub.plan)(f))
    })
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsPattern(PARAMETERIZED_QUERY)) {
      // One unresolved plan can have at most one ParameterizedQuery.
      val parameterizedQueries = plan.collect { case p: ParameterizedQuery => p }
      assert(parameterizedQueries.length == 1)
    }

    plan.resolveOperatorsWithPruning(_.containsPattern(PARAMETERIZED_QUERY)) {
      // We should wait for `CTESubstitution` to resolve CTE before binding parameters, as CTE
      // relations are not children of `UnresolvedWith`.
      case p @ NameParameterizedQuery(child, args) if !child.containsPattern(UNRESOLVED_WITH) =>
        checkArgs(args)
        bind(child) { case NamedParameter(name) if args.contains(name) => args(name) }

      case p @ PosParameterizedQuery(child, args) if !child.containsPattern(UNRESOLVED_WITH) =>
        val indexedArgs = args.zipWithIndex
        checkArgs(indexedArgs.map(arg => (s"_${arg._2}", arg._1)))

        val positions = scala.collection.mutable.Set.empty[Int]
        bind(child) { case p @ PosParameter(pos) => positions.add(pos); p }
        val posToIndex = positions.toSeq.sorted.zipWithIndex.toMap

        bind(child) {
          case PosParameter(pos) if posToIndex.contains(pos) && args.size > posToIndex(pos) =>
            args(posToIndex(pos))
        }

      case other => other
    }
  }
}
