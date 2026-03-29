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
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SupervisingCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, PARAMETER, PARAMETERIZED_QUERY, TreePattern, UNRESOLVED_WITH}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
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
 * The expression represents a named parameter that should be replaced by a literal or
 * collection constructor functions such as `map()`, `array()`, `struct()`.
 *
 * @param name The identifier of the parameter without the marker.
 */
case class NamedParameter(name: String) extends Parameter

/**
 * The expression represents a positional parameter that should be replaced by a literal or
 * by collection constructor functions such as `map()`, `array()`, `struct()`.
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
 * @param argNames Argument names.
 * @param argValues A sequence of argument values matched to argument names `argNames`.
 */
case class NameParameterizedQuery(
    child: LogicalPlan,
    argNames: Seq[String],
    argValues: Seq[Expression])
  extends ParameterizedQuery(child) {
  assert(argNames.nonEmpty && argValues.nonEmpty)
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

object NameParameterizedQuery {
  def apply(child: LogicalPlan, args: Map[String, Expression]): NameParameterizedQuery = {
    val argsSeq = args.toSeq
    new NameParameterizedQuery(child, argsSeq.map(_._1), argsSeq.map(_._2))
  }
}

/**
 * The logical plan representing a parameterized query with positional parameters.
 *
 * @param child The parameterized logical plan.
 * @param args The literal values or collection constructor functions such as `map()`,
 *             `array()`, `struct()` of positional parameters.
 */
case class PosParameterizedQuery(child: LogicalPlan, args: Seq[Expression])
  extends ParameterizedQuery(child) {
  assert(args.nonEmpty)
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

/**
 * The logical plan representing a parameterized query with general parameter support.
 * This allows the query to use either positional or named parameters based on the
 * parameter markers found in the query, with optional parameter names provided.
 *
 * @param child The parameterized logical plan.
 * @param args The literal values or collection constructor functions such as `map()`,
 *             `array()`, `struct()` of parameters.
 * @param paramNames Optional parameter names corresponding to args. If provided for an argument,
 *                   that argument can be used for named parameter binding. If not provided
 *                   parameters are treated as positional.
 */
case class GeneralParameterizedQuery(
    child: LogicalPlan,
    args: Seq[Expression],
    paramNames: Seq[String])
  extends ParameterizedQuery(child) {
  assert(args.nonEmpty)
  assert(paramNames.length == args.length,
    s"paramNames must be same length as args. " +
    s"paramNames.length=${paramNames.length}, args.length=${args.length}")
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

/**
 * Moves `ParameterizedQuery` inside `SupervisingCommand` for their supervised plans to be
 * resolved later by the analyzer.
 *
 * - Basic case:
 * `PosParameterizedQuery(ExplainCommand(SomeQuery(...)))` =>
 * `ExplainCommand(PosParameterizedQuery(SomeQuery(...)))`
 * - Nested `SupervisedCommand`s are handled recursively:
 * `PosParameterizedQuery(ExplainCommand(ExplainCommand(SomeQuery(...))))` =>
 * `ExplainCommand(ExplainCommand(PosParameterizedQuery(SomeQuery(...))))`
 */
object MoveParameterizedQueriesDown extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(PARAMETERIZED_QUERY)) {
      case pq: ParameterizedQuery if pq.exists(isSupervisingCommand) =>
        moveParameterizedQueryIntoSupervisingCommand(pq)
    }
  }

  private def moveParameterizedQueryIntoSupervisingCommand(pq: ParameterizedQuery): LogicalPlan = {
    // Moves parameterized query down recursively to handle nested `SupervisingCommand`s
    def transformSupervisedPlan: PartialFunction[LogicalPlan, LogicalPlan] = {
      case command: SupervisingCommand =>
        command.withTransformedSupervisedPlan {
          transformSupervisedPlan(_)
        }
      case plan => pq.withNewChildren(Seq(plan))
    }

    pq.child.resolveOperatorsWithPruning(_.containsPattern(COMMAND)) {
      case command: SupervisingCommand => transformSupervisedPlan(command)
    }
  }

  private def isSupervisingCommand(plan: LogicalPlan): Boolean =
    plan.containsPattern(COMMAND) && plan.isInstanceOf[SupervisingCommand]
}

/**
 * Binds all named parameters in `ParameterizedQuery` and substitutes them by literals or
 * by collection constructor functions such as `map()`, `array()`, `struct()`
 * from the user-specified arguments.
 */
object BindParameters extends Rule[LogicalPlan] with QueryErrorsBase {

  private def bind(p0: LogicalPlan)(f: PartialFunction[Expression, Expression]): LogicalPlan = {
    var stop = false
    p0.resolveOperatorsDownWithPruning(_.containsPattern(PARAMETER) && !stop) {
      case p1 =>
        stop = p1.isInstanceOf[ParameterizedQuery]
        p1.transformExpressionsWithPruning(_.containsPattern(PARAMETER)) (f orElse {
          case sub: SubqueryExpression => sub.withNewPlan(bind(sub.plan)(f))
        })
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(PARAMETERIZED_QUERY)) {
      // We should wait for `CTESubstitution` to resolve CTE before binding parameters, as CTE
      // relations are not children of `UnresolvedWith`.
      case NameParameterizedQuery(child, argNames, argValues)
        if !child.containsPattern(UNRESOLVED_WITH) &&
          argValues.forall(_.resolved) =>
        if (argNames.length != argValues.length) {
          throw SparkException.internalError(s"The number of argument names ${argNames.length} " +
            s"must be equal to the number of argument values ${argValues.length}.")
        }
        val args = argNames.zip(argValues).toMap
        ParameterizedQueryArgumentsValidator(args)
        bind(child) { case NamedParameter(name) if args.contains(name) => args(name) }

      case PosParameterizedQuery(child, args)
        if !child.containsPattern(UNRESOLVED_WITH) &&
          args.forall(_.resolved) =>

        val indexedArgs = args.zipWithIndex
        ParameterizedQueryArgumentsValidator(indexedArgs.map(arg => (s"_${arg._2}", arg._1)))

        val positions = scala.collection.mutable.Set.empty[Int]
        bind(child) { case p @ PosParameter(pos) => positions.add(pos); p }
        val posToIndex = positions.toSeq.sorted.zipWithIndex.toMap

        bind(child) {
          case PosParameter(pos) if posToIndex.contains(pos) && args.size > posToIndex(pos) =>
            args(posToIndex(pos))
        }

      case GeneralParameterizedQuery(child, args, paramNames)
        if !child.containsPattern(UNRESOLVED_WITH) &&
          args.forall(_.resolved) =>

        // Check all arguments for validity (args are already evaluated expressions/literals)
        val allArgs = args.zip(paramNames).zipWithIndex.map { case ((arg, name), index) =>
          val finalName = if (name.isEmpty) s"_$index" else name
          finalName -> arg
        }
        ParameterizedQueryArgumentsValidator(allArgs)

        // Collect parameter types used in the query to enforce invariants
        var hasNamedParam = false
        val positionalParams = scala.collection.mutable.Set.empty[Int]
        bind(child) {
          case p @ NamedParameter(_) => hasNamedParam = true; p
          case p @ PosParameter(pos) => positionalParams.add(pos); p
        }

        // Validate: no mixing of positional and named parameters
        if (hasNamedParam && positionalParams.nonEmpty) {
          throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
        }

        // Validate: if query uses named parameters, all USING expressions must have names
        if (hasNamedParam && positionalParams.isEmpty) {
          if (paramNames.isEmpty) {
            // Query uses named parameters but no USING expressions provided
            throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(Seq.empty)
          } else {
            // Check that all USING expressions have names
            val unnamedExpressions = paramNames.zipWithIndex.collect {
              case ("", index) => index // empty strings are unnamed
            }
            if (unnamedExpressions.nonEmpty) {
              val unnamedExprs = unnamedExpressions.map(args(_))
              throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(unnamedExprs)
            }
          }
        }

        // Now we can do simple binding based on which type we determined
        if (hasNamedParam) {
          // Named parameter binding - paramNames guaranteed to have no nulls at this point
          val namedArgsMap = paramNames.zip(args).toMap
          bind(child) {
            case NamedParameter(name) => namedArgsMap.getOrElse(name, NamedParameter(name))
          }
        } else {
          // Positional parameter binding (same logic as PosParameterizedQuery)
          val posToIndex = positionalParams.toSeq.sorted.zipWithIndex.toMap
          bind(child) {
            case PosParameter(pos) if posToIndex.contains(pos) && args.size > posToIndex(pos) =>
              args(posToIndex(pos))
          }
        }

      case other => other
    }
  }
}
