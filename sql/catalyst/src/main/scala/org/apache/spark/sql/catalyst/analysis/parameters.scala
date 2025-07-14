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
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, CreateMap, CreateNamedStruct, Expression, LeafExpression, Literal, MapFromArrays, MapFromEntries, SubqueryExpression, Unevaluable, VariableReference}
import org.apache.spark.sql.catalyst.parser.{SubstituteParamsParser, SubstitutionRule}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterViewAs, CreateTable, CreateUserDefinedFunction, CreateVariable, CreateView, DefaultValueExpression, LogicalPlan, SupervisingCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, PARAMETER, PARAMETERIZED_QUERY, TreePattern, UNRESOLVED_WITH}
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
 * Finds all named parameters in `ParameterizedQuery` and substitutes them by literals or
 * by collection constructor functions such as `map()`, `array()`, `struct()`
 * from the user-specified arguments.
 */
object BindParameters extends Rule[LogicalPlan] with QueryErrorsBase {
  private def checkArgs(args: Iterable[(String, Expression)]): Unit = {
    def isNotAllowed(expr: Expression): Boolean = expr.exists {
      case _: Literal | _: CreateArray | _: CreateNamedStruct |
        _: CreateMap | _: MapFromArrays |  _: MapFromEntries | _: VariableReference => false
      case a: Alias => isNotAllowed(a.child)
      case _ => true
    }
    args.find(arg => isNotAllowed(arg._2)).foreach { case (name, expr) =>
      expr.failAnalysis(
        errorClass = "INVALID_SQL_ARG",
        messageParameters = Map("name" -> name))
    }
  }

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

  private def substituteSQL(p0: LogicalPlan)(f: PartialFunction[LogicalPlan, LogicalPlan]):
  LogicalPlan = {
    var stop = false
    p0.resolveOperatorsDownWithPruning(_.containsPattern(PARAMETER) && !stop) {
      case p1 =>
        stop = p1.isInstanceOf[ParameterizedQuery]
        p1.resolveOperatorsWithPruning(_.containsPattern(COMMAND)) (f)
    }
  }

  /**
   * Checks if a parameterized query is ready for binding (CTE resolved and args resolved).
   */
  private def isReadyForBinding(child: LogicalPlan, args: Seq[Expression]): Boolean = {
    !child.containsPattern(UNRESOLVED_WITH) && args.forall(_.resolved)
  }

  /**
   * Converts expressions to string values for SQL substitution.
   */
  private def expressionToSqlValue(expr: Expression): String = expr match {
    case lit: Literal => lit.sql
    case _ => expr.toString // fallback for non-literal expressions
  }

  /**
   * Performs SQL substitution for CreateVariable and CreateView using the provided
   * substitution function.
   */
  private def performSqlSubstitution(
      child: LogicalPlan,
      substitutionFn: (String, SubstitutionRule) => String): LogicalPlan = {
    child match {
      /**
       * DECLARE VARIABLE v INT DEFAULT :parm + 1;
       */
      case createVariable: CreateVariable =>
        val substitutedSQL = substitutionFn(createVariable.defaultExpr.originalSQL,
          SubstitutionRule.Expression)
        val newDefaultExpr = createVariable.defaultExpr.copy(originalSQL = substitutedSQL)
        createVariable.copy(defaultExpr = newDefaultExpr)

      /**
       * CREATE VIEW v(c1) AS SELECT :parm + 1 AS c1;
       */
      case createView: CreateView if createView.originalText.isDefined =>
      val substitutedSQL = substitutionFn(createView.originalText.get,
        SubstitutionRule.Query)
      createView.copy(originalText = Some(substitutedSQL))

      /**
       * ALTER VIEW v AS SELECT :parm + 1 AS c1;
       */
      case alterViewAs: AlterViewAs =>
        val substitutedSQL = substitutionFn(alterViewAs.originalText,
          SubstitutionRule.Query)
        alterViewAs.copy(originalText = substitutedSQL)

      /**
       * ALTER TABLE t ALTER COLUMN c1 SET DEFAULT :parm + 1;
       */
      case alterColumns: AlterColumns =>
        val specs = alterColumns.specs
        val substitutedSpecs = specs.map { spec =>
          spec.newDefaultExpression match {
            case Some(defExpr: DefaultValueExpression) =>
              val substitutedSQL = substitutionFn(defExpr.originalSQL, SubstitutionRule.Expression)
              spec.copy(newDefaultExpression = Some(defExpr.copy(originalSQL = substitutedSQL)))
            case _ => spec
          }
        }
        alterColumns.copy(specs = substitutedSpecs)

      /**
       * ALTER TABLE t ADD COLUMN c2  INT DEFAULT :parm + 1;
       */
      case addColumns: AddColumns =>
        val columnsToAdd = addColumns.columnsToAdd
        val substitutedColumnsToAdd = columnsToAdd.map { col =>
          col.default match {
            case Some(defExpr: DefaultValueExpression) =>
              val substitutedSQL = substitutionFn(defExpr.originalSQL, SubstitutionRule.Expression)
              col.copy(default = Some(defExpr.copy(originalSQL = substitutedSQL)))
            case _ => col
          }
        }
        addColumns.copy(columnsToAdd = substitutedColumnsToAdd)

      /**
       * CREATE FUNCTION foo(a INT DEFAULT :parm1) RETURN a + :parm2;
       * CREATE FUNCTION foo(a INT DEFAULT :parm1) RETURNS TABLE RETURN SELECT a + :parm2;
       */
      case createFunction: CreateUserDefinedFunction =>
          val inputParamText = createFunction.inputParamText map (p => substitutionFn(p,
            SubstitutionRule.ColDefinitionList))
          val exprText = createFunction.exprText map (p => substitutionFn(p,
            SubstitutionRule.Expression))
          val queryText = createFunction.queryText map (p => substitutionFn(p,
            SubstitutionRule.Query))
          createFunction.copy(
            inputParamText = inputParamText,
            exprText = exprText,
            queryText = queryText)

      /**
       * CREATE TABLE t (c1 INT DEFAULT :parm1);
       */
      case createTable: CreateTable =>
        val columns = createTable.columns
        val substitutedColumns = columns.map { col =>
          val substitutedDefaultValue = col.defaultValue match {
            case Some(defExpr: DefaultValueExpression) =>
              val substitutedSQL = substitutionFn(
                defExpr.originalSQL,
                SubstitutionRule.Expression
              )
              Some(defExpr.copy(originalSQL = substitutedSQL))
            case _ => col.defaultValue
          }
          val substitutedGenerationExpression = col.generationExpression match {
            case Some(genExpr: String) =>
              val substitutedSQL = substitutionFn(
                genExpr,
                SubstitutionRule.Expression
              )
              Some(substitutedSQL)
            case other => other
          }
          col.copy(defaultValue = substitutedDefaultValue,
            generationExpression = substitutedGenerationExpression)
        }
        createTable.copy(columns = substitutedColumns)

      case other => other
    }
  }

  /**
   * Handles named parameter binding including validation, SQL substitution, and parameter binding.
   */
  private def bindNamedParameters(
      child: LogicalPlan,
      argNames: Seq[String],
      argValues: Seq[Expression]): LogicalPlan = {
    if (argNames.length != argValues.length) {
      throw SparkException.internalError(s"The number of argument names ${argNames.length} " +
        s"must be equal to the number of argument values ${argValues.length}.")
    }

    val args = argNames.zip(argValues).toMap
    checkArgs(args)

    val namedValues = args.map { case (name, expr) =>
      (name, expressionToSqlValue(expr))
    }

    val newChild = performSqlSubstitution(child, { (sql, rule) =>
      val parser = new SubstituteParamsParser()
      parser.substitute(sql, rule, namedValues)
    })

    bind(newChild) { case NamedParameter(name) if args.contains(name) => args(name) }
  }

  /**
   * Handles positional parameter binding including validation, SQL substitution,
   * and parameter binding.
   */
  private def bindPositionalParameters(
      child: LogicalPlan,
      args: Seq[Expression]): LogicalPlan = {
    val indexedArgs = args.zipWithIndex
    checkArgs(indexedArgs.map(arg => (s"_${arg._2}", arg._1)))

    val positionalValues = args.map(expressionToSqlValue).toList

    val newChild = performSqlSubstitution(child, { (sql, rule) =>
      val parser = new SubstituteParamsParser()
      parser.substitute(sql, rule, positionalParams = positionalValues)
    })

    val positions = scala.collection.mutable.Set.empty[Int]
    bind(newChild) { case p @ PosParameter(pos) => positions.add(pos); p }
    val posToIndex = positions.toSeq.sorted.zipWithIndex.toMap

    bind(newChild) {
      case PosParameter(pos) if posToIndex.contains(pos) && args.size > posToIndex(pos) =>
        args(posToIndex(pos))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(PARAMETERIZED_QUERY)) {
      // We should wait for `CTESubstitution` to resolve CTE before binding parameters, as CTE
      // relations are not children of `UnresolvedWith`.
      case NameParameterizedQuery(child, argNames, argValues)
        if isReadyForBinding(child, argValues) =>
        bindNamedParameters(child, argNames, argValues)

      case PosParameterizedQuery(child, args)
        if isReadyForBinding(child, args) =>
        bindPositionalParameters(child, args)

      case other => other
    }
  }
}
