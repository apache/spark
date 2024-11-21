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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  CatalogV2Util,
  FunctionCatalog,
  Identifier,
  LookupCatalog
}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.functions.{
  AggregateFunction => V2AggregateFunction,
  ScalarFunction
}
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.types._

class FunctionResolution(
    override val catalogManager: CatalogManager,
    relationResolution: RelationResolution)
    extends DataTypeErrorsBase with LookupCatalog {
  private val v1SessionCatalog = catalogManager.v1SessionCatalog

  private val trimWarningEnabled = new AtomicBoolean(true)

  def resolveFunction(u: UnresolvedFunction): Expression = {
    withPosition(u) {
      resolveBuiltinOrTempFunction(u.nameParts, u.arguments, u).getOrElse {
        val CatalogAndIdentifier(catalog, ident) =
          relationResolution.expandIdentifier(u.nameParts)
        if (CatalogV2Util.isSessionCatalog(catalog)) {
          resolveV1Function(ident.asFunctionIdentifier, u.arguments, u)
        } else {
          resolveV2Function(catalog.asFunctionCatalog, ident, u.arguments, u)
        }
      }
    }
  }

  /**
   * Check if the arguments of a function are either resolved or a lambda function.
   */
  def hasLambdaAndResolvedArguments(expressions: Seq[Expression]): Boolean = {
    val (lambdas, others) = expressions.partition(_.isInstanceOf[LambdaFunction])
    lambdas.nonEmpty && others.forall(_.resolved)
  }

  def lookupBuiltinOrTempFunction(
      name: Seq[String],
      u: Option[UnresolvedFunction]): Option[ExpressionInfo] = {
    if (name.size == 1 && u.exists(_.isInternal)) {
      FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head))
    } else if (name.size == 1) {
      v1SessionCatalog.lookupBuiltinOrTempFunction(name.head)
    } else {
      None
    }
  }

  def lookupBuiltinOrTempTableFunction(name: Seq[String]): Option[ExpressionInfo] = {
    if (name.length == 1) {
      v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.head)
    } else {
      None
    }
  }

  def resolveBuiltinOrTempFunction(
      name: Seq[String],
      arguments: Seq[Expression],
      u: UnresolvedFunction): Option[Expression] = {
    val expression = if (name.size == 1 && u.isInternal) {
      Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
    } else if (name.size == 1) {
      v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
    } else {
      None
    }
    expression.map { func =>
      validateFunction(func, arguments.length, u)
    }
  }

  def resolveBuiltinOrTempTableFunction(
      name: Seq[String],
      arguments: Seq[Expression]): Option[LogicalPlan] = {
    if (name.length == 1) {
      v1SessionCatalog.resolveBuiltinOrTempTableFunction(name.head, arguments)
    } else {
      None
    }
  }

  private def resolveV1Function(
      ident: FunctionIdentifier,
      arguments: Seq[Expression],
      u: UnresolvedFunction): Expression = {
    val func = v1SessionCatalog.resolvePersistentFunction(ident, arguments)
    validateFunction(func, arguments.length, u)
  }

  private def validateFunction(
      func: Expression,
      numArgs: Int,
      u: UnresolvedFunction): Expression = {
    func match {
      case owg: SupportsOrderingWithinGroup if !owg.isDistinctSupported && u.isDistinct =>
        throw QueryCompilationErrors.distinctWithOrderingFunctionUnsupportedError(owg.prettyName)
      case owg: SupportsOrderingWithinGroup
          if owg.isOrderingMandatory && !owg.orderingFilled && u.orderingWithinGroup.isEmpty =>
        throw QueryCompilationErrors.functionMissingWithinGroupError(owg.prettyName)
      case owg: SupportsOrderingWithinGroup
          if owg.orderingFilled && u.orderingWithinGroup.nonEmpty =>
        // e.g mode(expr1) within group (order by expr2) is not supported
        throw QueryCompilationErrors.wrongNumOrderingsForFunctionError(
          owg.prettyName,
          0,
          u.orderingWithinGroup.length
        )
      case f if !f.isInstanceOf[SupportsOrderingWithinGroup] && u.orderingWithinGroup.nonEmpty =>
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          func.prettyName,
          "WITHIN GROUP (ORDER BY ...)"
        )
      // AggregateWindowFunctions are AggregateFunctions that can only be evaluated within
      // the context of a Window clause. They do not need to be wrapped in an
      // AggregateExpression.
      case wf: AggregateWindowFunction =>
        if (u.isDistinct) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(wf.prettyName, "DISTINCT")
        } else if (u.filter.isDefined) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            wf.prettyName,
            "FILTER clause"
          )
        } else if (u.ignoreNulls) {
          wf match {
            case nthValue: NthValue =>
              nthValue.copy(ignoreNulls = u.ignoreNulls)
            case _ =>
              throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
                wf.prettyName,
                "IGNORE NULLS"
              )
          }
        } else {
          wf
        }
      case owf: FrameLessOffsetWindowFunction =>
        if (u.isDistinct) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            owf.prettyName,
            "DISTINCT"
          )
        } else if (u.filter.isDefined) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            owf.prettyName,
            "FILTER clause"
          )
        } else if (u.ignoreNulls) {
          owf match {
            case lead: Lead =>
              lead.copy(ignoreNulls = u.ignoreNulls)
            case lag: Lag =>
              lag.copy(ignoreNulls = u.ignoreNulls)
          }
        } else {
          owf
        }
      // We get an aggregate function, we need to wrap it in an AggregateExpression.
      case agg: AggregateFunction =>
        // Note: PythonUDAF does not support these advanced clauses.
        if (agg.isInstanceOf[PythonUDAF]) checkUnsupportedAggregateClause(agg, u)
        // After parse, the functions not set the ordering within group yet.
        val newAgg = agg match {
          case owg: SupportsOrderingWithinGroup
              if !owg.orderingFilled && u.orderingWithinGroup.nonEmpty =>
            owg.withOrderingWithinGroup(u.orderingWithinGroup)
          case _ =>
            agg
        }

        u.filter match {
          case Some(filter) if !filter.deterministic =>
            throw QueryCompilationErrors.nonDeterministicFilterInAggregateError(filterExpr = filter)
          case Some(filter) if filter.dataType != BooleanType =>
            throw QueryCompilationErrors.nonBooleanFilterInAggregateError(filterExpr = filter)
          case Some(filter) if filter.exists(_.isInstanceOf[AggregateExpression]) =>
            throw QueryCompilationErrors.aggregateInAggregateFilterError(
              filterExpr = filter,
              aggExpr = filter.find(_.isInstanceOf[AggregateExpression]).get
            )
          case Some(filter) if filter.exists(_.isInstanceOf[WindowExpression]) =>
            throw QueryCompilationErrors.windowFunctionInAggregateFilterError(
              filterExpr = filter,
              windowExpr = filter.find(_.isInstanceOf[WindowExpression]).get
            )
          case _ =>
        }
        if (u.ignoreNulls) {
          val aggFunc = newAgg match {
            case first: First => first.copy(ignoreNulls = u.ignoreNulls)
            case last: Last => last.copy(ignoreNulls = u.ignoreNulls)
            case any_value: AnyValue => any_value.copy(ignoreNulls = u.ignoreNulls)
            case _ =>
              throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
                newAgg.prettyName,
                "IGNORE NULLS"
              )
          }
          aggFunc.toAggregateExpression(u.isDistinct, u.filter)
        } else {
          newAgg.toAggregateExpression(u.isDistinct, u.filter)
        }
      // This function is not an aggregate function, just return the resolved one.
      case other =>
        checkUnsupportedAggregateClause(other, u)
        if (other.isInstanceOf[String2TrimExpression] && numArgs == 2) {
          if (trimWarningEnabled.get) {
            log.warn(
              "Two-parameter TRIM/LTRIM/RTRIM function signatures are deprecated." +
              " Use SQL syntax `TRIM((BOTH | LEADING | TRAILING)? trimStr FROM str)`" +
              " instead."
            )
            trimWarningEnabled.set(false)
          }
        }
        other
    }
  }

  private def checkUnsupportedAggregateClause(func: Expression, u: UnresolvedFunction): Unit = {
    if (u.isDistinct) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(func.prettyName, "DISTINCT")
    }
    if (u.filter.isDefined) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        func.prettyName,
        "FILTER clause"
      )
    }
    if (u.ignoreNulls) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        func.prettyName,
        "IGNORE NULLS"
      )
    }
  }

  private def resolveV2Function(
      catalog: FunctionCatalog,
      ident: Identifier,
      arguments: Seq[Expression],
      u: UnresolvedFunction): Expression = {
    val unbound = catalog.loadFunction(ident)
    val inputType = StructType(arguments.zipWithIndex.map {
      case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
    })
    val bound = try {
      unbound.bind(inputType)
    } catch {
      case unsupported: UnsupportedOperationException =>
        throw QueryCompilationErrors.functionCannotProcessInputError(
          unbound,
          arguments,
          unsupported
        )
    }

    if (bound.inputTypes().length != arguments.length) {
      throw QueryCompilationErrors.v2FunctionInvalidInputTypeLengthError(bound, arguments)
    }

    bound match {
      case scalarFunc: ScalarFunction[_] =>
        processV2ScalarFunction(scalarFunc, arguments, u)
      case aggFunc: V2AggregateFunction[_, _] =>
        processV2AggregateFunction(aggFunc, arguments, u)
      case _ =>
        failAnalysis(
          errorClass = "INVALID_UDF_IMPLEMENTATION",
          messageParameters = Map("funcName" -> toSQLId(bound.name()))
        )
    }
  }

  private def processV2ScalarFunction(
      scalarFunc: ScalarFunction[_],
      arguments: Seq[Expression],
      u: UnresolvedFunction): Expression = {
    if (u.isDistinct) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(scalarFunc.name(), "DISTINCT")
    } else if (u.filter.isDefined) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        scalarFunc.name(),
        "FILTER clause"
      )
    } else if (u.ignoreNulls) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        scalarFunc.name(),
        "IGNORE NULLS"
      )
    } else {
      V2ExpressionUtils.resolveScalarFunction(scalarFunc, arguments)
    }
  }

  private def processV2AggregateFunction(
      aggFunc: V2AggregateFunction[_, _],
      arguments: Seq[Expression],
      u: UnresolvedFunction): Expression = {
    if (u.ignoreNulls) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        aggFunc.name(),
        "IGNORE NULLS"
      )
    }
    val aggregator = V2Aggregator(aggFunc, arguments)
    aggregator.toAggregateExpression(u.isDistinct, u.filter)
  }

  private def failAnalysis(errorClass: String, messageParameters: Map[String, String]): Nothing = {
    throw new AnalysisException(
      errorClass = errorClass,
      messageParameters = messageParameters)
  }
}
