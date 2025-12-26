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

  /**
   * Checks if a multi-part name is qualified with a specific namespace.
   * Supports both 2-part (namespace.name) and 3-part (system.namespace.name) qualifications.
   *
   * @param nameParts The multi-part name to check
   * @param namespace The namespace to check for (e.g., "builtin", "session")
   * @return true if qualified with the given namespace
   */
  private def isQualifiedWithNamespace(nameParts: Seq[String], namespace: String): Boolean = {
    nameParts.length match {
      case 2 => nameParts.head.equalsIgnoreCase(namespace)
      case 3 =>
        nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(namespace)
      case _ => false
    }
  }

  /**
   * Check if a function name is qualified as a builtin function.
   * Valid forms: builtin.func or system.builtin.func
   */
  private def maybeBuiltinFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithNamespace(nameParts, CatalogManager.BUILTIN_NAMESPACE)
  }

  /**
   * Check if a function name is qualified as a session temporary function.
   * Valid forms: session.func or system.session.func
   */
  private def maybeTempFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithNamespace(nameParts, CatalogManager.SESSION_NAMESPACE)
  }

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
    } else if (maybeBuiltinFunctionName(name)) {
      // Explicitly qualified as builtin - lookup only builtin
      v1SessionCatalog.lookupBuiltinFunction(name.last)
    } else if (maybeTempFunctionName(name)) {
      // Explicitly qualified as temp - lookup only temp
      v1SessionCatalog.lookupTempFunction(name.last)
    } else if (name.size == 1) {
      // Unqualified - check temp first (shadowing), then builtin
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

    // Step 1: Try to resolve as scalar function
    val expression = if (name.size == 1 && u.isInternal) {
      Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
    } else if (maybeBuiltinFunctionName(name)) {
      // Explicitly qualified as builtin - resolve only builtin
      v1SessionCatalog.resolveBuiltinFunction(name.last, arguments)
    } else if (maybeTempFunctionName(name)) {
      // Explicitly qualified as temp - resolve only temp
      v1SessionCatalog.resolveTempFunction(name.last, arguments)
    } else if (name.size == 1) {
      // For unqualified names, check cross-type shadowing before resolving
      // If a temp table function exists with this name, it shadows any builtin scalar function
      val funcName = name.head
      if (v1SessionCatalog.lookupTempTableFunction(funcName).isDefined) {
        // Temp table function exists - throw error
        throw QueryCompilationErrors.notAScalarFunctionError(name.mkString("."), u)
      } else {
        // No temp table function - safe to resolve as scalar
        v1SessionCatalog.resolveBuiltinOrTempFunction(funcName, arguments)
      }
    } else {
      None
    }

    // Step 2: Check for table-only functions (cross-type error detection)
    // If not found as scalar, check if it exists as a table-only function
    if (expression.isEmpty && name.size == 1) {
      if (v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.head).isDefined) {
        throw QueryCompilationErrors.notAScalarFunctionError(name.mkString("."), u)
      }
    }

    expression.map { func =>
      validateFunction(func, arguments.length, u)
    }
  }

  def resolveBuiltinOrTempTableFunction(
      name: Seq[String],
      arguments: Seq[Expression]): Option[LogicalPlan] = {

    // Step 1: Try to resolve as table function
    val tableFunctionResult = if (name.length == 1) {
      // For unqualified names, check cross-type shadowing before resolving
      // If a temp scalar function exists with this name, it shadows any builtin table function
      val funcName = name.head
      if (v1SessionCatalog.lookupTempFunction(funcName).isDefined) {
        // Temp scalar function exists - will throw error below
        None
      } else {
        // No temp scalar function - safe to resolve as table function
        v1SessionCatalog.resolveBuiltinOrTempTableFunction(funcName, arguments)
      }
    } else {
      None
    }

    // Step 2: Fallback to scalar registry for type mismatch detection
    // Architecture: Generators are one-way (table-to-scalar extraction). If a function exists
    // ONLY as a scalar function and is used in table context, throw specific error.
    //
    // Note: This also handles cross-type shadowing. If a temp scalar function shadows a builtin
    // table function, the check above returns None, and we fall through here to detect
    // it's a scalar-only function and throw NOT_A_TABLE_FUNCTION.
    if (tableFunctionResult.isEmpty && name.length == 1) {
      if (v1SessionCatalog.lookupBuiltinOrTempFunction(name.head).isDefined) {
        throw QueryCompilationErrors.notATableFunctionError(name.mkString("."))
      }
    }

    tableFunctionResult
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
