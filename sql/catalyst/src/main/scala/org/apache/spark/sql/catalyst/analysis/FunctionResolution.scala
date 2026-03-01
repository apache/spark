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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  CatalogNotFoundException,
  LookupCatalog
}

/**
 * Represents the type/location of a function.
 */
sealed trait FunctionType
object FunctionType {
  /** Function is a built-in function in the builtin registry. */
  case object Builtin extends FunctionType
  /** Function is a temporary function in the session registry. */
  case object Temporary extends FunctionType
  /** Function is a persistent function in the external catalog. */
  case object Persistent extends FunctionType
  /** Function exists only as a table function (cannot be used in scalar context). */
  case object TableOnly extends FunctionType
  /** Function does not exist anywhere. */
  case object NotFound extends FunctionType
}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.functions.{
  AggregateFunction => V2AggregateFunction,
  ScalarFunction,
  UnboundFunction
}
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types._

class FunctionResolution(
    override val catalogManager: CatalogManager,
    relationResolution: RelationResolution)
    extends DataTypeErrorsBase with LookupCatalog with Logging {
  private val v1SessionCatalog = catalogManager.v1SessionCatalog

  private val trimWarningEnabled = new AtomicBoolean(true)

  /**
   * A single place to look for a scalar function. Fully qualified => one candidate;
   * partially qualified => typically two (system + current catalog); unqualified => three
   * (e.g. builtin, session, persistent in path order). Builtin and session are just two
   * lookups in the path; they may share the same registry internally.
   */
  private sealed trait ScalarCandidate
  private case class SessionNamespaceCandidate(
      kind: SessionCatalog.SessionFunctionKind,
      funcName: String) extends ScalarCandidate
  private case class PersistentCandidate(nameParts: Seq[String]) extends ScalarCandidate
  private case class InternalCandidate(funcName: String) extends ScalarCandidate

  /** Produces the ordered list of candidates for scalar resolution. */
  private def scalarResolutionCandidates(
      nameParts: Seq[String],
      unresolvedFunc: UnresolvedFunction): Seq[ScalarCandidate] = {
    val resolutionOrder = SQLConf.get.sessionFunctionResolutionOrder
    if (nameParts.size == 1 && unresolvedFunc.isInternal) {
      InternalCandidate(nameParts.head) +: unqualifiedScalarCandidates(nameParts, resolutionOrder)
    } else {
      nameParts.size match {
        case 1 =>
          unqualifiedScalarCandidates(nameParts, resolutionOrder)
        case 2 if FunctionResolution.sessionNamespaceKind(nameParts).isDefined =>
          // Partially qualified builtin/session: try persistent first so user schema wins
          PersistentCandidate(nameParts) +: sessionNamespaceToCandidate(nameParts).toSeq
        case 3 if FunctionResolution.sessionNamespaceKind(nameParts).isDefined =>
          // Fully qualified system.builtin.func or system.session.func => single candidate
          sessionNamespaceToCandidate(nameParts).toSeq
        case _ =>
          Seq(PersistentCandidate(nameParts))
      }
    }
  }

  private def sessionNamespaceToCandidate(
      nameParts: Seq[String]): Option[SessionNamespaceCandidate] =
    FunctionResolution.sessionNamespaceKind(nameParts)
      .map(k => SessionNamespaceCandidate(k, nameParts.last))

  private def unqualifiedSessionCandidates(
      nameParts: Seq[String],
      resolutionOrder: String): Seq[SessionNamespaceCandidate] = {
    val funcName = nameParts.head
    resolutionOrder match {
      case "first" =>
        Seq(
          SessionNamespaceCandidate(SessionCatalog.Temp, funcName),
          SessionNamespaceCandidate(SessionCatalog.Builtin, funcName))
      case "last" =>
        Seq(
          SessionNamespaceCandidate(SessionCatalog.Builtin, funcName),
          SessionNamespaceCandidate(SessionCatalog.Temp, funcName))
      case _ =>
        Seq(
          SessionNamespaceCandidate(SessionCatalog.Builtin, funcName),
          SessionNamespaceCandidate(SessionCatalog.Temp, funcName))
    }
  }

  /** For unqualified names, full candidate list in resolution order (may interleave persistent). */
  private def unqualifiedScalarCandidates(
      nameParts: Seq[String],
      resolutionOrder: String): Seq[ScalarCandidate] = {
    val sessionCandidates = unqualifiedSessionCandidates(nameParts, resolutionOrder)
    if (resolutionOrder == "last") {
      Seq(sessionCandidates.head, PersistentCandidate(nameParts), sessionCandidates(1))
    } else {
      sessionCandidates :+ PersistentCandidate(nameParts)
    }
  }

  private def tryResolveScalarCandidate(
      c: ScalarCandidate,
      unresolvedFunc: UnresolvedFunction): Option[Expression] = c match {
    case InternalCandidate(funcName) =>
      Option(FunctionRegistry.internal.lookupFunction(
        FunctionIdentifier(funcName), unresolvedFunc.arguments))
    case SessionNamespaceCandidate(kind, funcName) =>
      val expr = v1SessionCatalog.resolveScalarFunction(kind, funcName, unresolvedFunc.arguments)
      if (expr.isEmpty) {
        if (v1SessionCatalog.lookupFunctionInfo(
            SessionCatalog.Temp, funcName, tableFunction = true).isDefined) {
          throw QueryCompilationErrors.notAScalarFunctionError(funcName, unresolvedFunc)
        }
        if (v1SessionCatalog.lookupBuiltinOrTempTableFunction(funcName).isDefined) {
          throw QueryCompilationErrors.notAScalarFunctionError(funcName, unresolvedFunc)
        }
      }
      expr.map(e => validateFunction(e, unresolvedFunc.arguments.length, unresolvedFunc))
    case PersistentCandidate(parts) =>
      try {
        val CatalogAndIdentifier(catalog, ident) =
          relationResolution.expandIdentifier(parts)
        val loaded = catalog.asFunctionCatalog.loadFunction(ident)
        Some(loaded match {
        case v1Func: V1Function =>
          val func = v1Func.invoke(unresolvedFunc.arguments)
          validateFunction(func, unresolvedFunc.arguments.length, unresolvedFunc)
        case unboundV2Func =>
          resolveV2Function(unboundV2Func, unresolvedFunc.arguments, unresolvedFunc)
        })
      } catch {
        case e: AnalysisException if e.getCondition == "REQUIRES_SINGLE_PART_NAMESPACE" &&
            parts.size == 3 =>
          val catalogPath = (
            catalogManager.currentCatalog.name +: catalogManager.currentNamespace
          ).mkString(".")
          val searchPath = SQLConf.get.functionResolutionSearchPath(catalogPath)
          throw QueryCompilationErrors.unresolvedRoutineError(
            unresolvedFunc.nameParts, searchPath, unresolvedFunc.origin)
        case _: NoSuchFunctionException =>
          None
        case _: NoSuchNamespaceException =>
          None
        case _: CatalogNotFoundException =>
          None
        case e: AnalysisException
            if e.getCondition == "FORBIDDEN_OPERATION" =>
          None
        case e: AnalysisException =>
          throw e
        case NonFatal(e) =>
          logWarning(s"Persistent lookup failed for ${parts.mkString(".")}", e)
          None
      }
  }

  def resolveFunction(unresolvedFunc: UnresolvedFunction): Expression = {
    withPosition(unresolvedFunc) {
      val candidates = scalarResolutionCandidates(unresolvedFunc.nameParts, unresolvedFunc)
      for (c <- candidates) {
        tryResolveScalarCandidate(c, unresolvedFunc) match {
          case Some(expr) => return expr
          case None =>
        }
      }
      val catalogPath = (
        catalogManager.currentCatalog.name +: catalogManager.currentNamespace
      ).mkString(".")
      val searchPath = SQLConf.get.functionResolutionSearchPath(catalogPath)
      throw QueryCompilationErrors.unresolvedRoutineError(
        unresolvedFunc.nameParts, searchPath, unresolvedFunc.origin)
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
      unresolvedFunc: Option[UnresolvedFunction]): Option[ExpressionInfo] = {
    if (name.size == 1 && unresolvedFunc.exists(_.isInternal)) {
      FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head))
    } else {
      FunctionResolution.sessionNamespaceKind(name) match {
        case Some(kind) =>
          v1SessionCatalog.lookupFunctionInfo(kind, name.last, tableFunction = false)
        case None =>
          if (name.size == 1) {
            v1SessionCatalog.lookupBuiltinOrTempFunction(name.head)
          } else {
            None
          }
      }
    }
  }

  def lookupBuiltinOrTempTableFunction(name: Seq[String]): Option[ExpressionInfo] = {
    FunctionResolution.sessionNamespaceKind(name) match {
      case Some(kind) =>
        v1SessionCatalog.lookupFunctionInfo(kind, name.last, tableFunction = true)
      case None =>
        if (name.length == 1) {
          v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.head)
        } else {
          None
        }
    }
  }

  /**
   * Checks if a function is a builtin or temporary function (scalar or table).
   * This is a convenience method that uses lookupFunctionType internally.
   *
   * @param nameParts The function name parts.
   * @param unresolvedFunc Optional UnresolvedFunction for internal function detection.
   * @return true if the function is a builtin or temporary function, false otherwise.
   */
  def isBuiltinOrTemporaryFunction(
      nameParts: Seq[String],
      unresolvedFunc: Option[UnresolvedFunction]): Boolean = {
    lookupFunctionType(nameParts, unresolvedFunc) match {
      case FunctionType.Builtin | FunctionType.Temporary | FunctionType.TableOnly => true
      case _ => false
    }
  }

  /**
   * Determines the type/location of a function (builtin, temporary, persistent, etc.).
   * This is used by the LookupFunctions analyzer rule for early validation and optimization.
   * This method only performs the lookup and classification - it does not throw errors.
   *
   * @param nameParts The function name parts.
   * @param unresolvedFunc Optional UnresolvedFunction node for lookups that may need it.
   * @return The type of the function (Builtin, Temporary, Persistent, TableOnly, or NotFound).
   */
  def lookupFunctionType(
      nameParts: Seq[String],
      unresolvedFunc: Option[UnresolvedFunction] = None): FunctionType = {

    // Check if it's explicitly qualified as extension, builtin, or temp
    FunctionResolution.sessionNamespaceKind(nameParts) match {
      case Some(SessionCatalog.Extension) | Some(SessionCatalog.Builtin) =>
        if (lookupBuiltinOrTempFunction(nameParts, unresolvedFunc).isDefined) {
          return FunctionType.Builtin  // Extension and builtin both as Builtin
        }
      case Some(SessionCatalog.Temp) =>
        if (lookupBuiltinOrTempFunction(nameParts, unresolvedFunc).isDefined) {
          return FunctionType.Temporary
        }
      case None =>
        // Unqualified or qualified with a catalog
        // Use lookupBuiltinOrTempFunction which handles internal functions correctly
        val funcInfoOpt = lookupBuiltinOrTempFunction(nameParts, unresolvedFunc)
        funcInfoOpt match {
          case Some(info) =>
            // Determine if it's extension, temp, or builtin from the ExpressionInfo
            if (info.getDb == CatalogManager.EXTENSION_NAMESPACE) {
              return FunctionType.Builtin
            } else if (info.getDb == CatalogManager.SESSION_NAMESPACE) {
              if (nameParts.size == 1 && unresolvedFunc.exists(_.isInternal)) {
                return FunctionType.Builtin
              } else {
                return FunctionType.Temporary
              }
            } else {
              return FunctionType.Builtin
            }
          case None =>
        }
    }

    // Check if function exists as table function only
    if (lookupBuiltinOrTempTableFunction(nameParts).isDefined) {
      return FunctionType.TableOnly
    }

    // Check external catalog for persistent functions
    val CatalogAndIdentifier(catalog, ident) = relationResolution.expandIdentifier(nameParts)
    if (catalog.asFunctionCatalog.functionExists(ident)) {
      return FunctionType.Persistent
    }

    // Function doesn't exist anywhere
    FunctionType.NotFound
  }

  /**
   * Tries to resolve a function from the session catalog only (builtin, temp, or extension).
   * Returns None if the name is not session-qualified or not found there. The caller is
   * responsible for then trying the persistent catalog when this returns None.
   */
  def tryResolveFromSessionCatalog(
      name: Seq[String],
      arguments: Seq[Expression],
      unresolvedFunc: UnresolvedFunction): Option[Expression] = {

    // Step 1: Try to resolve as scalar function
    val expression = if (name.size == 1 && unresolvedFunc.isInternal) {
      Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
    } else {
      FunctionResolution.sessionNamespaceKind(name) match {
        case Some(kind) =>
          v1SessionCatalog.resolveScalarFunction(kind, name.last, arguments)
        case None =>
          if (name.size == 1) {
            val funcName = name.head
            val scalarResult = v1SessionCatalog.resolveBuiltinOrTempFunction(funcName, arguments)
            if (scalarResult.isEmpty &&
                v1SessionCatalog.lookupFunctionInfo(
                  SessionCatalog.Temp, funcName, tableFunction = true).isDefined) {
              throw QueryCompilationErrors.notAScalarFunctionError(
                name.mkString("."), unresolvedFunc)
            } else {
              scalarResult
            }
          } else {
            None
          }
      }
    }

    // Step 2: Check for table-only functions (cross-type error detection)
    // If not found as scalar, check if it exists as a table-only function
    if (expression.isEmpty && name.size == 1) {
      if (v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.head).isDefined) {
        throw QueryCompilationErrors.notAScalarFunctionError(name.mkString("."), unresolvedFunc)
      }
    }

    expression.map { func =>
      validateFunction(func, arguments.length, unresolvedFunc)
    }
  }

  /**
   * Tries to resolve a table function from the session catalog only (builtin, temp, or extension).
   * Returns None if the name is not session-qualified or not found there. The caller may
   * then try the persistent catalog or report an error.
   */
  def tryResolveTableFunctionFromSessionCatalog(
      name: Seq[String],
      arguments: Seq[Expression]): Option[LogicalPlan] = {

    // Step 1: Try to resolve as table function
    val tableFunctionResult = FunctionResolution.sessionNamespaceKind(name) match {
      case Some(kind) =>
        v1SessionCatalog.resolveTableFunction(kind, name.last, arguments)
      case None =>
        if (name.length == 1) {
          val funcName = name.head
          v1SessionCatalog
            .resolveBuiltinOrTempTableFunctionRespectingPathOrder(funcName, arguments) match {
            case Some(scala.util.Left(plan)) => Some(plan)
            case Some(scala.util.Right(())) =>
              throw QueryCompilationErrors.notATableFunctionError(name.mkString("."))
            case None => None
          }
        } else {
          None
        }
    }

    // Step 2: If no table function was found in path, check if a scalar exists (wrong type).
    if (tableFunctionResult.isEmpty && name.length == 1) {
      if (v1SessionCatalog.lookupBuiltinOrTempFunction(name.head).isDefined) {
        throw QueryCompilationErrors.notATableFunctionError(name.mkString("."))
      }
    }

    tableFunctionResult
  }

  private def validateFunction(
      func: Expression,
      numArgs: Int,
      unresolvedFunc: UnresolvedFunction): Expression = {
    func match {
      case owg: SupportsOrderingWithinGroup
          if !owg.isDistinctSupported && unresolvedFunc.isDistinct =>
        throw QueryCompilationErrors.distinctWithOrderingFunctionUnsupportedError(owg.prettyName)
      case owg: SupportsOrderingWithinGroup
          if owg.isOrderingMandatory && !owg.orderingFilled &&
            unresolvedFunc.orderingWithinGroup.isEmpty =>
        throw QueryCompilationErrors.functionMissingWithinGroupError(owg.prettyName)
      case owg: SupportsOrderingWithinGroup
          if owg.orderingFilled && unresolvedFunc.orderingWithinGroup.nonEmpty =>
        // e.g mode(expr1) within group (order by expr2) is not supported
        throw QueryCompilationErrors.wrongNumOrderingsForFunctionError(
          owg.prettyName,
          0,
          unresolvedFunc.orderingWithinGroup.length
        )
      case f if !f.isInstanceOf[SupportsOrderingWithinGroup] &&
          unresolvedFunc.orderingWithinGroup.nonEmpty =>
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          func.prettyName,
          "WITHIN GROUP (ORDER BY ...)"
        )
      // AggregateWindowFunctions are AggregateFunctions that can only be evaluated within
      // the context of a Window clause. They do not need to be wrapped in an
      // AggregateExpression.
      case wf: AggregateWindowFunction =>
        if (unresolvedFunc.isDistinct) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(wf.prettyName, "DISTINCT")
        } else if (unresolvedFunc.filter.isDefined) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            wf.prettyName,
            "FILTER clause"
          )
        } else {
          resolveIgnoreNulls(wf, unresolvedFunc.ignoreNulls)
        }
      case owf: FrameLessOffsetWindowFunction =>
        if (unresolvedFunc.isDistinct) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            owf.prettyName,
            "DISTINCT"
          )
        } else if (unresolvedFunc.filter.isDefined) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            owf.prettyName,
            "FILTER clause"
          )
        } else {
          resolveIgnoreNulls(owf, unresolvedFunc.ignoreNulls)
        }
      // We get an aggregate function, we need to wrap it in an AggregateExpression.
      case agg: AggregateFunction =>
        // Note: PythonUDAF does not support these advanced clauses.
        if (agg.isInstanceOf[PythonUDAF]) checkUnsupportedAggregateClause(agg, unresolvedFunc)
        // After parse, the functions not set the ordering within group yet.
        val newAgg = agg match {
          case owg: SupportsOrderingWithinGroup
              if !owg.orderingFilled && unresolvedFunc.orderingWithinGroup.nonEmpty =>
            owg.withOrderingWithinGroup(unresolvedFunc.orderingWithinGroup)
          case _ =>
            agg
        }

        unresolvedFunc.filter match {
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
        val aggFunc = resolveIgnoreNulls(newAgg, unresolvedFunc.ignoreNulls)
        aggFunc.toAggregateExpression(unresolvedFunc.isDistinct, unresolvedFunc.filter)
      // This function is not an aggregate function, just return the resolved one.
      case other =>
        checkUnsupportedAggregateClause(other, unresolvedFunc)
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

  private def checkUnsupportedAggregateClause(
      func: Expression,
      unresolvedFunc: UnresolvedFunction): Unit = {
    if (unresolvedFunc.isDistinct) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(func.prettyName, "DISTINCT")
    }
    if (unresolvedFunc.filter.isDefined) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        func.prettyName,
        "FILTER clause"
      )
    }
    // Only fail for IGNORE NULLS; RESPECT NULLS is the default behavior
    if (unresolvedFunc.ignoreNulls.contains(true)) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        func.prettyName,
        "IGNORE NULLS"
      )
    }
  }

  /**
   * Resolves the IGNORE NULLS / RESPECT NULLS clause for a function.
   * If ignoreNulls is defined, applies it to the function; otherwise returns unchanged.
   */
  private def resolveIgnoreNulls[T <: Expression](func: T, ignoreNulls: Option[Boolean]): T = {
    ignoreNulls.map(applyIgnoreNulls(func, _)).getOrElse(func)
  }

  /**
   * Applies the IGNORE NULLS / RESPECT NULLS clause to functions that support it.
   * Returns the modified function if supported, throws error otherwise.
   */
  private def applyIgnoreNulls[T <: Expression](func: T, ignoreNulls: Boolean): T = {
    val result = func match {
      // Window functions
      case nthValue: NthValue => nthValue.copy(ignoreNulls = ignoreNulls)
      case lead: Lead => lead.copy(ignoreNulls = ignoreNulls)
      case lag: Lag => lag.copy(ignoreNulls = ignoreNulls)
      // Aggregate functions
      case first: First => first.copy(ignoreNulls = ignoreNulls)
      case last: Last => last.copy(ignoreNulls = ignoreNulls)
      case anyValue: AnyValue => anyValue.copy(ignoreNulls = ignoreNulls)
      case collectList: CollectList => collectList.copy(ignoreNulls = ignoreNulls)
      case collectSet: CollectSet => collectSet.copy(ignoreNulls = ignoreNulls)
      case _ if ignoreNulls =>
        // Only fail for IGNORE NULLS; RESPECT NULLS is the default behavior
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          func.prettyName,
          "IGNORE NULLS"
        )
      case _ =>
        // RESPECT NULLS is the default, silently return unchanged
        func
    }
    result.asInstanceOf[T]
  }

  private def resolveV2Function(
      unbound: UnboundFunction,
      arguments: Seq[Expression],
      unresolvedFunc: UnresolvedFunction): Expression = {
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
        processV2ScalarFunction(scalarFunc, arguments, unresolvedFunc)
      case aggFunc: V2AggregateFunction[_, _] =>
        processV2AggregateFunction(aggFunc, arguments, unresolvedFunc)
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
      unresolvedFunc: UnresolvedFunction): Expression = {
    if (unresolvedFunc.isDistinct) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(scalarFunc.name(), "DISTINCT")
    } else if (unresolvedFunc.filter.isDefined) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        scalarFunc.name(),
        "FILTER clause"
      )
    } else if (unresolvedFunc.ignoreNulls.contains(true)) {
      // Only fail for IGNORE NULLS; RESPECT NULLS is the default behavior
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
      unresolvedFunc: UnresolvedFunction): Expression = {
    // Only fail for IGNORE NULLS; RESPECT NULLS is the default behavior
    if (unresolvedFunc.ignoreNulls.contains(true)) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        aggFunc.name(),
        "IGNORE NULLS"
      )
    }
    val aggregator = V2Aggregator(aggFunc, arguments)
    aggregator.toAggregateExpression(unresolvedFunc.isDistinct, unresolvedFunc.filter)
  }

  private def failAnalysis(errorClass: String, messageParameters: Map[String, String]): Nothing = {
    throw new AnalysisException(
      errorClass = errorClass,
      messageParameters = messageParameters)
  }
}

/**
 * Companion object with shared utility methods for function name qualification checks.
 */
object FunctionResolution {
  /**
   * Check if a function name is qualified as an extension function.
   * Valid forms: extension.func or system.extension.func
   */
  def maybeExtensionFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithNamespace(nameParts, CatalogManager.EXTENSION_NAMESPACE)
  }

  /**
   * Check if a function name is qualified as a builtin function.
   * Valid forms: builtin.func or system.builtin.func
   */
  def maybeBuiltinFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithNamespace(nameParts, CatalogManager.BUILTIN_NAMESPACE)
  }

  /**
   * Check if a function name is qualified as a session temporary function.
   * Valid forms: session.func or system.session.func
   */
  def maybeTempFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithNamespace(nameParts, CatalogManager.SESSION_NAMESPACE)
  }

  /**
   * Single qualification result for session namespaces: returns the kind when nameParts
   * is explicitly qualified as extension, builtin, or session; None otherwise.
   * Use this instead of calling maybeBuiltinFunctionName/maybeTempFunctionName/
   * maybeExtensionFunctionName in multiple places so namespace rules live in one place.
   *
   * @param nameParts The function name parts (e.g. Seq("builtin", "abs"), Seq("session", "my_udf"))
   * @return Some(Builtin), Some(Temp), or Some(Extension) for 2/3-part session qualification;
   *         None otherwise
   */
  def sessionNamespaceKind(nameParts: Seq[String])
    : Option[org.apache.spark.sql.catalyst.catalog.SessionCatalog.SessionFunctionKind] = {
    if (nameParts.length <= 1) None
    else if (maybeExtensionFunctionName(nameParts)) {
      Some(org.apache.spark.sql.catalyst.catalog.SessionCatalog.Extension)
    } else if (maybeBuiltinFunctionName(nameParts)) {
      Some(org.apache.spark.sql.catalyst.catalog.SessionCatalog.Builtin)
    } else if (maybeTempFunctionName(nameParts)) {
      Some(org.apache.spark.sql.catalyst.catalog.SessionCatalog.Temp)
    } else None
  }

  /**
   * Checks if a multi-part name is qualified with a specific namespace.
   * Supports both 2-part (namespace.name) and 3-part (system.namespace.name) qualifications.
   * Validates both the namespace prefix AND that a function name is present.
   *
   * @param nameParts The multi-part name to check
   * @param namespace The namespace to check for (e.g., "extension", "builtin", "session")
   * @return true if qualified with the given namespace and has a non-empty function name
   */
  def isQualifiedWithNamespace(nameParts: Seq[String], namespace: String): Boolean = {
    nameParts.length match {
      case 2 =>
        // Format: namespace.funcName (e.g., "builtin.abs")
        nameParts.head.equalsIgnoreCase(namespace) && nameParts.last.nonEmpty
      case 3 =>
        // Format: system.namespace.funcName (e.g., "system.builtin.abs")
        nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(namespace) &&
        nameParts(2).nonEmpty
      case _ => false
    }
  }

  /**
   * Returns true if the name is unqualified or explicitly qualified as builtin with the given
   * function name. Used for special syntax (e.g. COUNT(*) -> COUNT(1)) that should only apply
   * to the builtin function, not to user-defined or persistent functions.
   *
   * @param nameParts The function name parts (e.g. Seq("count"), Seq("builtin", "count")).
   * @param expectedName The expected function name (e.g. "count").
   */
  def isUnqualifiedOrBuiltinFunctionName(nameParts: Seq[String], expectedName: String): Boolean = {
    nameParts.lastOption.exists(_.equalsIgnoreCase(expectedName)) &&
      (nameParts.size == 1 || maybeBuiltinFunctionName(nameParts))
  }
}
