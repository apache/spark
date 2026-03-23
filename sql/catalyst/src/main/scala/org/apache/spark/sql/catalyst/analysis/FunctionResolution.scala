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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  CatalogNotFoundException,
  CatalogPlugin,
  CatalogV2Util,
  Identifier,
  LookupCatalog
}

/**
 * Represents the type/location of a function.
 */
sealed trait FunctionType
object FunctionType {
  /** Function is a built-in or temporary function in the session registry. */
  case object Local extends FunctionType
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

  /** Returns the current catalog path, preferring the view's context if resolving a view. */
  private def currentCatalogPath: Seq[String] = {
    val ctx = AnalysisContext.get.catalogAndNamespace
    if (ctx.nonEmpty) ctx
    else (Seq(catalogManager.currentCatalog.name) ++ catalogManager.currentNamespace).toSeq
  }

  /** True if nameParts is 3-part and the first part is the system catalog name. */
  private def isSystemCatalogQualified(nameParts: Seq[String]): Boolean =
    nameParts.length == 3 &&
      nameParts.head.equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME)

  /**
   * Produces the ordered list of candidate names for resolution. Expansion happens in two cases:
   *
   * 1. Single-part names: expanded via the search path, where each search path entry is
   *    fully qualified so appending the name produces fully qualified candidates.
   * 2. `builtin.name` or `session.name`: prepending `system` creates a fully qualified
   *    system catalog candidate. The original 2-part name is also kept as a persistent
   *    catalog candidate (qualified downstream). Order is controlled by
   *    the `persistentCatalogFirst` config.
   *
   * All other multi-part names are returned as-is for downstream resolution.
   */
  private def resolutionCandidates(nameParts: Seq[String]): Seq[Seq[String]] = {
    if (nameParts.size == 1) {
      val searchPath = SQLConf.get.resolutionSearchPath(currentCatalogPath)
      searchPath.map(_ ++ nameParts)
    } else if (nameParts.size == 2 &&
        FunctionResolution.sessionNamespaceKind(nameParts).isDefined) {
      val systemCandidate = CatalogManager.SYSTEM_CATALOG_NAME +: nameParts
      if (SQLConf.get.prioritizeSystemCatalog) {
        Seq(systemCandidate, nameParts)
      } else {
        Seq(nameParts, systemCandidate)
      }
    } else {
      Seq(nameParts)
    }
  }

  private def resolveFunctionCandidate(
      nameParts: Seq[String],
      unresolvedFunc: UnresolvedFunction): Option[Expression] = {
    if (isSystemCatalogQualified(nameParts)) {
      v1SessionCatalog.identifierFromSystemNameParts(nameParts).flatMap { ident =>
        val expr = v1SessionCatalog.resolveScalarFunctionByIdentifier(
          ident, unresolvedFunc.arguments)
        if (expr.isEmpty) {
          if (v1SessionCatalog.lookupFunctionInfoByIdentifier(
              ident, tableFunction = true).isDefined) {
            throw QueryCompilationErrors.notAScalarFunctionError(ident.funcName, unresolvedFunc)
          }
        }
        expr.map(e => validateFunction(e, unresolvedFunc.arguments.length, unresolvedFunc))
      }
    } else {
      try {
        val CatalogAndIdentifier(catalog, ident) =
          relationResolution.expandIdentifier(nameParts)
        val loaded = catalog.asFunctionCatalog.loadFunction(ident)
        Some(loaded match {
          case v1Func: V1Function =>
            val func = v1Func.invoke(unresolvedFunc.arguments)
            validateFunction(func, unresolvedFunc.arguments.length, unresolvedFunc)
          case unboundV2Func =>
            resolveV2Function(unboundV2Func, unresolvedFunc.arguments, unresolvedFunc)
        })
      } catch {
        case _: NoSuchFunctionException |
             _: NoSuchNamespaceException |
             _: CatalogNotFoundException =>
          None
        case e: AnalysisException if e.getCondition == "FORBIDDEN_OPERATION" =>
          None
        case e: AnalysisException =>
          throw e
      }
    }
  }

  def resolveFunction(unresolvedFunc: UnresolvedFunction): Expression = {
    withPosition(unresolvedFunc) {
      // Internal functions resolve via the internal registry when the parser marks them as
      // internal; they are not resolved via the search path.
      if (unresolvedFunc.isInternal && unresolvedFunc.nameParts.size == 1) {
        try {
          val func = FunctionRegistry.internal.lookupFunction(
            FunctionIdentifier(unresolvedFunc.nameParts.head), unresolvedFunc.arguments)
          return validateFunction(func, unresolvedFunc.arguments.length, unresolvedFunc)
        } catch {
          case _: NoSuchFunctionException =>
            // Ignore and try standard resolution.
          case e: AnalysisException if e.getCondition == "UNRESOLVED_ROUTINE" =>
            // The internal registry throws AnalysisException when the function is not found;
            // fall through to standard resolution.
        }
      }

      val candidates = resolutionCandidates(unresolvedFunc.nameParts)
      for (nameParts <- candidates) {
        resolveFunctionCandidate(nameParts, unresolvedFunc) match {
          case Some(expr) => return expr
          case None =>
        }
      }
      val searchPath = SQLConf.get.resolutionSearchPath(currentCatalogPath)
      throw QueryCompilationErrors.unresolvedRoutineError(
        unresolvedFunc.nameParts, searchPath.map(toSQLId), unresolvedFunc.origin)
    }
  }

  private def resolveTableFunctionCandidate(
      nameParts: Seq[String],
      arguments: Seq[Expression]): Option[LogicalPlan] = {
    if (isSystemCatalogQualified(nameParts)) {
      v1SessionCatalog.identifierFromSystemNameParts(nameParts).flatMap { ident =>
        val resolvedPlan = v1SessionCatalog.resolveTableFunctionByIdentifier(ident, arguments)
        if (resolvedPlan.isDefined) return resolvedPlan
        if (v1SessionCatalog.lookupFunctionInfoByIdentifier(
            ident, tableFunction = false).isDefined) {
          throw QueryCompilationErrors.notATableFunctionError(ident.funcName)
        }
        None
      }
    } else {
      val CatalogAndIdentifier(catalog, ident) = relationResolution.expandIdentifier(nameParts)
      try {
        if (CatalogV2Util.isSessionCatalog(catalog)) {
          Some(v1SessionCatalog.resolvePersistentTableFunction(
            ident.asFunctionIdentifier, arguments))
        } else {
          throw QueryCompilationErrors.missingCatalogTableValuedFunctionsAbilityError(catalog)
        }
      } catch {
        case _: NoSuchFunctionException | _: NoSuchNamespaceException |
             _: CatalogNotFoundException =>
          tryRethrowNotTableFunction(catalog, ident)
          None
        case e: AnalysisException if e.getCondition == "FORBIDDEN_OPERATION" =>
          None
        case e: AnalysisException =>
          throw e
      }
    }
  }

  /**
   * On table-function lookup failure, throw a clearer error if the name exists
   * as a non-table function.
   */
  private def tryRethrowNotTableFunction(catalog: CatalogPlugin, ident: Identifier): Unit = {
    try {
      if (CatalogV2Util.isSessionCatalog(catalog)) {
        if (v1SessionCatalog.isPersistentFunction(ident.asFunctionIdentifier)) {
          throw QueryCompilationErrors.notATableFunctionError(ident.name())
        }
      } else if (catalog.asFunctionCatalog.functionExists(ident)) {
        throw QueryCompilationErrors.notATableFunctionError(ident.name())
      }
    } catch {
      case _: NoSuchFunctionException | _: NoSuchNamespaceException |
           _: CatalogNotFoundException =>
        ()
      case e: AnalysisException if e.getCondition == "FORBIDDEN_OPERATION" =>
        ()
    }
  }

  def resolveTableFunction(
      nameParts: Seq[String],
      arguments: Seq[Expression]): Option[LogicalPlan] = {
    val candidates = resolutionCandidates(nameParts)
    for (nameParts <- candidates) {
      resolveTableFunctionCandidate(nameParts, arguments) match {
        case Some(plan) => return Some(plan)
        case None =>
      }
    }
    None
  }

  private def resolveInternalFunction(
      name: String, arguments: Seq[Expression]): Expression = {
    val qualified = FunctionIdentifier(
      name, Some(CatalogManager.SESSION_NAMESPACE), Some(CatalogManager.SYSTEM_CATALOG_NAME))
    if (FunctionRegistry.internal.functionExists(qualified)) {
      FunctionRegistry.internal.lookupFunction(qualified, arguments)
    } else {
      FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name), arguments)
    }
  }

  def resolveBuiltinOrTempFunction(
      name: Seq[String],
      arguments: Seq[Expression],
      u: UnresolvedFunction): Option[Expression] = {
    val expression = if (name.length == 1 && u.isInternal) {
      Option(resolveInternalFunction(name.head, arguments))
    } else if (name.length == 1) {
      v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
    } else {
      None
    }
    expression.map { func =>
      validateFunction(func, arguments.length, u)
    }
  }

  def resolveTableValuedFunction(u: UnresolvedTableValuedFunction): LogicalPlan = {
    resolveTableFunction(u.name, u.functionArgs)
      .getOrElse {
        throw new NoSuchFunctionException(
          db = u.name.dropRight(1).mkString("."),
          func = u.name.last)
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
      nameParts: Seq[String],
      unresolvedFunc: Option[UnresolvedFunction]): Option[ExpressionInfo] = {
    val n = nameParts.length
    if (n == 1 && unresolvedFunc.exists(_.isInternal)) {
      return FunctionRegistry.internal.lookupFunction(FunctionIdentifier(nameParts.head))
    }
    v1SessionCatalog.identifierFromSystemNameParts(nameParts) match {
      case Some(ident) =>
        v1SessionCatalog.lookupFunctionInfoByIdentifier(ident, tableFunction = false)
      case None =>
        if (n == 1) v1SessionCatalog.lookupBuiltinOrTempFunction(nameParts.head)
        else None
    }
  }

  def lookupBuiltinOrTempTableFunction(nameParts: Seq[String]): Option[ExpressionInfo] = {
    val n = nameParts.length
    v1SessionCatalog.identifierFromSystemNameParts(nameParts) match {
      case Some(ident) =>
        v1SessionCatalog.lookupFunctionInfoByIdentifier(ident, tableFunction = true)
      case None =>
        if (n == 1) v1SessionCatalog.lookupBuiltinOrTempTableFunction(nameParts.head)
        else None
    }
  }

  /**
   * Determines the type/location of a function (builtin, temporary, persistent, etc.).
   * This is used by the LookupFunctions analyzer rule for early validation and optimization.
   *
   * Note: may throw for malformed identifiers (e.g. REQUIRES_SINGLE_PART_NAMESPACE).
   *
   * @param nameParts The function name parts.
   * @param unresolvedFunc Optional UnresolvedFunction node for lookups that may need it.
   * @return The type of the function (Local, Persistent, TableOnly, or NotFound).
   */
  def lookupFunctionType(
      nameParts: Seq[String],
      unresolvedFunc: Option[UnresolvedFunction] = None): FunctionType = {

    if (lookupBuiltinOrTempFunction(nameParts, unresolvedFunc).isDefined) {
      return FunctionType.Local
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
   * Check if a function name is qualified as a builtin function.
   * Valid forms: builtin.func or system.builtin.func
   */
  private def maybeBuiltinFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithSystemNamespace(nameParts, CatalogManager.BUILTIN_NAMESPACE)
  }

  /**
   * Check if a function name is qualified as a session temporary function.
   * Valid forms: session.func or system.session.func
   */
  private def maybeTempFunctionName(nameParts: Seq[String]): Boolean = {
    isQualifiedWithSystemNamespace(nameParts, CatalogManager.SESSION_NAMESPACE)
  }

  /**
   * Single qualification result for session namespaces: returns the kind when nameParts
   * is explicitly qualified as builtin or session; None otherwise.
   *
   * @param nameParts The function name parts (e.g. Seq("builtin", "abs"), Seq("session", "my_udf"))
   * @return Some(Builtin) or Some(Temp) for 2/3-part session qualification; None otherwise
   */
  def sessionNamespaceKind(nameParts: Seq[String])
    : Option[org.apache.spark.sql.catalyst.catalog.SessionCatalog.SessionFunctionKind] = {
    if (nameParts.length <= 1) None
    else if (maybeBuiltinFunctionName(nameParts)) {
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
   * @param namespace The namespace to check for (e.g., "builtin", "session")
   * @return true if qualified with the given namespace and has a non-empty function name
   */
  private def isQualifiedWithSystemNamespace(nameParts: Seq[String], namespace: String): Boolean = {
    val n = nameParts.length
    if (n == 2) {
      // Format: namespace.funcName (e.g., "builtin.abs")
      nameParts.head.equalsIgnoreCase(namespace) && nameParts.last.nonEmpty
    } else if (n == 3) {
      // Format: system.namespace.funcName (e.g., "system.builtin.abs")
      nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(namespace) &&
        nameParts(2).nonEmpty
    } else {
      false
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
      (nameParts.length == 1 || maybeBuiltinFunctionName(nameParts))
  }
}
