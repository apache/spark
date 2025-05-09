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

import scala.collection.mutable

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.analysis.ResolveWithCTE.checkIfSelfReferenceIsPlacedCorrectly
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ListAgg, Median, PercentileCont, PercentileDisc}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, PLAN_EXPRESSION, UNRESOLVED_WINDOW_EXPRESSION}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, StringUtils, TypeUtils}
import org.apache.spark.sql.connector.catalog.{LookupCatalog, SupportsPartitionManagement}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/**
 * Throws user facing errors when passed invalid queries that fail to analyze.
 */
trait CheckAnalysis extends LookupCatalog with QueryErrorsBase with PlanToString {

  protected def isView(nameParts: Seq[String]): Boolean

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  // Error that is not supposed to throw immediately on triggering, e.g. certain internal errors.
  // The error will be thrown at the end of the whole check analysis process, if no other error
  // occurs.
  val preemptedError = new PreemptedError()

  /**
   * Fails the analysis at the point where a specific tree node was parsed using a provided
   * error class and message parameters.
   */
  def failAnalysis(errorClass: String, messageParameters: Map[String, String]): Nothing = {
    throw new AnalysisException(
      errorClass = errorClass,
      messageParameters = messageParameters)
  }

  protected def hasMapType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[MapType])
  }

  protected def hasVariantType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[VariantType])
  }

  protected def mapColumnInSetOperation(plan: LogicalPlan): Option[Attribute] = plan match {
    case _: Intersect | _: Except | _: Distinct =>
      plan.output.find(a => hasMapType(a.dataType))
    case d: Deduplicate =>
      d.keys.find(a => hasMapType(a.dataType))
    case _ => None
  }

  protected def variantColumnInSetOperation(plan: LogicalPlan): Option[Attribute] = plan match {
    case _: Intersect | _: Except | _: Distinct =>
      plan.output.find(a => hasVariantType(a.dataType))
    case d: Deduplicate =>
      d.keys.find(a => hasVariantType(a.dataType))
    case _ => None
  }

  protected def variantExprInPartitionExpression(plan: LogicalPlan): Option[Expression] =
    plan match {
      case r: RepartitionByExpression =>
        r.partitionExpressions.find(e => hasVariantType(e.dataType))
      case _ => None
    }

  private def checkLimitLikeClause(name: String, limitExpr: Expression): Unit = {
    limitExpr match {
      case e if !e.foldable => limitExpr.failAnalysis(
        errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE",
        messageParameters = Map(
          "name" -> name,
          "expr" -> toSQLExpr(limitExpr)))
      case e if e.dataType != IntegerType => limitExpr.failAnalysis(
        errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
        messageParameters = Map(
          "name" -> name,
          "expr" -> toSQLExpr(limitExpr),
          "dataType" -> toSQLType(e.dataType)))
      case e =>
        e.eval() match {
          case null => limitExpr.failAnalysis(
            errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL",
            messageParameters = Map(
              "name" -> name,
              "expr" -> toSQLExpr(limitExpr)))
          case v: Int if v < 0 => limitExpr.failAnalysis(
            errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE",
            messageParameters = Map(
              "name" -> name,
              "expr" -> toSQLExpr(limitExpr),
              "v" -> toSQLValue(v, IntegerType)))
          case _ => // OK
        }
    }
  }

  /** Check and throw exception when a given resolved plan contains LateralColumnAliasReference. */
  private def checkNotContainingLCA(exprs: Seq[Expression], plan: LogicalPlan): Unit = {
    exprs.foreach(_.transformDownWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
      case lcaRef: LateralColumnAliasReference =>
        // this should be a low priority internal error to be preempted
        preemptedError.set(
          SparkException.internalError(
            "Resolved plan should not contain any " +
            s"LateralColumnAliasReference.\nDebugging information: plan:\n$plan",
            context = lcaRef.origin.getQueryContext,
            summary = lcaRef.origin.context.summary)
        )
        lcaRef
    })
  }

  private def isMapWithStringKey(e: Expression): Boolean = if (e.resolved) {
    e.dataType match {
      case m: MapType => m.keyType.isInstanceOf[StringType]
      case _ => false
    }
  } else {
    false
  }

  private def failUnresolvedAttribute(
      operator: LogicalPlan,
      a: Attribute,
      errorClass: String): Nothing = {
    val missingCol = a.sql
    val candidates = operator.inputSet.toSeq
      .map(attr => attr.qualifier :+ attr.name)
    val orderedCandidates =
      StringUtils.orderSuggestedIdentifiersBySimilarity(missingCol, candidates)
    throw QueryCompilationErrors.unresolvedAttributeError(
      errorClass, missingCol, orderedCandidates, a.origin)
  }

  /**
   * Checks whether the operator allows non-deterministic expressions.
   */
  private def operatorAllowsNonDeterministicExpressions(plan: LogicalPlan): Boolean = {
    plan match {
      case p: SupportsNonDeterministicExpression =>
        p.allowNonDeterministicExpression
      case _ => false
    }
  }

  private def checkForUnspecifiedWindow(expressions: Seq[Expression]): Unit = {
    expressions.foreach(_.transformDownWithPruning(
      _.containsPattern(UNRESOLVED_WINDOW_EXPRESSION)) {
      case UnresolvedWindowExpression(_, windowSpec) =>
        throw QueryCompilationErrors.windowSpecificationNotDefinedError(windowSpec.name)
      }
    )
  }

  private def containsUnsupportedLCA(e: Expression, operator: LogicalPlan): Boolean = {
    e.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE) && operator.expressions.exists {
      case a: Alias
        if e.collect { case l: LateralColumnAliasReference => l.nameParts.head }.contains(a.name) =>
        a.exists(_.isInstanceOf[Generator])
      case _ => false
    }
  }

  /**
   * Checks for errors in a `SELECT` clause, such as a trailing comma or an empty select list.
   *
   * @param plan The logical plan of the query.
   * @param starRemoved Whether a '*' (wildcard) was removed from the select list.
   * @throws AnalysisException if the select list is empty or ends with a trailing comma.
   */
  protected def checkTrailingCommaInSelect(
      plan: LogicalPlan,
      starRemoved: Boolean = false): Unit = {
    val exprList = plan match {
      case proj: Project if proj.projectList.nonEmpty =>
        proj.projectList
      case agg: Aggregate if agg.aggregateExpressions.nonEmpty =>
        agg.aggregateExpressions
      case _ =>
        Seq.empty
    }

    exprList.lastOption match {
      case Some(Alias(UnresolvedAttribute(Seq(name)), _)) =>
        if (name.equalsIgnoreCase("FROM") && plan.exists(_.isInstanceOf[OneRowRelation])) {
          if (exprList.size > 1  || starRemoved) {
            throw QueryCompilationErrors.trailingCommaInSelectError(exprList.last.origin)
          }
        }
      case _ =>
    }
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We should inline all CTE relations to restore the original plan shape, as the analysis check
    // may need to match certain plan shapes. For dangling CTE relations, they will still be kept
    // in the original `WithCTE` node, as we need to perform analysis check for them as well.
    val inlineCTE = InlineCTE(alwaysInline = true, keepDanglingRelations = true)
    val inlinedPlan: LogicalPlan = try {
      inlineCTE(plan)
    } catch {
      case e: AnalysisException =>
        throw new ExtendedAnalysisException(e, plan)
    }
    preemptedError.clear()
    try {
      checkAnalysis0(inlinedPlan)
      preemptedError.getErrorOpt().foreach(throw _) // throw preempted error if any
    } catch {
      case e: AnalysisException =>
        throw new ExtendedAnalysisException(e, inlinedPlan)
    } finally {
      preemptedError.clear()
    }
    plan.setAnalyzed()
  }

  def checkAnalysis0(plan: LogicalPlan): Unit = {
    // The target table is not a child plan of the insert command. We should report errors for table
    // not found first, instead of errors in the input query of the insert command, by doing a
    // top-down traversal.
    plan.foreach {
      case InsertIntoStatement(u: UnresolvedRelation, _, _, _, _, _, _) =>
        u.tableNotFound(u.multipartIdentifier)

      // TODO (SPARK-27484): handle streaming write commands when we have them.
      case write: V2WriteCommand if write.table.isInstanceOf[UnresolvedRelation] =>
        val tblName = write.table.asInstanceOf[UnresolvedRelation].multipartIdentifier
        write.table.tableNotFound(tblName)

      // We should check for trailing comma errors first, since we would get less obvious
      // unresolved column errors if we do it bottom up
      case proj: Project =>
        checkTrailingCommaInSelect(proj)
      case agg: Aggregate =>
        checkTrailingCommaInSelect(agg)
      case unionLoop: UnionLoop =>
        // Recursive CTEs have already substituted Union to UnionLoop at this stage.
        // Here we perform additional checks for them.
        checkIfSelfReferenceIsPlacedCorrectly(unionLoop, unionLoop.id)

      case _ =>
    }

    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {
      case p if p.analyzed => // Skip already analyzed sub-plans

      case leaf: LeafNode if !SQLConf.get.preserveCharVarcharTypeInfo &&
        leaf.output.map(_.dataType).exists(CharVarcharUtils.hasCharVarchar) =>
        throw SparkException.internalError(
          s"Logical plan should not have output of char/varchar type when " +
            s"${SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key} is false: " + leaf)

      case u: UnresolvedNamespace =>
        u.schemaNotFound(u.multipartIdentifier)

      case u: UnresolvedTable =>
        u.tableNotFound(u.multipartIdentifier)

      case u: UnresolvedView =>
        u.tableNotFound(u.multipartIdentifier)

      case u: UnresolvedTableOrView =>
        u.tableNotFound(u.multipartIdentifier)

      case u: UnresolvedRelation =>
        u.tableNotFound(u.multipartIdentifier)

      case u: UnresolvedFunctionName =>
        val catalogPath = (currentCatalog.name +: catalogManager.currentNamespace).mkString(".")
        throw QueryCompilationErrors.unresolvedRoutineError(
          u.multipartIdentifier,
          Seq("system.builtin", "system.session", catalogPath),
          u.origin)

      case u: UnresolvedHint =>
        throw SparkException.internalError(
          msg = s"Hint not found: ${toSQLId(u.name)}",
          context = u.origin.getQueryContext,
          summary = u.origin.context.summary)

      case u: UnresolvedInlineTable if unresolvedInlineTableContainsScalarSubquery(u) =>
        throw QueryCompilationErrors.inlineTableContainsScalarSubquery(u)

      case command: V2PartitionCommand =>
        command.table match {
          case r @ ResolvedTable(_, _, table, _) => table match {
            case t: SupportsPartitionManagement =>
              if (t.partitionSchema.isEmpty) {
                r.failAnalysis(
                  errorClass = "INVALID_PARTITION_OPERATION.PARTITION_SCHEMA_IS_EMPTY",
                  messageParameters = Map("name" -> toSQLId(r.name)))
              }
            case _ =>
              r.failAnalysis(
                errorClass = "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED",
                messageParameters = Map("name" -> toSQLId(r.name)))
          }
          case _ =>
        }

      case o: OverwriteByExpression if o.deleteExpr.exists(_.isInstanceOf[SubqueryExpression]) =>
        o.deleteExpr.failAnalysis (
          errorClass = "UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY",
          messageParameters = Map.empty)

      case operator: LogicalPlan =>
        operator transformExpressionsDown {
          case hof: HigherOrderFunction if hof.arguments.exists {
            case LambdaFunction(_, _, _) => true
            case _ => false
          } =>
            throw new AnalysisException(
              errorClass =
                "INVALID_LAMBDA_FUNCTION_CALL.PARAMETER_DOES_NOT_ACCEPT_LAMBDA_FUNCTION",
              messageParameters = Map.empty,
              origin = hof.origin
            )
          // Check argument data types of higher-order functions downwards first.
          // If the arguments of the higher-order functions are resolved but the type check fails,
          // the argument functions will not get resolved, but we should report the argument type
          // check failure instead of claiming the argument functions are unresolved.
          case hof: HigherOrderFunction
              if hof.argumentsResolved && hof.checkArgumentDataTypes().isFailure =>
            hof.checkArgumentDataTypes() match {
              case checkRes: TypeCheckResult.DataTypeMismatch =>
                hof.dataTypeMismatch(hof, checkRes)
              case checkRes: TypeCheckResult.InvalidFormat =>
                hof.invalidFormat(checkRes)
            }

          case hof: HigherOrderFunction
              if hof.resolved && hof.functions
                .exists(_.exists(_.isInstanceOf[PythonUDF])) =>
            val u = hof.functions.flatMap(_.find(_.isInstanceOf[PythonUDF])).head
            hof.failAnalysis(
              errorClass = "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF",
              messageParameters = Map("funcName" -> toSQLExpr(u)))

          // If an attribute can't be resolved as a map key of string type, either the key should be
          // surrounded with single quotes, or there is a typo in the attribute name.
          case GetMapValue(map, key: Attribute) if isMapWithStringKey(map) && !key.resolved =>
            failUnresolvedAttribute(operator, key, "UNRESOLVED_MAP_KEY")

          case e: Expression if containsUnsupportedLCA(e, operator) =>
            val lcaRefNames =
              e.collect { case lcaRef: LateralColumnAliasReference => lcaRef.name }.distinct
            failAnalysis(
              errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_GENERATOR",
              messageParameters =
                Map("lca" -> toSQLId(lcaRefNames), "generatorExpr" -> toSQLExpr(e)))
        }

        // Fail if we still have an unresolved all in group by. This needs to run before the
        // general unresolved check below to throw a more tailored error message.
        new ResolveReferencesInAggregate(catalogManager).checkUnresolvedGroupByAll(operator)

        // Early checks for column definitions, to produce better error messages
        ColumnDefinition.checkColumnDefinitions(operator)

        var stagedError: Option[() => Unit] = None
        getAllExpressions(operator).foreach(_.foreachUp {
          case a: Attribute if !a.resolved =>
            failUnresolvedAttribute(operator, a, "UNRESOLVED_COLUMN")

          case s: Star =>
            withPosition(s) {
              throw QueryCompilationErrors.invalidStarUsageError(operator.nodeName, Seq(s))
            }

          // Should be before `e.checkInputDataTypes()` to produce the correct error for unknown
          // window expressions nested inside other expressions
          case UnresolvedWindowExpression(_, WindowSpecReference(windowName)) =>
            throw QueryCompilationErrors.windowSpecificationNotDefinedError(windowName)

          case e: Expression if e.checkInputDataTypes().isFailure =>
            TypeCoercionValidation.failOnTypeCheckResult(e, Some(operator))

          case c: Cast if !c.resolved =>
            throw SparkException.internalError(
              msg = s"Found the unresolved Cast: ${c.simpleString(SQLConf.get.maxToStringFields)}",
              context = c.origin.getQueryContext,
              summary = c.origin.context.summary)
          case e: RuntimeReplaceable if !e.replacement.resolved =>
            throw SparkException.internalError(
              s"Cannot resolve the runtime replaceable expression ${toSQLExpr(e)}. " +
              s"The replacement is unresolved: ${toSQLExpr(e.replacement)}.")

          // `Grouping` and `GroupingID` are considered as of having lower priority than the other
          // nodes which cause errors.
          case g: Grouping =>
            if (stagedError.isEmpty) stagedError = Some(() => g.failAnalysis(
              errorClass = "UNSUPPORTED_GROUPING_EXPRESSION", messageParameters = Map.empty))
          case g: GroupingID =>
            if (stagedError.isEmpty) stagedError = Some(() => g.failAnalysis(
              errorClass = "UNSUPPORTED_GROUPING_EXPRESSION", messageParameters = Map.empty))

          case e: Expression if e.children.exists(_.isInstanceOf[WindowFunction]) &&
              !e.isInstanceOf[WindowExpression] && e.resolved =>
            val w = e.children.find(_.isInstanceOf[WindowFunction]).get
            e.failAnalysis(
              errorClass = "WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE",
              messageParameters = Map("funcName" -> toSQLExpr(w)))

          case w @ WindowExpression(AggregateExpression(_, _, true, _, _), _) =>
            w.failAnalysis(
              errorClass = "DISTINCT_WINDOW_FUNCTION_UNSUPPORTED",
              messageParameters = Map("windowExpr" -> toSQLExpr(w)))

          case w @ WindowExpression(wf: FrameLessOffsetWindowFunction,
            WindowSpecDefinition(_, order, frame: SpecifiedWindowFrame))
             if order.isEmpty || !frame.isOffset =>
            w.failAnalysis(
              errorClass = "WINDOW_FUNCTION_AND_FRAME_MISMATCH",
              messageParameters = Map(
                "funcName" -> toSQLExpr(wf),
                "windowExpr" -> toSQLExpr(w)))

          case agg @ AggregateExpression(listAgg: ListAgg, _, _, _, _)
            if agg.isDistinct && listAgg.needSaveOrderValue =>
            throw QueryCompilationErrors.functionAndOrderExpressionMismatchError(
              listAgg.prettyName, listAgg.child, listAgg.orderExpressions)

          case w: WindowExpression =>
            // Only allow window functions with an aggregate expression or an offset window
            // function or a Pandas window UDF.
            w.windowFunction match {
              case agg @ AggregateExpression(fun: ListAgg, _, _, _, _)
                // listagg(...) WITHIN GROUP (ORDER BY ...) OVER (ORDER BY ...) is unsupported
                if fun.orderingFilled && (w.windowSpec.orderSpec.nonEmpty ||
                  w.windowSpec.frameSpecification !=
                  SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)) =>
                agg.failAnalysis(
                  errorClass = "INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC",
                  messageParameters = Map("aggFunc" -> toSQLExpr(agg.aggregateFunction)))
              case agg @ AggregateExpression(
                _: PercentileCont | _: PercentileDisc | _: Median, _, _, _, _)
                if w.windowSpec.orderSpec.nonEmpty || w.windowSpec.frameSpecification !=
                    SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing) =>
                agg.failAnalysis(
                  errorClass = "INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC",
                  messageParameters = Map("aggFunc" -> toSQLExpr(agg.aggregateFunction)))
              case _: AggregateExpression | _: FrameLessOffsetWindowFunction |
                  _: AggregateWindowFunction => // OK
              case other =>
                other.failAnalysis(
                  errorClass = "UNSUPPORTED_EXPR_FOR_WINDOW",
                  messageParameters = Map("sqlExpr" -> toSQLExpr(other)))
            }

          case s: SubqueryExpression =>
            checkSubqueryExpression(operator, s)

          case e: ExpressionWithRandomSeed if !e.seedExpression.foldable =>
            e.failAnalysis(
              errorClass = "SEED_EXPRESSION_IS_UNFOLDABLE",
              messageParameters = Map(
                "seedExpr" -> toSQLExpr(e.seedExpression),
                "exprWithSeed" -> toSQLExpr(e)))

          case p: Parameter =>
            p.failAnalysis(
              errorClass = "UNBOUND_SQL_PARAMETER",
              messageParameters = Map("name" -> p.name))

          case ma @ MultiAlias(child, names) if child.resolved && !child.isInstanceOf[Generator] =>
            ma.failAnalysis(
              errorClass = "MULTI_ALIAS_WITHOUT_GENERATOR",
              messageParameters = Map("expr" -> toSQLExpr(child), "names" -> names.mkString(", ")))
          case _ =>
        })

        // Check for unresolved TABLE arguments after the main check above to allow other analysis
        // errors to apply first, providing better error messages.
        getAllExpressions(operator).foreach(_.foreachUp {
          case expr: FunctionTableSubqueryArgumentExpression =>
            expr.failAnalysis(
              errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_TABLE_ARGUMENT",
              messageParameters = Map("treeNode" -> planToString(plan)))
          case _ =>
        })

        if (stagedError.isDefined) stagedError.get.apply()

        operator match {
          case RelationTimeTravel(u: UnresolvedRelation, _, _) =>
            u.tableNotFound(u.multipartIdentifier)

          case etw: EventTimeWatermark =>
            etw.eventTime.dataType match {
              case s: StructType
                if s.find(_.name == "end").map(_.dataType) == Some(TimestampType) =>
              case _: TimestampType =>
              case _ =>
                etw.failAnalysis(
                  errorClass = "EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE",
                  messageParameters = Map(
                    "eventName" -> toSQLId(etw.eventTime.name),
                    "eventType" -> toSQLType(etw.eventTime.dataType)))
            }

          case f: Filter if f.condition.dataType != BooleanType =>
            f.failAnalysis(
              errorClass = "DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN",
              messageParameters = Map(
                "sqlExpr" -> f.expressions.map(toSQLExpr).mkString(","),
                "filter" -> toSQLExpr(f.condition),
                "type" -> toSQLType(f.condition.dataType)))

          case j @ Join(_, _, _, Some(condition), _) if condition.dataType != BooleanType =>
            j.failAnalysis(
              errorClass = "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
              messageParameters = Map(
                "joinCondition" -> toSQLExpr(condition),
                "conditionType" -> toSQLType(condition.dataType)))

          case j @ AsOfJoin(_, _, _, Some(condition), _, _, _)
              if condition.dataType != BooleanType =>
            throw SparkException.internalError(
              msg = s"join condition '${toSQLExpr(condition)}' " +
                s"of type ${toSQLType(condition.dataType)} is not a boolean.",
              context = j.origin.getQueryContext,
              summary = j.origin.context.summary)

          case j @ AsOfJoin(_, _, _, _, _, _, Some(toleranceAssertion)) =>
            if (!toleranceAssertion.foldable) {
              j.failAnalysis(
                errorClass = "AS_OF_JOIN.TOLERANCE_IS_UNFOLDABLE",
                messageParameters = Map.empty)
            }
            if (!toleranceAssertion.eval().asInstanceOf[Boolean]) {
              j.failAnalysis(
                errorClass = "AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE",
                messageParameters = Map.empty)
            }

          case a: Aggregate => ExprUtils.assertValidAggregation(a)

          case CollectMetrics(name, metrics, _, _) =>
            if (name == null || name.isEmpty) {
              operator.failAnalysis(
                errorClass = "INVALID_OBSERVED_METRICS.MISSING_NAME",
                messageParameters = Map("operator" -> planToString(operator)))
            }
            // Check if an expression is a valid metric. A metric must meet the following criteria:
            // - Is not a window function;
            // - Is not nested aggregate function;
            // - Is not a distinct aggregate function;
            // - Has only non-deterministic functions that are nested inside an aggregate function;
            // - Has only attributes that are nested inside an aggregate function.
            def checkMetric(s: Expression, e: Expression, seenAggregate: Boolean = false): Unit = {
              e match {
                case _: WindowExpression =>
                  e.failAnalysis(
                    "INVALID_OBSERVED_METRICS.WINDOW_EXPRESSIONS_UNSUPPORTED",
                    Map("expr" -> toSQLExpr(s)))
                case a: AggregateExpression if seenAggregate =>
                  e.failAnalysis(
                    "INVALID_OBSERVED_METRICS.NESTED_AGGREGATES_UNSUPPORTED",
                    Map("expr" -> toSQLExpr(s)))
                case a: AggregateExpression if a.isDistinct =>
                  e.failAnalysis(
                    "INVALID_OBSERVED_METRICS.AGGREGATE_EXPRESSION_WITH_DISTINCT_UNSUPPORTED",
                    Map("expr" -> toSQLExpr(s)))
                case a: AggregateExpression if a.filter.isDefined =>
                  e.failAnalysis(
                    "INVALID_OBSERVED_METRICS.AGGREGATE_EXPRESSION_WITH_FILTER_UNSUPPORTED",
                    Map("expr" -> toSQLExpr(s)))
                case _: AggregateExpression | _: AggregateFunction =>
                  e.children.foreach(checkMetric (s, _, seenAggregate = true))
                case _: Attribute if !seenAggregate =>
                  e.failAnalysis(
                    "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE",
                    Map("expr" -> toSQLExpr(s)))
                case a: Alias =>
                  checkMetric(s, a.child, seenAggregate)
                case a if !e.deterministic && !seenAggregate =>
                  e.failAnalysis(
                    "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_NON_DETERMINISTIC",
                    Map("expr" -> toSQLExpr(s)))
                case _ =>
                  e.children.foreach(checkMetric (s, _, seenAggregate))
              }
            }
            metrics.foreach(m => checkMetric(m, m))

          // see Analyzer.ResolveUnpivot
          // given ids must be AttributeReference when no values given
          case up @Unpivot(Some(ids), None, _, _, _, _)
            if up.childrenResolved && ids.forall(_.resolved) &&
              ids.exists(! _.isInstanceOf[AttributeReference]) =>
            throw QueryCompilationErrors.unpivotRequiresAttributes("id", "value", up.ids.get)
          // given values must be AttributeReference when no ids given
          case up @Unpivot(None, Some(values), _, _, _, _)
            if up.childrenResolved && values.forall(_.forall(_.resolved)) &&
              values.exists(_.exists(! _.isInstanceOf[AttributeReference])) =>
            throw QueryCompilationErrors.unpivotRequiresAttributes("value", "id", values.flatten)
          // given values must not be empty seq
          case up @Unpivot(Some(ids), Some(Seq()), _, _, _, _)
            if up.childrenResolved && ids.forall(_.resolved) =>
            throw QueryCompilationErrors.unpivotRequiresValueColumns()
          // all values must have same length as there are value column names
          case up @Unpivot(Some(ids), Some(values), _, _, _, _)
            if up.childrenResolved && ids.forall(_.resolved) &&
              values.exists(_.length != up.valueColumnNames.length) =>
            throw QueryCompilationErrors.unpivotValueSizeMismatchError(up.valueColumnNames.length)
          // see TypeCoercionBase.UnpivotCoercion
          case up: Unpivot if up.canBeCoercioned && !up.valuesTypeCoercioned =>
            throw QueryCompilationErrors.unpivotValueDataTypeMismatchError(up.values.get)

          case Sort(orders, _, _, _) =>
            orders.foreach { order =>
              TypeUtils.tryThrowNotOrderableExpression(order)
            }

          case Window(_, partitionSpec, _, _, _) =>
            // Both `partitionSpec` and `orderSpec` must be orderable. We only need an extra check
            // for `partitionSpec` here because `orderSpec` has the type check itself.
            partitionSpec.foreach { p =>
              TypeUtils.tryThrowNotOrderableExpression(p)
            }

          case GlobalLimit(limitExpr, _) => checkLimitLikeClause("limit", limitExpr)

          case LocalLimit(limitExpr, child) =>
            checkLimitLikeClause("limit", limitExpr)
            child match {
              case Offset(offsetExpr, _) =>
                val limit = limitExpr.eval().asInstanceOf[Int]
                val offset = offsetExpr.eval().asInstanceOf[Int]
                if (Int.MaxValue - limit < offset) {
                  child.failAnalysis(
                    errorClass = "SUM_OF_LIMIT_AND_OFFSET_EXCEEDS_MAX_INT",
                    messageParameters = Map(
                      "limit" -> limit.toString,
                      "offset" -> offset.toString))
                }
              case _ =>
            }

          case Offset(offsetExpr, _) => checkLimitLikeClause("offset", offsetExpr)

          case Tail(limitExpr, _) => checkLimitLikeClause("tail", limitExpr)

          case e @ (_: Union | _: SetOperation) if operator.children.length > 1 =>
            def dataTypes(plan: LogicalPlan): Seq[DataType] = plan.output.map(_.dataType)

            val ref = dataTypes(operator.children.head)
            operator.children.tail.zipWithIndex.foreach { case (child, ti) =>
              // Check the number of columns
              if (child.output.length != ref.length) {
                throw QueryCompilationErrors.numColumnsMismatch(
                  operator = operator.nodeName,
                  firstNumColumns = ref.length,
                  invalidOrdinalNum = ti + 1,
                  invalidNumColumns = child.output.length,
                  origin = operator.origin
                )
              }

              val dataTypesAreCompatibleFn =
                TypeCoercionValidation.getDataTypesAreCompatibleFn(operator)
              // Check if the data types match.
              dataTypes(child).zip(ref).zipWithIndex.foreach { case ((dt1, dt2), ci) =>
                // SPARK-18058: we shall not care about the nullability of columns
                if (!dataTypesAreCompatibleFn(dt1, dt2)) {
                  throw QueryCompilationErrors.incompatibleColumnTypeError(
                    operator = operator.nodeName,
                    columnOrdinalNumber = ci,
                    tableOrdinalNumber = ti + 1,
                    dataType1 = dt1,
                    dataType2 = dt2,
                    hint = TypeCoercionValidation.getHintForOperatorCoercion(operator),
                    origin = operator.origin
                  )
                }
              }
            }

          case create: V2CreateTablePlan =>
            val references = create.partitioning.flatMap(_.references).toSet
            val badReferences = references.map(_.fieldNames).flatMap { column =>
              create.tableSchema.findNestedField(column.toImmutableArraySeq) match {
                case Some(_) =>
                  None
                case _ =>
                  Some(column.quoted)
              }
            }

            if (badReferences.nonEmpty) {
              create.failAnalysis(
                errorClass = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
                messageParameters = Map(
                  "cols" -> badReferences.map(r => toSQLId(r)).mkString(", ")))
            }

            create.tableSchema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))
            SchemaUtils.checkIndeterminateCollationInSchema(create.tableSchema)

          case write: V2WriteCommand if write.resolved =>
            write.query.schema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))

          case alter: AlterTableCommand =>
            checkAlterTableCommand(alter)

          case c: CreateVariable
              if c.resolved && c.defaultExpr.child.containsPattern(PLAN_EXPRESSION) =>
            val ident = c.name.asInstanceOf[ResolvedIdentifier]
            val varName = toSQLId(
              (ident.catalog.name +: ident.identifier.namespace :+ ident.identifier.name)
                .toImmutableArraySeq)
            throw QueryCompilationErrors.defaultValuesMayNotContainSubQueryExpressions(
              "DECLARE VARIABLE",
              varName,
              c.defaultExpr.originalSQL)

          case c: Call if c.resolved && c.bound && c.checkArgTypes().isFailure =>
            c.checkArgTypes() match {
              case mismatch: TypeCheckResult.DataTypeMismatch =>
                c.dataTypeMismatch("CALL", mismatch)
              case _ =>
                throw SparkException.internalError("Invalid input for procedure")
            }

          case _ => // Falls back to the following checks
        }

        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty =>
            val missingAttributes = o.missingInput.map(attr => toSQLExpr(attr)).mkString(", ")
            val input = o.inputSet.map(attr => toSQLExpr(attr)).mkString(", ")

            val resolver = plan.conf.resolver
            val attrsWithSameName = o.missingInput.filter { missing =>
              o.inputSet.exists(input => resolver(missing.name, input.name))
            }

            if (attrsWithSameName.nonEmpty) {
              val sameNames = attrsWithSameName.map(attr => toSQLExpr(attr)).mkString(", ")
              o.failAnalysis(
                errorClass = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION",
                messageParameters = Map(
                  "missingAttributes" -> missingAttributes,
                  "input" -> input,
                  "operator" -> operator.simpleString(SQLConf.get.maxToStringFields),
                  "operation" -> sameNames
                ))
            } else {
              o.failAnalysis(
                errorClass = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
                messageParameters = Map(
                  "missingAttributes" -> missingAttributes,
                  "input" -> input,
                  "operator" -> operator.simpleString(SQLConf.get.maxToStringFields)
                ))
            }

          case p @ Project(projectList, _) =>
            checkForUnspecifiedWindow(projectList)

          case agg@Aggregate(_, aggregateExpressions, _, _) if
            PlanHelper.specialExpressionsInUnsupportedOperator(agg).isEmpty =>
            checkForUnspecifiedWindow(aggregateExpressions)

          case j: Join if !j.duplicateResolved =>
            val conflictingAttributes =
              j.left.outputSet.intersect(j.right.outputSet).map(toSQLExpr(_)).mkString(", ")
            throw SparkException.internalError(
              msg = s"""
                       |Failure when resolving conflicting references in ${j.nodeName}:
                       |${planToString(plan)}
                       |Conflicting attributes: $conflictingAttributes.""".stripMargin,
              context = j.origin.getQueryContext,
              summary = j.origin.context.summary)

          case i: Intersect if !i.duplicateResolved =>
            val conflictingAttributes =
              i.left.outputSet.intersect(i.right.outputSet).map(toSQLExpr(_)).mkString(", ")
            throw SparkException.internalError(
              msg = s"""
                       |Failure when resolving conflicting references in ${i.nodeName}:
                       |${planToString(plan)}
                       |Conflicting attributes: $conflictingAttributes.""".stripMargin,
              context = i.origin.getQueryContext,
              summary = i.origin.context.summary)

          case e: Except if !e.duplicateResolved =>
            val conflictingAttributes =
              e.left.outputSet.intersect(e.right.outputSet).map(toSQLExpr(_)).mkString(", ")
            throw SparkException.internalError(
              msg = s"""
                       |Failure when resolving conflicting references in ${e.nodeName}:
                       |${planToString(plan)}
                       |Conflicting attributes: $conflictingAttributes.""".stripMargin,
              context = e.origin.getQueryContext,
              summary = e.origin.context.summary)

          case j: AsOfJoin if !j.duplicateResolved =>
            val conflictingAttributes =
              j.left.outputSet.intersect(j.right.outputSet).map(toSQLExpr(_)).mkString(", ")
            throw SparkException.internalError(
              msg = s"""
                       |Failure when resolving conflicting references in ${j.nodeName}:
                       |${planToString(plan)}
                       |Conflicting attributes: $conflictingAttributes.""".stripMargin,
              context = j.origin.getQueryContext,
              summary = j.origin.context.summary)

          // TODO: although map type is not orderable, technically map type should be able to be
          // used in equality comparison, remove this type check once we support it.
          case o if mapColumnInSetOperation(o).isDefined =>
            val mapCol = mapColumnInSetOperation(o).get
            throw QueryCompilationErrors.unsupportedSetOperationOnMapType(
              mapCol = mapCol,
              origin = operator.origin
            )

          // TODO: Remove this type check once we support Variant ordering
          case o if variantColumnInSetOperation(o).isDefined =>
            val variantCol = variantColumnInSetOperation(o).get
            throw QueryCompilationErrors.unsupportedSetOperationOnVariantType(
              variantCol = variantCol,
              origin = operator.origin
            )

          case o if variantExprInPartitionExpression(o).isDefined =>
            val variantExpr = variantExprInPartitionExpression(o).get
            o.failAnalysis(
              errorClass = "UNSUPPORTED_FEATURE.PARTITION_BY_VARIANT",
              messageParameters = Map(
                "expr" -> toSQLExpr(variantExpr),
                "dataType" -> toSQLType(variantExpr.dataType)))

          case o if o.expressions.exists(!_.deterministic) &&
            !operatorAllowsNonDeterministicExpressions(o) &&
            !o.isInstanceOf[Project] &&
            // non-deterministic expressions inside CollectMetrics have been
            // already validated inside checkMetric function
            !o.isInstanceOf[CollectMetrics] &&
            !o.isInstanceOf[Filter] &&
            !o.isInstanceOf[Aggregate] &&
            !o.isInstanceOf[Window] &&
            !o.isInstanceOf[Expand] &&
            !o.isInstanceOf[Generate] &&
            !o.isInstanceOf[CreateVariable] &&
            !o.isInstanceOf[MapInPandas] &&
            !o.isInstanceOf[MapInArrow] &&
            // Lateral join is checked in checkSubqueryExpression.
            !o.isInstanceOf[LateralJoin] =>
            // The rule above is used to check Aggregate operator.
            o.failAnalysis(
              errorClass = "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
              messageParameters = Map("sqlExprs" -> o.expressions.map(toSQLExpr(_)).mkString(", "))
            )

          case _: UnresolvedHint => throw SparkException.internalError(
            "Logical hint operator should be removed during analysis.")

          case f @ Filter(condition, _)
            if PlanHelper.specialExpressionsInUnsupportedOperator(f).nonEmpty =>
            val invalidExprSqls = PlanHelper.specialExpressionsInUnsupportedOperator(f).map(_.sql)
            f.failAnalysis(
              errorClass = "INVALID_WHERE_CONDITION",
              messageParameters = Map(
                "condition" -> toSQLExpr(condition),
                "expressionList" -> invalidExprSqls.mkString(", ")))

          case other if PlanHelper.specialExpressionsInUnsupportedOperator(other).nonEmpty =>
            val invalidExprSqls =
              PlanHelper.specialExpressionsInUnsupportedOperator(other).map(toSQLExpr)
            other.failAnalysis(
              errorClass = "UNSUPPORTED_EXPR_FOR_OPERATOR",
              messageParameters = Map(
                "invalidExprSqls" -> invalidExprSqls.mkString(", ")))

          case _ => // Analysis successful!
        }
    }
    checkCollectedMetrics(plan)
    extendedCheckRules.foreach(_(plan))
    plan.foreachUp {
      case o if !o.resolved =>
        throw SparkException.internalError(
          msg = s"Found the unresolved operator: ${o.simpleString(SQLConf.get.maxToStringFields)}",
          context = o.origin.getQueryContext,
          summary = o.origin.context.summary)
      // If the plan is resolved, all lateral column alias references should have been either
      // restored or resolved. Add check for extra safe.
      case o if o.expressions.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
        checkNotContainingLCA(o.expressions, o)
      case _ =>
    }
  }

  private def getAllExpressions(plan: LogicalPlan): Seq[Expression] = {
    plan match {
      // We only resolve `groupingExpressions` if `aggregateExpressions` is resolved first (See
      // `ResolveReferencesInAggregate`). We should check errors in `aggregateExpressions` first.
      case a: Aggregate => a.aggregateExpressions ++ a.groupingExpressions
      case _ => plan.expressions
    }
  }

  def checkSubqueryExpression(plan: LogicalPlan, expr: SubqueryExpression): Unit = {
    if (expr.plan.isStreaming) {
      plan.failAnalysis("INVALID_SUBQUERY_EXPRESSION.STREAMING_QUERY", Map.empty)
    }
    assertNoRecursiveCTE(expr.plan)
    checkAnalysis0(expr.plan)
    ValidateSubqueryExpression(plan, expr)
  }

  private def assertNoRecursiveCTE(plan: LogicalPlan): Unit = {
    plan.foreach {
      case r: CTERelationRef if r.recursive =>
        throw new AnalysisException(
          errorClass = "INVALID_RECURSIVE_REFERENCE.PLACE",
          messageParameters = Map.empty)
      case p => p.expressions.filter(_.containsPattern(PLAN_EXPRESSION)).foreach {
        expr => expr.foreach {
          case s: SubqueryExpression => assertNoRecursiveCTE(s.plan)
          case _ =>
        }
      }
    }
  }

  /**
   * Validate that collected metrics names are unique. The same name cannot be used for metrics
   * with different results. However multiple instances of metrics with with same result and name
   * are allowed (e.g. self-joins).
   */
  private def checkCollectedMetrics(plan: LogicalPlan): Unit = {
    val metricsMap = mutable.Map.empty[String, CollectMetrics]
    def check(plan: LogicalPlan): Unit = plan.foreach { node =>
      node match {
        case metrics @ CollectMetrics(name, _, _, dataframeId) =>
          metricsMap.get(name) match {
            case Some(other) =>
              // Exact duplicates are allowed. They can be the result
              // of a CTE that is used multiple times or a self join.
              if (dataframeId != other.dataframeId) {
                failAnalysis(
                  errorClass = "DUPLICATED_METRICS_NAME",
                  messageParameters = Map("metricName" -> name))
              }
            case None =>
              metricsMap.put(name, metrics)
          }
        case _ =>
      }
      node.expressions.foreach(_.foreach {
        case subquery: SubqueryExpression =>
          check(subquery.plan)
        case _ =>
      })
    }
    check(plan)
  }

  /**
   * Validates the options used for alter table commands after table and columns are resolved.
   */
  private def checkAlterTableCommand(alter: AlterTableCommand): Unit = {
    def checkColumnNotExists(op: String, fieldNames: Seq[String], struct: StructType): Unit = {
      if (struct.findNestedField(
          fieldNames, includeCollections = true, alter.conf.resolver).isDefined) {
        alter.failAnalysis(
          errorClass = "FIELD_ALREADY_EXISTS",
          messageParameters = Map(
            "op" -> op,
            "fieldNames" -> toSQLId(fieldNames),
            "struct" -> toSQLType(struct)))
      }
    }

    def checkColumnNameDuplication(colsToAdd: Seq[QualifiedColType]): Unit = {
      SchemaUtils.checkColumnNameDuplication(
        colsToAdd.map(_.name.quoted),
        alter.conf.resolver)
    }

    def checkNoCollationsInMapKeys(colsToAdd: Seq[QualifiedColType]): Unit = {
      if (!alter.conf.allowCollationsInMapKeys) {
        colsToAdd.foreach(col => SchemaUtils.checkNoCollationsInMapKeys(col.dataType))
      }
    }

    alter match {
      case AddColumns(table: ResolvedTable, colsToAdd) =>
        colsToAdd.foreach { colToAdd =>
          checkColumnNotExists("add", colToAdd.name, table.schema)
        }
        checkColumnNameDuplication(colsToAdd)
        checkNoCollationsInMapKeys(colsToAdd)

      case ReplaceColumns(_: ResolvedTable, colsToAdd) =>
        checkColumnNameDuplication(colsToAdd)
        checkNoCollationsInMapKeys(colsToAdd)

      case RenameColumn(table: ResolvedTable, col: ResolvedFieldName, newName) =>
        checkColumnNotExists("rename", col.path :+ newName, table.schema)

      case AddConstraint(_: ResolvedTable, check: CheckConstraint) if !check.deterministic =>
        check.child.failAnalysis(
          errorClass = "NON_DETERMINISTIC_CHECK_CONSTRAINT",
          messageParameters = Map("checkCondition" -> check.condition)
        )

      case AlterColumns(table: ResolvedTable, specs) =>
        val groupedColumns = specs.groupBy(_.column.name)
        groupedColumns.collect {
          case (name, occurrences) if occurrences.length > 1 =>
            alter.failAnalysis(
              errorClass = "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
              messageParameters = Map(
                "table" -> toSQLId(table.name),
                "fieldName" -> toSQLId(name)))
        }
        groupedColumns.keys.foreach { name =>
          if (groupedColumns.keys.exists(child => child != name && child.startsWith(name))) {
            alter.failAnalysis(
              errorClass = "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
              messageParameters = Map(
                "table" -> toSQLId(table.name),
                "fieldName" -> toSQLId(name)))
          }
        }
        specs.foreach {
          case AlterColumnSpec(col: ResolvedFieldName, dataType, nullable, _, _, _) =>
            val fieldName = col.name.quoted
            if (dataType.isDefined) {
              val field = CharVarcharUtils.getRawType(col.field.metadata)
                .map(dt => col.field.copy(dataType = dt))
                .getOrElse(col.field)
              val newDataType = dataType.get
              newDataType match {
                case _: StructType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.STRUCT_TYPE",
                  Map("table" -> toSQLId(table.name), "fieldName" -> toSQLId(fieldName)))
                case _: MapType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.MAP_TYPE",
                  Map("table" -> toSQLId(table.name), "fieldName" -> toSQLId(fieldName)))
                case _: ArrayType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.ARRAY_TYPE",
                  Map("table" -> toSQLId(table.name), "fieldName" -> toSQLId(fieldName)))
                case u: UserDefinedType[_] => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.USER_DEFINED_TYPE",
                  Map(
                    "table" -> toSQLId(table.name),
                    "fieldName" -> toSQLId(fieldName),
                    "udtSql" -> toSQLType(u)))
                case _: CalendarIntervalType | _: AnsiIntervalType => alter.failAnalysis(
                  "CANNOT_UPDATE_FIELD.INTERVAL_TYPE",
                  Map("table" -> toSQLId(table.name), "fieldName" -> toSQLId(fieldName)))
                case _ => // update is okay
              }

              // We don't need to handle nested types here which shall fail before.
              def canAlterColumnType(from: DataType, to: DataType): Boolean = (from, to) match {
                case (CharType(l1), CharType(l2)) => l1 == l2
                case (CharType(l1), VarcharType(l2)) => l1 <= l2
                case (VarcharType(l1), VarcharType(l2)) => l1 <= l2
                case _ => Cast.canUpCast(from, to)
              }
              if (!canAlterColumnType(field.dataType, newDataType)) {
                alter.failAnalysis(
                  errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
                  messageParameters = Map(
                    "table" -> toSQLId(table.name),
                    "originName" -> toSQLId(fieldName),
                    "originType" -> toSQLType(field.dataType),
                    "newName" -> toSQLId(fieldName),
                    "newType" -> toSQLType(newDataType)))
              }
            }
            if (nullable.isDefined) {
              if (!nullable.get && col.field.nullable) {
                alter.failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2330",
                  messageParameters = Map("fieldName" -> fieldName))
              }
            }
          case _ =>
        }
      case _ =>
    }
  }

  private def unresolvedInlineTableContainsScalarSubquery(
      unresolvedInlineTable: UnresolvedInlineTable) = {
    unresolvedInlineTable.rows.exists { row =>
      row.exists { expression =>
        expression.exists(_.isInstanceOf[ScalarSubquery])
      }
    }
  }
}

// a heap of the preempted error that only keeps the top priority element, representing the sole
// error to be thrown at the end of the whole check analysis process, if no other error occurs.
class PreemptedError() {
  case class ErrorWithPriority(error: Exception with SparkThrowable, priority: Int) {}

  private var errorOpt: Option[ErrorWithPriority] = None

  // Set/overwrite the given error as the preempted error, if no other errors are preempted, or it
  // has a higher priority than the existing one.
  // If the priority is not provided, it will be calculated based on error class. Currently internal
  // errors have the lowest priority.
  def set(error: Exception with SparkThrowable, priority: Option[Int] = None): Unit = {
    val calculatedPriority = priority.getOrElse {
      error.getCondition match {
        case c if c.startsWith("INTERNAL_ERROR") => 1
        case _ => 2
      }
    }
    if (errorOpt.isEmpty || calculatedPriority > errorOpt.get.priority) {
      errorOpt = Some(ErrorWithPriority(error, calculatedPriority))
    }
  }

  def getErrorOpt(): Option[Exception with SparkThrowable] = errorOpt.map(_.error)

  def clear(): Unit = errorOpt = None
}
