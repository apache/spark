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

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Median, PercentileCont, PercentileDisc}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, DecorrelateInnerQuery, InlineCTE}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, PLAN_EXPRESSION, UNRESOLVED_WINDOW_EXPRESSION}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, StringUtils, TypeUtils}
import org.apache.spark.sql.connector.catalog.{LookupCatalog, SupportsPartitionManagement}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Throws user facing errors when passed invalid queries that fail to analyze.
 */
trait CheckAnalysis extends PredicateHelper with LookupCatalog with QueryErrorsBase with Logging {

  protected def isView(nameParts: Seq[String]): Boolean

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  val DATA_TYPE_MISMATCH_ERROR = TreeNodeTag[Unit]("dataTypeMismatchError")
  val INVALID_FORMAT_ERROR = TreeNodeTag[Unit]("invalidFormatError")

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

  protected def mapColumnInSetOperation(plan: LogicalPlan): Option[Attribute] = plan match {
    case _: Intersect | _: Except | _: Distinct =>
      plan.output.find(a => hasMapType(a.dataType))
    case d: Deduplicate =>
      d.keys.find(a => hasMapType(a.dataType))
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
        throw SparkException.internalError("Resolved plan should not contain any " +
          s"LateralColumnAliasReference.\nDebugging information: plan:\n$plan",
          context = lcaRef.origin.getQueryContext,
          summary = lcaRef.origin.context.summary)
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
    try {
      checkAnalysis0(inlinedPlan)
    } catch {
      case e: AnalysisException =>
        throw new ExtendedAnalysisException(e, inlinedPlan)
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

      case _ =>
    }

    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {
      case p if p.analyzed => // Skip already analyzed sub-plans

      case leaf: LeafNode if leaf.output.map(_.dataType).exists(CharVarcharUtils.hasCharVarchar) =>
        throw SparkException.internalError(
          "Logical plan should not have output of char/varchar type: " + leaf)

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
                hof.setTagValue(INVALID_FORMAT_ERROR, ())
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

          case e: Expression if e.checkInputDataTypes().isFailure =>
            e.checkInputDataTypes() match {
              case checkRes: TypeCheckResult.DataTypeMismatch =>
                e.setTagValue(DATA_TYPE_MISMATCH_ERROR, ())
                e.dataTypeMismatch(e, checkRes)
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.setTagValue(DATA_TYPE_MISMATCH_ERROR, ())
                val extraHint = extraHintForAnsiTypeCoercionExpression(operator)
                e.failAnalysis(
                  errorClass = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
                  messageParameters = Map(
                    "sqlExpr" -> toSQLExpr(e),
                    "msg" -> message,
                    "hint" -> extraHint))
              case checkRes: TypeCheckResult.InvalidFormat =>
                e.setTagValue(INVALID_FORMAT_ERROR, ())
                e.invalidFormat(checkRes)
            }

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

          case w: WindowExpression =>
            // Only allow window functions with an aggregate expression or an offset window
            // function or a Pandas window UDF.
            w.windowFunction match {
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

          case Sort(orders, _, _) =>
            orders.foreach { order =>
              if (!RowOrdering.isOrderable(order.dataType)) {
                order.failAnalysis(
                  errorClass = "EXPRESSION_TYPE_IS_NOT_ORDERABLE",
                  messageParameters = Map("exprType" -> toSQLType(order.dataType)))
              }
            }

          case Window(_, partitionSpec, _, _) =>
            // Both `partitionSpec` and `orderSpec` must be orderable. We only need an extra check
            // for `partitionSpec` here because `orderSpec` has the type check itself.
            partitionSpec.foreach { p =>
              if (!RowOrdering.isOrderable(p.dataType)) {
                p.failAnalysis(
                  errorClass = "EXPRESSION_TYPE_IS_NOT_ORDERABLE",
                  messageParameters = Map(
                    "expr" -> toSQLExpr(p),
                    "exprType" -> toSQLType(p.dataType)))
              }
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
                e.failAnalysis(
                  errorClass = "NUM_COLUMNS_MISMATCH",
                  messageParameters = Map(
                    "operator" -> toSQLStmt(operator.nodeName),
                    "firstNumColumns" -> ref.length.toString,
                    "invalidOrdinalNum" -> ordinalNumber(ti + 1),
                    "invalidNumColumns" -> child.output.length.toString))
              }

              val dataTypesAreCompatibleFn = getDataTypesAreCompatibleFn(operator)
              // Check if the data types match.
              dataTypes(child).zip(ref).zipWithIndex.foreach { case ((dt1, dt2), ci) =>
                // SPARK-18058: we shall not care about the nullability of columns
                if (!dataTypesAreCompatibleFn(dt1, dt2)) {
                  e.failAnalysis(
                    errorClass = "INCOMPATIBLE_COLUMN_TYPE",
                    messageParameters = Map(
                      "operator" -> toSQLStmt(operator.nodeName),
                      "columnOrdinalNumber" -> ordinalNumber(ci),
                      "tableOrdinalNumber" -> ordinalNumber(ti + 1),
                      "dataType1" -> toSQLType(dt1),
                      "dataType2" -> toSQLType(dt2),
                      "hint" -> extraHintForAnsiTypeCoercionPlan(operator)))
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

          case agg@Aggregate(_, aggregateExpressions, _) if
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
            o.failAnalysis(
              errorClass = "UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE",
              messageParameters = Map(
                "colName" -> toSQLId(mapCol.name),
                "dataType" -> toSQLType(mapCol.dataType)))

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

  private def getDataTypesAreCompatibleFn(plan: LogicalPlan): (DataType, DataType) => Boolean = {
    val isUnion = plan.isInstanceOf[Union]
    if (isUnion) {
      (dt1: DataType, dt2: DataType) =>
        DataType.equalsStructurally(dt1, dt2, true)
    } else {
      // SPARK-18058: we shall not care about the nullability of columns
      (dt1: DataType, dt2: DataType) =>
        TypeCoercion.findWiderTypeForTwo(dt1.asNullable, dt2.asNullable).nonEmpty
    }
  }

  private def getDefaultTypeCoercionPlan(plan: LogicalPlan): LogicalPlan =
    TypeCoercion.typeCoercionRules.foldLeft(plan) { case (p, rule) => rule(p) }

  private def extraHintMessage(issueFixedIfAnsiOff: Boolean): String = {
    if (issueFixedIfAnsiOff) {
      "\nTo fix the error, you might need to add explicit type casts. If necessary set " +
        s"${SQLConf.ANSI_ENABLED.key} to false to bypass this error."
    } else {
      ""
    }
  }

  private[analysis] def extraHintForAnsiTypeCoercionExpression(plan: LogicalPlan): String = {
    if (!SQLConf.get.ansiEnabled) {
      ""
    } else {
      val nonAnsiPlan = getDefaultTypeCoercionPlan(plan)
      var issueFixedIfAnsiOff = true
      getAllExpressions(nonAnsiPlan).foreach(_.foreachUp {
        case e: Expression if e.getTagValue(DATA_TYPE_MISMATCH_ERROR).isDefined &&
            e.checkInputDataTypes().isFailure =>
          e.checkInputDataTypes() match {
            case TypeCheckResult.TypeCheckFailure(_) | _: TypeCheckResult.DataTypeMismatch =>
              issueFixedIfAnsiOff = false
          }

        case _ =>
      })
      extraHintMessage(issueFixedIfAnsiOff)
    }
  }

  private def extraHintForAnsiTypeCoercionPlan(plan: LogicalPlan): String = {
    if (!SQLConf.get.ansiEnabled) {
      ""
    } else {
      val nonAnsiPlan = getDefaultTypeCoercionPlan(plan)
      var issueFixedIfAnsiOff = true
      nonAnsiPlan match {
        case _: Union | _: SetOperation if nonAnsiPlan.children.length > 1 =>
          def dataTypes(plan: LogicalPlan): Seq[DataType] = plan.output.map(_.dataType)

          val ref = dataTypes(nonAnsiPlan.children.head)
          val dataTypesAreCompatibleFn = getDataTypesAreCompatibleFn(nonAnsiPlan)
          nonAnsiPlan.children.tail.zipWithIndex.foreach { case (child, ti) =>
            // Check if the data types match.
            dataTypes(child).zip(ref).zipWithIndex.foreach { case ((dt1, dt2), ci) =>
              if (!dataTypesAreCompatibleFn(dt1, dt2)) {
                issueFixedIfAnsiOff = false
              }
            }
          }

        case _ =>
      }
      extraHintMessage(issueFixedIfAnsiOff)
    }
  }

  private def scrubOutIds(string: String): String =
    string.replaceAll("#\\d+", "#x")
      .replaceAll("operator id = \\d+", "operator id = #x")
      .replaceAll("rand\\(-?\\d+\\)", "rand(number)")

  private def planToString(plan: LogicalPlan): String = {
    if (Utils.isTesting) scrubOutIds(plan.toString) else plan.toString
  }

  private def exprsToString(exprs: Seq[Expression]): String = {
    val result = exprs.map(_.toString).mkString("\n")
    if (Utils.isTesting) scrubOutIds(result) else result
  }

  /**
   * Validates subquery expressions in the plan. Upon failure, returns an user facing error.
   */
  def checkSubqueryExpression(plan: LogicalPlan, expr: SubqueryExpression): Unit = {
    def checkAggregateInScalarSubquery(
        conditions: Seq[Expression],
        query: LogicalPlan, agg: Aggregate): Unit = {
      // Make sure correlated scalar subqueries contain one row for every outer row by
      // enforcing that they are aggregates containing exactly one aggregate expression.
      val aggregates = agg.expressions.flatMap(_.collect {
        case a: AggregateExpression => a
      })
      if (aggregates.isEmpty) {
        expr.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
          messageParameters = Map.empty)
      }

      // SPARK-18504/SPARK-18814: Block cases where GROUP BY columns
      // are not part of the correlated columns.

      // Collect the inner query expressions that are guaranteed to have a single value for each
      // outer row. See comment on getCorrelatedEquivalentInnerExpressions.
      val correlatedEquivalentExprs = getCorrelatedEquivalentInnerExpressions(query)
      // Grouping expressions, except outer refs and constant expressions - grouping by an
      // outer ref or a constant is always ok
      val groupByExprs =
        ExpressionSet(agg.groupingExpressions.filter(x => !x.isInstanceOf[OuterReference] &&
          x.references.nonEmpty))
      val nonEquivalentGroupByExprs = groupByExprs -- correlatedEquivalentExprs

      val invalidCols = if (!SQLConf.get.getConf(
        SQLConf.LEGACY_SCALAR_SUBQUERY_ALLOW_GROUP_BY_NON_EQUALITY_CORRELATED_PREDICATE)) {
        nonEquivalentGroupByExprs
      } else {
        // Legacy incorrect logic for checking for invalid group-by columns (see SPARK-48503).
        // Allows any inner attribute that appears in a correlated predicate, even if it is a
        // non-equality predicate or under an operator that can change the values of the attribute
        // (see comments on getCorrelatedEquivalentInnerColumns for examples).
        // Note: groupByCols does not contain outer refs - grouping by an outer ref is always ok
        val groupByCols = AttributeSet(agg.groupingExpressions.flatMap(_.references))
        val subqueryColumns = getCorrelatedPredicates(query).flatMap(_.references)
          .filterNot(conditions.flatMap(_.references).contains)
        val correlatedCols = AttributeSet(subqueryColumns)
        val invalidColsLegacy = groupByCols -- correlatedCols
        if (!nonEquivalentGroupByExprs.isEmpty && invalidColsLegacy.isEmpty) {
          logWarning(log"Using legacy behavior for " +
            log"${MDC(LogKeys.CONFIG, SQLConf
            .LEGACY_SCALAR_SUBQUERY_ALLOW_GROUP_BY_NON_EQUALITY_CORRELATED_PREDICATE.key)}. " +
            log"Query would be rejected with non-legacy behavior but is allowed by " +
            log"legacy behavior. Query may be invalid and return wrong results if the scalar " +
            log"subquery's group-by outputs multiple rows.")
        }
        invalidColsLegacy
      }

      if (invalidCols.nonEmpty) {
        val names = invalidCols.map { el =>
          el match {
            case attr: Attribute => attr.name
            case expr: Expression => expr.toString
          }
        }
        expr.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "NON_CORRELATED_COLUMNS_IN_GROUP_BY",
          messageParameters = Map("value" -> names.mkString(",")))
      }
    }

    // Skip subquery aliases added by the Analyzer as well as hints.
    // For projects, do the necessary mapping and skip to its child.
    @scala.annotation.tailrec
    def cleanQueryInScalarSubquery(p: LogicalPlan): LogicalPlan = p match {
      case s: SubqueryAlias => cleanQueryInScalarSubquery(s.child)
      case p: Project => cleanQueryInScalarSubquery(p.child)
      case h: ResolvedHint => cleanQueryInScalarSubquery(h.child)
      case child => child
    }

    // Check whether the given expressions contains the subquery expression.
    def containsExpr(expressions: Seq[Expression]): Boolean = {
      expressions.exists(_.exists(_.semanticEquals(expr)))
    }

    def checkOuterReference(p: LogicalPlan, expr: SubqueryExpression): Unit = p match {
      case f: Filter =>
        if (hasOuterReferences(expr.plan)) {
          expr.plan.expressions.foreach(_.foreachUp {
            case o: OuterReference =>
              p.children.foreach(e =>
                if (!e.output.exists(_.exprId == o.exprId)) {
                  o.failAnalysis(
                    errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                      "CORRELATED_COLUMN_NOT_FOUND",
                    messageParameters = Map("value" -> o.name))
                })
            case _ =>
          })
        }
      case _ =>
    }

    // Validate the subquery plan.
    checkAnalysis0(expr.plan)

    // Check if there is outer attribute that cannot be found from the plan.
    checkOuterReference(plan, expr)

    expr match {
      case ScalarSubquery(query, outerAttrs, _, _, _, _) =>
        // Scalar subquery must return one column as output.
        if (query.output.size != 1) {
          throw QueryCompilationErrors.subqueryReturnMoreThanOneColumn(query.output.size,
            expr.origin)
        }

        if (outerAttrs.nonEmpty) {
          cleanQueryInScalarSubquery(query) match {
            case a: Aggregate => checkAggregateInScalarSubquery(outerAttrs, query, a)
            case Filter(_, a: Aggregate) => checkAggregateInScalarSubquery(outerAttrs, query, a)
            case p: LogicalPlan if p.maxRows.exists(_ <= 1) => // Ok
            case other =>
              expr.failAnalysis(
                errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                  "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
                messageParameters = Map.empty)
          }

          // Only certain operators are allowed to host subquery expression containing
          // outer references.
          plan match {
            case _: Filter | _: Project | _: SupportsSubquery => // Ok
            case a: Aggregate =>
              // If the correlated scalar subquery is in the grouping expressions of an Aggregate,
              // it must also be in the aggregate expressions to be rewritten in the optimization
              // phase.
              if (containsExpr(a.groupingExpressions) && !containsExpr(a.aggregateExpressions)) {
                a.failAnalysis(
                  errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                    "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
                  messageParameters = Map.empty)
              }
            case other =>
              other.failAnalysis(
                errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                  "UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY",
                messageParameters = Map("treeNode" -> planToString(other)))
          }
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan, isScalar = true)

      case _: LateralSubquery =>
        assert(plan.isInstanceOf[LateralJoin])
        val join = plan.asInstanceOf[LateralJoin]
        // A lateral join with a multi-row outer query and a non-deterministic lateral subquery
        // cannot be decorrelated. Otherwise it may produce incorrect results.
        if (!expr.deterministic && !join.left.maxRows.exists(_ <= 1)) {
          cleanQueryInScalarSubquery(join.right.plan) match {
            // Python UDTFs are by default non-deterministic. They are constructed as a
            // OneRowRelation subquery and can be rewritten by the optimizer without
            // any decorrelation.
            case Generate(_: PythonUDTF, _, _, _, _, _: OneRowRelation)
              if SQLConf.get.getConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY) =>  // Ok
            case _ =>
              expr.failAnalysis(
                errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                  "NON_DETERMINISTIC_LATERAL_SUBQUERIES",
                messageParameters = Map("treeNode" -> planToString(plan)))
          }
        }
        // Check if the lateral join's join condition is deterministic.
        if (join.condition.exists(!_.deterministic)) {
          join.condition.get.failAnalysis(
            errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
              "LATERAL_JOIN_CONDITION_NON_DETERMINISTIC",
            messageParameters = Map("condition" -> join.condition.get.sql))
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan, isLateral = true)

      case _: FunctionTableSubqueryArgumentExpression =>
        expr.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_TABLE_ARGUMENT",
          messageParameters = Map("treeNode" -> planToString(plan)))

      case inSubqueryOrExistsSubquery =>
        plan match {
          case _: Filter | _: SupportsSubquery | _: Join |
            _: Project | _: Aggregate | _: Window => // Ok
          case _ =>
            expr.failAnalysis(
              errorClass =
                "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY",
              messageParameters = Map("treeNode" -> planToString(plan)))
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan)
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
   * Validates to make sure the outer references appearing inside the subquery
   * are allowed.
   */
  private def checkCorrelationsInSubquery(
      sub: LogicalPlan,
      isScalar: Boolean = false,
      isLateral: Boolean = false): Unit = {
    // Some query shapes are only supported with the DecorrelateInnerQuery framework.
    // Support for Exists and IN subqueries is subject to a separate config flag
    // 'decorrelateInnerQueryEnabledForExistsIn'.
    val usingDecorrelateInnerQueryFramework =
      (SQLConf.get.decorrelateInnerQueryEnabledForExistsIn || isScalar || isLateral) &&
        SQLConf.get.decorrelateInnerQueryEnabled

    // Validate that correlated aggregate expression do not contain a mixture
    // of outer and local references.
    def checkMixedReferencesInsideAggregateExpr(expr: Expression): Unit = {
      expr.foreach {
        case a: AggregateExpression if containsOuter(a) =>
          if (a.references.nonEmpty) {
            a.failAnalysis(
              errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                "AGGREGATE_FUNCTION_MIXED_OUTER_LOCAL_REFERENCES",
              messageParameters = Map("function" -> a.sql))
          }
        case _ =>
      }
    }

    // Make sure expressions of a plan do not contain outer references.
    def failOnOuterReferenceInPlan(p: LogicalPlan): Unit = {
      if (p.expressions.exists(containsOuter)) {
        p.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED",
          messageParameters = Map("treeNode" -> planToString(p)))
      }
    }

    // Check whether the logical plan node can host outer references.
    // A `Project` can host outer references if it is inside a scalar or a lateral subquery and
    // DecorrelateInnerQuery is enabled. Otherwise, only Filter can only outer references.
    def canHostOuter(plan: LogicalPlan): Boolean = plan match {
      case _: Filter => true
      case _: Project => usingDecorrelateInnerQueryFramework
      case _: Join => usingDecorrelateInnerQueryFramework
      case _ => false
    }

    // Make sure a plan's expressions do not contain :
    // 1. Aggregate expressions that have mixture of outer and local references.
    // 2. Expressions containing outer references on plan nodes other than allowed operators.
    def failOnInvalidOuterReference(p: LogicalPlan): Unit = {
      p.expressions.foreach(checkMixedReferencesInsideAggregateExpr)
      val exprs = stripOuterReferences(p.expressions.filter(expr => containsOuter(expr)))
      if (!canHostOuter(p) && !exprs.isEmpty) {
        p.failAnalysis(
          errorClass =
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE",
          messageParameters = Map("sqlExprs" -> exprs.map(toSQLExpr).mkString(",")))
      }
    }

    // SPARK-17348: A potential incorrect result case.
    // When a correlated predicate is a non-equality predicate,
    // certain operators are not permitted from the operator
    // hosting the correlated predicate up to the operator on the outer table.
    // Otherwise, the pull up of the correlated predicate
    // will generate a plan with a different semantics
    // which could return incorrect result.
    // Currently we check for Aggregate and Window operators
    //
    // Below shows an example of a Logical Plan during Analyzer phase that
    // show this problem. Pulling the correlated predicate [outer(c2#77) >= ..]
    // through the Aggregate (or Window) operator could alter the result of
    // the Aggregate.
    //
    // Project [c1#76]
    // +- Project [c1#87, c2#88]
    // :  (Aggregate or Window operator)
    // :  +- Filter [outer(c2#77) >= c2#88)]
    // :     +- SubqueryAlias t2, `t2`
    // :        +- Project [_1#84 AS c1#87, _2#85 AS c2#88]
    // :           +- LocalRelation [_1#84, _2#85]
    // +- SubqueryAlias t1, `t1`
    // +- Project [_1#73 AS c1#76, _2#74 AS c2#77]
    // +- LocalRelation [_1#73, _2#74]
    // SPARK-35080: The same issue can happen to correlated equality predicates when
    // they do not guarantee one-to-one mapping between inner and outer attributes.
    // For example:
    // Table:
    //   t1(a, b): [(0, 6), (1, 5), (2, 4)]
    //   t2(c): [(6)]
    //
    // Query:
    //   SELECT c, (SELECT COUNT(*) FROM t1 WHERE a + b = c) FROM t2
    //
    // Original subquery plan:
    //   Aggregate [count(1)]
    //   +- Filter ((a + b) = outer(c))
    //      +- LocalRelation [a, b]
    //
    // Plan after pulling up correlated predicates:
    //   Aggregate [a, b] [count(1), a, b]
    //   +- LocalRelation [a, b]
    //
    // Plan after rewrite:
    //   Project [c1, count(1)]
    //   +- Join LeftOuter ((a + b) = c)
    //      :- LocalRelation [c]
    //      +- Aggregate [a, b] [count(1), a, b]
    //         +- LocalRelation [a, b]
    //
    // The right hand side of the join transformed from the subquery will output
    //   count(1) | a | b
    //      1     | 0 | 6
    //      1     | 1 | 5
    //      1     | 2 | 4
    // and the plan after rewrite will give the original query incorrect results.
    def failOnUnsupportedCorrelatedPredicate(predicates: Seq[Expression], p: LogicalPlan): Unit = {
      // Correlated non-equality predicates are only supported with the decorrelate
      // inner query framework. Currently we only use this new framework for scalar
      // and lateral subqueries.
      val allowNonEqualityPredicates = usingDecorrelateInnerQueryFramework
      if (!allowNonEqualityPredicates && predicates.nonEmpty) {
        // Report a non-supported case as an exception
        p.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "CORRELATED_COLUMN_IS_NOT_ALLOWED_IN_PREDICATE",
          messageParameters =
            Map("treeNode" -> s"${exprsToString(predicates)}\n${planToString(p)}"))
      }
    }

    // Recursively check invalid outer references in the plan.
    def checkPlan(
        plan: LogicalPlan,
        aggregated: Boolean = false,
        canContainOuter: Boolean = true): Unit = {

      if (!canContainOuter) {
        failOnOuterReferenceInPlan(plan)
      }

      // Approve operators allowed in a correlated subquery
      // There are 4 categories:
      // 1. Operators that are allowed anywhere in a correlated subquery, and,
      //    by definition of the operators, they either do not contain
      //    any columns or cannot host outer references.
      // 2. Operators that are allowed anywhere in a correlated subquery
      //    so long as they do not host outer references.
      // 3. Operators that need special handling. These operators are
      //    Filter, Join, Aggregate, and Generate.
      //
      // Any operators that are not in the above list are allowed
      // in a correlated subquery only if they are not on a correlation path.
      // In other word, these operators are allowed only under a correlation point.
      //
      // A correlation path is defined as the sub-tree of all the operators that
      // are on the path from the operator hosting the correlated expressions
      // up to the operator producing the correlated values.
      plan match {
        // Category 1:
        // ResolvedHint, LeafNode, Repartition, and SubqueryAlias
        case p @ (_: ResolvedHint | _: LeafNode | _: Repartition | _: SubqueryAlias) =>
          p.children.foreach(child => checkPlan(child, aggregated, canContainOuter))

        case p @ (_ : Union | _: SetOperation) =>
          // Set operations (e.g. UNION) containing correlated values are only supported
          // with DecorrelateInnerQuery framework.
          val childCanContainOuter = (canContainOuter
            && usingDecorrelateInnerQueryFramework
            && SQLConf.get.getConf(SQLConf.DECORRELATE_SET_OPS_ENABLED))
          p.children.foreach(child => checkPlan(child, aggregated, childCanContainOuter))

        // Category 2:
        // These operators can be anywhere in a correlated subquery.
        // so long as they do not host outer references in the operators.
        case p: Project =>
          failOnInvalidOuterReference(p)
          checkPlan(p.child, aggregated, canContainOuter)

        case s: Sort =>
          failOnInvalidOuterReference(s)
          checkPlan(s.child, aggregated, canContainOuter)

        case r: RepartitionByExpression =>
          failOnInvalidOuterReference(r)
          checkPlan(r.child, aggregated, canContainOuter)

        case l: LateralJoin =>
          failOnInvalidOuterReference(l)
          checkPlan(l.child, aggregated, canContainOuter)

        // Category 3:
        // Filter is one of the two operators allowed to host correlated expressions.
        // The other operator is Join. Filter can be anywhere in a correlated subquery.
        case f: Filter =>
          failOnInvalidOuterReference(f)
          val (correlated, _) = splitConjunctivePredicates(f.condition).partition(containsOuter)
          val unsupportedPredicates = correlated.filterNot(DecorrelateInnerQuery.canPullUpOverAgg)
          if (aggregated) {
            failOnUnsupportedCorrelatedPredicate(unsupportedPredicates, f)
          }
          checkPlan(f.child, aggregated, canContainOuter)

        // Aggregate cannot host any correlated expressions
        // It can be on a correlation path if the correlation contains
        // only supported correlated equality predicates.
        // It cannot be on a correlation path if the correlation has
        // non-equality correlated predicates.
        case a: Aggregate =>
          failOnInvalidOuterReference(a)
          checkPlan(a.child, aggregated = true, canContainOuter)

        // Same as Aggregate above.
        case w: Window =>
          failOnInvalidOuterReference(w)
          checkPlan(w.child, aggregated = true, canContainOuter)

        // Distinct does not host any correlated expressions, but during the optimization phase
        // it will be rewritten as Aggregate, which can only be on a correlation path if the
        // correlation contains only the supported correlated equality predicates.
        // Only block it for lateral subqueries because scalar subqueries must be aggregated
        // and it does not impact the results for IN/EXISTS subqueries.
        case d: Distinct =>
          checkPlan(d.child, aggregated = isLateral, canContainOuter)

        // Join can host correlated expressions.
        case j @ Join(left, right, joinType, _, _) =>
          failOnInvalidOuterReference(j)
          joinType match {
            // Inner join, like Filter, can be anywhere.
            case _: InnerLike =>
              j.children.foreach(child => checkPlan(child, aggregated, canContainOuter))

            // Left outer join's right operand cannot be on a correlation path.
            // LeftAnti and ExistenceJoin are special cases of LeftOuter.
            // Note that ExistenceJoin cannot be expressed externally in both SQL and DataFrame
            // so it should not show up here in Analysis phase. This is just a safety net.
            //
            // LeftSemi does not allow output from the right operand.
            // Any correlated references in the subplan
            // of the right operand cannot be pulled up.
            case LeftOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
              checkPlan(left, aggregated, canContainOuter)
              checkPlan(right, aggregated, canContainOuter = false)

            // Likewise, Right outer join's left operand cannot be on a correlation path.
            case RightOuter =>
              checkPlan(left, aggregated, canContainOuter = false)
              checkPlan(right, aggregated, canContainOuter)

            // Any other join types not explicitly listed above,
            // including Full outer join, are treated as Category 4.
            case _ =>
              j.children.foreach(child => checkPlan(child, aggregated, canContainOuter = false))
          }

        // Generator with join=true, i.e., expressed with
        // LATERAL VIEW [OUTER], similar to inner join,
        // allows to have correlation under it
        // but must not host any outer references.
        // Note:
        // Generator with requiredChildOutput.isEmpty is treated as Category 4.
        case g: Generate if g.requiredChildOutput.nonEmpty =>
          failOnInvalidOuterReference(g)
          checkPlan(g.child, aggregated, canContainOuter)

        // Correlated subquery can have a LIMIT clause
        case l @ Limit(_, input) =>
          failOnInvalidOuterReference(l)
          checkPlan(
            input,
            aggregated,
            canContainOuter && SQLConf.get.getConf(SQLConf.DECORRELATE_LIMIT_ENABLED))

        case o @ Offset(_, input) =>
          failOnInvalidOuterReference(o)
          checkPlan(
            input,
            aggregated,
            canContainOuter && SQLConf.get.getConf(SQLConf.DECORRELATE_OFFSET_ENABLED))

        // We always inline CTE relations before analysis check, and only un-referenced CTE
        // relations will be kept in the plan. Here we should simply skip them and check the
        // children, as un-referenced CTE relations won't be executed anyway and doesn't need to
        // be restricted by the current subquery correlation limitations.
        case _: WithCTE | _: CTERelationDef =>
          plan.children.foreach(p => checkPlan(p, aggregated, canContainOuter))

        // Category 4: Any other operators not in the above 3 categories
        // cannot be on a correlation path, that is they are allowed only
        // under a correlation point but they and their descendant operators
        // are not allowed to have any correlated expressions.
        case p =>
          p.children.foreach(p => checkPlan(p, aggregated, canContainOuter = false))
      }
    }

    // Simplify the predicates before validating any unsupported correlation patterns in the plan.
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      checkPlan(BooleanSimplification(sub))
    }
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

    alter match {
      case AddColumns(table: ResolvedTable, colsToAdd) =>
        colsToAdd.foreach { colToAdd =>
          checkColumnNotExists("add", colToAdd.name, table.schema)
        }
        checkColumnNameDuplication(colsToAdd)

      case ReplaceColumns(_: ResolvedTable, colsToAdd) =>
        checkColumnNameDuplication(colsToAdd)

      case RenameColumn(table: ResolvedTable, col: ResolvedFieldName, newName) =>
        checkColumnNotExists("rename", col.path :+ newName, table.schema)

      case a @ AlterColumn(table: ResolvedTable, col: ResolvedFieldName, _, _, _, _, _) =>
        val fieldName = col.name.quoted
        if (a.dataType.isDefined) {
          val field = CharVarcharUtils.getRawType(col.field.metadata)
            .map(dt => col.field.copy(dataType = dt))
            .getOrElse(col.field)
          val newDataType = a.dataType.get
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
        if (a.nullable.isDefined) {
          if (!a.nullable.get && col.field.nullable) {
            alter.failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2330",
              messageParameters = Map("fieldName" -> fieldName))
          }
        }
      case _ =>
    }
  }
}
