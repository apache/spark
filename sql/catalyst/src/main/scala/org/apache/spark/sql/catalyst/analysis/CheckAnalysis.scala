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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Median, PercentileCont, PercentileDisc}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, DecorrelateInnerQuery, InlineCTE}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_WINDOW_EXPRESSION
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, StringUtils, TypeUtils}
import org.apache.spark.sql.connector.catalog.{LookupCatalog, SupportsPartitionManagement}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.Utils

/**
 * Throws user facing errors when passed invalid queries that fail to analyze.
 */
trait CheckAnalysis extends PredicateHelper with LookupCatalog {

  protected def isView(nameParts: Seq[String]): Boolean

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  val DATA_TYPE_MISMATCH_ERROR = TreeNodeTag[Boolean]("dataTypeMismatchError")

  /**
   * Fails the analysis at the point where a specific tree node was parsed using a provided
   * error class and message parameters.
   */
  def failAnalysis(errorClass: String, messageParameters: Map[String, String]): Nothing = {
    throw new AnalysisException(
      errorClass = errorClass,
      messageParameters = messageParameters)
  }

  protected def containsMultipleGenerators(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(_.collect {
      case e: Generator => e
    }).length > 1
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
      case e if !e.foldable => failAnalysis(
        errorClass = "_LEGACY_ERROR_TEMP_2400",
        messageParameters = Map(
          "name" -> name,
          "limitExpr" -> limitExpr.sql))
      case e if e.dataType != IntegerType => failAnalysis(
        errorClass = "_LEGACY_ERROR_TEMP_2401",
        messageParameters = Map(
          "name" -> name,
          "dataType" -> e.dataType.catalogString))
      case e =>
        e.eval() match {
          case null => failAnalysis(
            errorClass = "_LEGACY_ERROR_TEMP_2402",
            messageParameters = Map(
              "name" -> name,
              "limitExpr" -> limitExpr.sql))
          case v: Int if v < 0 => failAnalysis(
            errorClass = "_LEGACY_ERROR_TEMP_2403",
            messageParameters = Map(
              "name" -> name,
              "v" -> v.toString))
          case _ => // OK
        }
    }
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
    val candidates = operator.inputSet.toSeq.map(_.qualifiedName)
    val orderedCandidates = StringUtils.orderStringsBySimilarity(missingCol, candidates)
    throw QueryCompilationErrors.unresolvedAttributeError(
      errorClass, missingCol, orderedCandidates, a.origin)
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    val inlineCTE = InlineCTE(alwaysInline = true)
    val cteMap = mutable.HashMap.empty[Long, (CTERelationDef, Int)]
    inlineCTE.buildCTEMap(plan, cteMap)
    cteMap.values.foreach { case (relation, refCount) =>
      // If a CTE relation is never used, it will disappear after inline. Here we explicitly check
      // analysis for it, to make sure the entire query plan is valid.
      if (refCount == 0) checkAnalysis0(relation.child)
    }
    // Inline all CTEs in the plan to help check query plan structures in subqueries.
    checkAnalysis0(inlineCTE(plan))
  }

  def checkAnalysis0(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {
      case p if p.analyzed => // Skip already analyzed sub-plans

      case leaf: LeafNode if leaf.output.map(_.dataType).exists(CharVarcharUtils.hasCharVarchar) =>
        throw new IllegalStateException(
          "[BUG] logical plan should not have output of char/varchar type: " + leaf)

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

      case u: UnresolvedFunc =>
        throw QueryCompilationErrors.noSuchFunctionError(
          u.multipartIdentifier, u, u.possibleQualifiedName)

      case u: UnresolvedHint =>
        u.failAnalysis(
          errorClass = "_LEGACY_ERROR_TEMP_2313",
          messageParameters = Map("name" -> u.name))

      case InsertIntoStatement(u: UnresolvedRelation, _, _, _, _, _) =>
        u.tableNotFound(u.multipartIdentifier)

      // TODO (SPARK-27484): handle streaming write commands when we have them.
      case write: V2WriteCommand if write.table.isInstanceOf[UnresolvedRelation] =>
        val tblName = write.table.asInstanceOf[UnresolvedRelation].multipartIdentifier
        write.table.tableNotFound(tblName)

      case command: V2PartitionCommand =>
        command.table match {
          case r @ ResolvedTable(_, _, table, _) => table match {
            case t: SupportsPartitionManagement =>
              if (t.partitionSchema.isEmpty) {
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2404",
                  messageParameters = Map("name" -> r.name))
              }
            case _ =>
              failAnalysis(
                errorClass = "_LEGACY_ERROR_TEMP_2405",
                messageParameters = Map("name" -> r.name))
          }
          case _ =>
        }

      // `ShowTableExtended` should have been converted to the v1 command if the table is v1.
      case _: ShowTableExtended =>
        throw QueryCompilationErrors.commandUnsupportedInV2TableError("SHOW TABLE EXTENDED")

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
              case TypeCheckResult.TypeCheckFailure(message) =>
                hof.failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2314",
                  messageParameters = Map("sqlExpr" -> hof.sql, "msg" -> message))
            }

          // If an attribute can't be resolved as a map key of string type, either the key should be
          // surrounded with single quotes, or there is a typo in the attribute name.
          case GetMapValue(map, key: Attribute) if isMapWithStringKey(map) && !key.resolved =>
            failUnresolvedAttribute(operator, key, "UNRESOLVED_MAP_KEY")
        }

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
                e.setTagValue(DATA_TYPE_MISMATCH_ERROR, true)
                e.dataTypeMismatch(e, checkRes)
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.setTagValue(DATA_TYPE_MISMATCH_ERROR, true)
                extraHintForAnsiTypeCoercionExpression(operator)
                e.failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2315",
                  messageParameters = Map(
                    "sqlExpr" -> e.sql,
                    "msg" -> message,
                    "hint" -> extraHintForAnsiTypeCoercionExpression(operator)))
            }

          case c: Cast if !c.resolved =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2406",
              messageParameters = Map(
                "srcType" -> c.child.dataType.catalogString,
                "targetType" -> c.dataType.catalogString))
          case e: RuntimeReplaceable if !e.replacement.resolved =>
            throw new IllegalStateException("Illegal RuntimeReplaceable: " + e +
              "\nReplacement is unresolved: " + e.replacement)

          case g: Grouping =>
            failAnalysis(errorClass = "_LEGACY_ERROR_TEMP_2445", messageParameters = Map.empty)
          case g: GroupingID =>
            failAnalysis(errorClass = "_LEGACY_ERROR_TEMP_2407", messageParameters = Map.empty)

          case e: Expression if e.children.exists(_.isInstanceOf[WindowFunction]) &&
              !e.isInstanceOf[WindowExpression] && e.resolved =>
            val w = e.children.find(_.isInstanceOf[WindowFunction]).get
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2408",
              messageParameters = Map("w" -> w.toString))

          case w @ WindowExpression(AggregateExpression(_, _, true, _, _), _) =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2409",
              messageParameters = Map("w" -> w.toString))

          case w @ WindowExpression(wf: FrameLessOffsetWindowFunction,
            WindowSpecDefinition(_, order, frame: SpecifiedWindowFrame))
             if order.isEmpty || !frame.isOffset =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2410",
              messageParameters = Map(
                "wf" -> wf.prettyName,
                "w" -> w.toString))

          case w: WindowExpression =>
            // Only allow window functions with an aggregate expression or an offset window
            // function or a Pandas window UDF.
            w.windowFunction match {
              case agg @ AggregateExpression(
                _: PercentileCont | _: PercentileDisc | _: Median, _, _, _, _)
                if w.windowSpec.orderSpec.nonEmpty || w.windowSpec.frameSpecification !=
                    SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing) =>
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2411",
                  messageParameters = Map(
                    "aggFunc" -> agg.aggregateFunction.prettyName))
              case _: AggregateExpression | _: FrameLessOffsetWindowFunction |
                  _: AggregateWindowFunction => // OK
              case f: PythonUDF if PythonUDF.isWindowPandasUDF(f) => // OK
              case other =>
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2412",
                  messageParameters = Map("sqlExpr" -> other.toString))
            }

          case s: SubqueryExpression =>
            checkSubqueryExpression(operator, s)

          case e: ExpressionWithRandomSeed if !e.seedExpression.foldable =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2413",
              messageParameters = Map("argName" -> e.prettyName))

          case _ =>
        })

        operator match {
          case etw: EventTimeWatermark =>
            etw.eventTime.dataType match {
              case s: StructType
                if s.find(_.name == "end").map(_.dataType) == Some(TimestampType) =>
              case _: TimestampType =>
              case _ =>
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2414",
                  messageParameters = Map(
                    "evName" -> etw.eventTime.name,
                    "evType" -> etw.eventTime.dataType.catalogString))
            }
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2415",
              messageParameters = Map(
                "filter" -> f.condition.sql,
                "type" -> f.condition.dataType.catalogString))

          case j @ Join(_, _, _, Some(condition), _) if condition.dataType != BooleanType =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2416",
              messageParameters = Map(
                "join" -> condition.sql,
                "type" -> condition.dataType.catalogString))

          case j @ AsOfJoin(_, _, _, Some(condition), _, _, _)
              if condition.dataType != BooleanType =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2417",
              messageParameters = Map(
                "condition" -> condition.sql,
                "dataType" -> condition.dataType.catalogString))

          case j @ AsOfJoin(_, _, _, _, _, _, Some(toleranceAssertion)) =>
            if (!toleranceAssertion.foldable) {
              failAnalysis(
                errorClass = "_LEGACY_ERROR_TEMP_2418",
                messageParameters = Map.empty)
            }
            if (!toleranceAssertion.eval().asInstanceOf[Boolean]) {
              failAnalysis(
                errorClass = "_LEGACY_ERROR_TEMP_2419",
                messageParameters = Map.empty)
            }

          case Aggregate(groupingExprs, aggregateExprs, _) =>
            def checkValidAggregateExpression(expr: Expression): Unit = expr match {
              case expr: Expression if AggregateExpression.isAggregate(expr) =>
                val aggFunction = expr match {
                  case agg: AggregateExpression => agg.aggregateFunction
                  case udf: PythonUDF => udf
                }
                aggFunction.children.foreach { child =>
                  child.foreach {
                    case expr: Expression if AggregateExpression.isAggregate(expr) =>
                      failAnalysis(
                        errorClass = "_LEGACY_ERROR_TEMP_2420",
                        messageParameters = Map.empty)
                    case other => // OK
                  }

                  if (!child.deterministic) {
                    failAnalysis(
                      errorClass = "_LEGACY_ERROR_TEMP_2421",
                      messageParameters = Map("sqlExpr" -> expr.sql))
                  }
                }
              case e: Attribute if groupingExprs.isEmpty =>
                // Collect all [[AggregateExpressions]]s.
                val aggExprs = aggregateExprs.filter(_.collect {
                  case a: AggregateExpression => a
                }.nonEmpty)
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2422",
                  messageParameters = Map(
                    "sqlExpr" -> e.sql,
                    "aggExprs" -> aggExprs.map(_.sql).mkString("(", ", ", ")")))
              case e: Attribute if !groupingExprs.exists(_.semanticEquals(e)) =>
                throw QueryCompilationErrors.columnNotInGroupByClauseError(e)
              case s: ScalarSubquery
                  if s.children.nonEmpty && !groupingExprs.exists(_.semanticEquals(s)) =>
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2423",
                  messageParameters = Map("sqlExpr" -> s.sql))
              case e if groupingExprs.exists(_.semanticEquals(e)) => // OK
              case e => e.children.foreach(checkValidAggregateExpression)
            }

            def checkValidGroupingExprs(expr: Expression): Unit = {
              if (expr.exists(_.isInstanceOf[AggregateExpression])) {
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2424",
                  messageParameters = Map("sqlExpr" -> expr.sql))
              }

              // Check if the data type of expr is orderable.
              if (!RowOrdering.isOrderable(expr.dataType)) {
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2425",
                  messageParameters = Map(
                    "sqlExpr" -> expr.sql,
                    "dataType" -> expr.dataType.catalogString))
              }

              if (!expr.deterministic) {
                // This is just a sanity check, our analysis rule PullOutNondeterministic should
                // already pull out those nondeterministic expressions and evaluate them in
                // a Project node.
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2426",
                  messageParameters = Map("sqlExpr" -> expr.sql))
              }
            }

            groupingExprs.foreach(checkValidGroupingExprs)
            aggregateExprs.foreach(checkValidAggregateExpression)

          case CollectMetrics(name, metrics, _) =>
            if (name == null || name.isEmpty) {
              operator.failAnalysis(
                errorClass = "_LEGACY_ERROR_TEMP_2316",
                messageParameters = Map("operator" -> operator.toString))
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
                  e.failAnalysis("_LEGACY_ERROR_TEMP_2317", Map("sqlExpr" -> s.sql))
                case _ if !e.deterministic && !seenAggregate =>
                  e.failAnalysis("_LEGACY_ERROR_TEMP_2318", Map("sqlExpr" -> s.sql))
                case a: AggregateExpression if seenAggregate =>
                  e.failAnalysis("_LEGACY_ERROR_TEMP_2319", Map("sqlExpr" -> s.sql))
                case a: AggregateExpression if a.isDistinct =>
                  e.failAnalysis("_LEGACY_ERROR_TEMP_2320", Map("sqlExpr" -> s.sql))
                case a: AggregateExpression if a.filter.isDefined =>
                  e.failAnalysis("_LEGACY_ERROR_TEMP_2321", Map("sqlExpr" -> s.sql))
                case _: Attribute if !seenAggregate =>
                  e.failAnalysis("_LEGACY_ERROR_TEMP_2322", Map("sqlExpr" -> s.sql))
                case _: AggregateExpression =>
                  e.children.foreach(checkMetric (s, _, seenAggregate = true))
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
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2427",
                  messageParameters = Map("type" -> order.dataType.catalogString))
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
                  failAnalysis(
                    errorClass = "_LEGACY_ERROR_TEMP_2428",
                    messageParameters = Map(
                      "limit" -> limit.toString,
                      "offset" -> offset.toString))
                }
              case _ =>
            }

          case Offset(offsetExpr, _) => checkLimitLikeClause("offset", offsetExpr)

          case Tail(limitExpr, _) => checkLimitLikeClause("tail", limitExpr)

          case _: Union | _: SetOperation if operator.children.length > 1 =>
            def dataTypes(plan: LogicalPlan): Seq[DataType] = plan.output.map(_.dataType)
            def ordinalNumber(i: Int): String = i match {
              case 0 => "first"
              case 1 => "second"
              case 2 => "third"
              case i => s"${i + 1}th"
            }
            val ref = dataTypes(operator.children.head)
            operator.children.tail.zipWithIndex.foreach { case (child, ti) =>
              // Check the number of columns
              if (child.output.length != ref.length) {
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2429",
                  messageParameters = Map(
                    "operator" -> operator.nodeName,
                    "firstColNum" -> ref.length.toString,
                    "nTab" -> ordinalNumber(ti + 1),
                    "nColNum" -> child.output.length.toString))
              }

              val dataTypesAreCompatibleFn = getDataTypesAreCompatibleFn(operator)
              // Check if the data types match.
              dataTypes(child).zip(ref).zipWithIndex.foreach { case ((dt1, dt2), ci) =>
                // SPARK-18058: we shall not care about the nullability of columns
                if (!dataTypesAreCompatibleFn(dt1, dt2)) {
                  val errorMessage =
                    s"""
                       |${operator.nodeName} can only be performed on tables with compatible
                       |column types. The ${ordinalNumber(ci)} column of the
                       |${ordinalNumber(ti + 1)} table is ${dt1.catalogString} type which is not
                       |compatible with ${dt2.catalogString} at the same column of the first table
                    """.stripMargin.replace("\n", " ").trim()
                  failAnalysis(
                    errorClass = "_LEGACY_ERROR_TEMP_2430",
                    messageParameters = Map(
                      "operator" -> operator.nodeName,
                      "ci" -> ordinalNumber(ci),
                      "ti" -> ordinalNumber(ti + 1),
                      "dt1" -> dt1.catalogString,
                      "dt2" -> dt2.catalogString,
                      "hint" -> extraHintForAnsiTypeCoercionPlan(operator)))
                }
              }
            }

          case create: V2CreateTablePlan =>
            val references = create.partitioning.flatMap(_.references).toSet
            val badReferences = references.map(_.fieldNames).flatMap { column =>
              create.tableSchema.findNestedField(column) match {
                case Some(_) =>
                  None
                case _ =>
                  Some(column.quoted)
              }
            }

            if (badReferences.nonEmpty) {
              failAnalysis(
                errorClass = "_LEGACY_ERROR_TEMP_2431",
                messageParameters = Map(
                  "cols" -> badReferences.mkString(", ")))
            }

            create.tableSchema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))

          case write: V2WriteCommand if write.resolved =>
            write.query.schema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))

          case alter: AlterTableCommand =>
            checkAlterTableCommand(alter)

          case _ => // Falls back to the following checks
        }

        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty =>
            val missingAttributes = o.missingInput.mkString(",")
            val input = o.inputSet.mkString(",")
            val msgForMissingAttributes = s"Resolved attribute(s) $missingAttributes missing " +
              s"from $input in operator ${operator.simpleString(SQLConf.get.maxToStringFields)}."

            val resolver = plan.conf.resolver
            val attrsWithSameName = o.missingInput.filter { missing =>
              o.inputSet.exists(input => resolver(missing.name, input.name))
            }

            val msg = if (attrsWithSameName.nonEmpty) {
              val sameNames = attrsWithSameName.map(_.name).mkString(",")
              s"$msgForMissingAttributes Attribute(s) with the same name appear in the " +
                s"operation: $sameNames. Please check if the right attribute(s) are used."
            } else {
              msgForMissingAttributes
            }

            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2432",
              messageParameters = Map("msg" -> msg))

          case p @ Project(exprs, _) if containsMultipleGenerators(exprs) =>
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2433",
              messageParameters = Map(
                "sqlExprs" -> exprs.map(_.sql).mkString(",")))

          case p @ Project(projectList, _) =>
            projectList.foreach(_.transformDownWithPruning(
              _.containsPattern(UNRESOLVED_WINDOW_EXPRESSION)) {
              case UnresolvedWindowExpression(_, windowSpec) =>
                throw QueryCompilationErrors.windowSpecificationNotDefinedError(windowSpec.name)
            })

          case j: Join if !j.duplicateResolved =>
            val conflictingAttributes = j.left.outputSet.intersect(j.right.outputSet)
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2434",
              messageParameters = Map(
                "plan" -> plan.toString,
                "conflictingAttributes" -> conflictingAttributes.mkString(",")))

          case i: Intersect if !i.duplicateResolved =>
            val conflictingAttributes = i.left.outputSet.intersect(i.right.outputSet)
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2435",
              messageParameters = Map(
                "plan" -> plan.toString,
                "conflictingAttributes" -> conflictingAttributes.mkString(",")))

          case e: Except if !e.duplicateResolved =>
            val conflictingAttributes = e.left.outputSet.intersect(e.right.outputSet)
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2436",
              messageParameters = Map(
                "plan" -> plan.toString,
                "conflictingAttributes" -> conflictingAttributes.mkString(",")))

          case j: AsOfJoin if !j.duplicateResolved =>
            val conflictingAttributes = j.left.outputSet.intersect(j.right.outputSet)
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2437",
              messageParameters = Map(
                "plan" -> plan.toString,
                "conflictingAttributes" -> conflictingAttributes.mkString(",")))

          // TODO: although map type is not orderable, technically map type should be able to be
          // used in equality comparison, remove this type check once we support it.
          case o if mapColumnInSetOperation(o).isDefined =>
            val mapCol = mapColumnInSetOperation(o).get
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2438",
              messageParameters = Map(
                "colName" -> mapCol.name,
                "dataType" -> mapCol.dataType.catalogString))

          case o if o.expressions.exists(!_.deterministic) &&
            !o.isInstanceOf[Project] && !o.isInstanceOf[Filter] &&
            !o.isInstanceOf[Aggregate] && !o.isInstanceOf[Window] &&
            // Lateral join is checked in checkSubqueryExpression.
            !o.isInstanceOf[LateralJoin] =>
            // The rule above is used to check Aggregate operator.
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2439",
              messageParameters = Map(
                "sqlExprs" -> o.expressions.map(_.sql).mkString(","),
                "operator" -> operator.simpleString(SQLConf.get.maxToStringFields)))

          case _: UnresolvedHint => throw new IllegalStateException(
            "Logical hint operator should be removed during analysis.")

          case f @ Filter(condition, _)
            if PlanHelper.specialExpressionsInUnsupportedOperator(f).nonEmpty =>
            val invalidExprSqls = PlanHelper.specialExpressionsInUnsupportedOperator(f).map(_.sql)
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2440",
              messageParameters = Map(
                "condition" -> condition.sql,
                "invalidExprSqls" -> invalidExprSqls.mkString(", ")))

          case other if PlanHelper.specialExpressionsInUnsupportedOperator(other).nonEmpty =>
            val invalidExprSqls =
              PlanHelper.specialExpressionsInUnsupportedOperator(other).map(_.sql)
            failAnalysis(
              errorClass = "_LEGACY_ERROR_TEMP_2441",
              messageParameters = Map(
                "operator" -> other.nodeName,
                "invalidExprSqls" -> invalidExprSqls.mkString(", ")))

          case _ => // Analysis successful!
        }
    }
    checkCollectedMetrics(plan)
    extendedCheckRules.foreach(_(plan))
    plan.foreachUp {
      case o if !o.resolved =>
        failAnalysis(
          errorClass = "_LEGACY_ERROR_TEMP_2442",
          messageParameters = Map("operator" -> o.simpleString(SQLConf.get.maxToStringFields)))
      case _ =>
    }

    plan.setAnalyzed()
  }

  private def getAllExpressions(plan: LogicalPlan): Seq[Expression] = {
    plan match {
      // `groupingExpressions` may rely on `aggregateExpressions`, due to the GROUP BY alias
      // feature. We should check errors in `aggregateExpressions` first.
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
        case e: Expression if e.getTagValue(DATA_TYPE_MISMATCH_ERROR).contains(true) &&
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
            "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY_OUTPUT",
          messageParameters = Map.empty)
      }

      // SPARK-18504/SPARK-18814: Block cases where GROUP BY columns
      // are not part of the correlated columns.
      val groupByCols = AttributeSet(agg.groupingExpressions.flatMap(_.references))
      // Collect the local references from the correlated predicate in the subquery.
      val subqueryColumns = getCorrelatedPredicates(query).flatMap(_.references)
        .filterNot(conditions.flatMap(_.references).contains)
      val correlatedCols = AttributeSet(subqueryColumns)
      val invalidCols = groupByCols -- correlatedCols
      // GROUP BY columns must be a subset of columns in the predicates
      if (invalidCols.nonEmpty) {
        expr.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "NON_CORRELATED_COLUMNS_IN_GROUP_BY",
          messageParameters = Map("value" -> invalidCols.map(_.name).mkString(",")))
      }
    }

    // Skip subquery aliases added by the Analyzer.
    // For projects, do the necessary mapping and skip to its child.
    @scala.annotation.tailrec
    def cleanQueryInScalarSubquery(p: LogicalPlan): LogicalPlan = p match {
      case s: SubqueryAlias => cleanQueryInScalarSubquery(s.child)
      case p: Project => cleanQueryInScalarSubquery(p.child)
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
    checkAnalysis(expr.plan)

    // Check if there is outer attribute that cannot be found from the plan.
    checkOuterReference(plan, expr)

    expr match {
      case ScalarSubquery(query, outerAttrs, _, _) =>
        // Scalar subquery must return one column as output.
        if (query.output.size != 1) {
          expr.failAnalysis(
            errorClass = "INVALID_SUBQUERY_EXPRESSION." +
              "SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
            messageParameters = Map("number" -> query.output.size.toString))
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
                messageParameters = Map("treeNode" -> planToString(other)))
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
                  messageParameters = Map("treeNode" -> planToString(a)))
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
          expr.failAnalysis(
            errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
              "NON_DETERMINISTIC_LATERAL_SUBQUERIES",
            messageParameters = Map("treeNode" -> planToString(plan)))
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
    val metricsMap = mutable.Map.empty[String, LogicalPlan]
    def check(plan: LogicalPlan): Unit = plan.foreach { node =>
      node match {
        case metrics @ CollectMetrics(name, _, _) =>
          metricsMap.get(name) match {
            case Some(other) =>
              // Exact duplicates are allowed. They can be the result
              // of a CTE that is used multiple times or a self join.
              if (!metrics.sameResult(other)) {
                failAnalysis(
                  errorClass = "_LEGACY_ERROR_TEMP_2443",
                  messageParameters = Map(
                    "name" -> name,
                    "plan" -> plan.toString))
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
      case _: Project => (isScalar || isLateral) && SQLConf.get.decorrelateInnerQueryEnabled
      case _ => false
    }

    // Make sure a plan's expressions do not contain :
    // 1. Aggregate expressions that have mixture of outer and local references.
    // 2. Expressions containing outer references on plan nodes other than allowed operators.
    def failOnInvalidOuterReference(p: LogicalPlan): Unit = {
      p.expressions.foreach(checkMixedReferencesInsideAggregateExpr)
      if (!canHostOuter(p) && p.expressions.exists(containsOuter)) {
        p.failAnalysis(
          errorClass =
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_REFERENCE",
          messageParameters = Map("treeNode" -> planToString(p)))
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
      val allowNonEqualityPredicates =
        SQLConf.get.decorrelateInnerQueryEnabled && (isScalar || isLateral)
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
          errorClass = "_LEGACY_ERROR_TEMP_2323",
          messageParameters = Map(
            "op" -> op,
            "fieldNames" -> fieldNames.quoted,
            "struct" -> struct.treeString))
      }
    }

    def checkColumnNameDuplication(colsToAdd: Seq[QualifiedColType]): Unit = {
      SchemaUtils.checkColumnNameDuplication(
        colsToAdd.map(_.name.quoted),
        "in the user specified columns",
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
              "_LEGACY_ERROR_TEMP_2324", Map("table" -> table.name, "fieldName" -> fieldName))
            case _: MapType => alter.failAnalysis(
              "_LEGACY_ERROR_TEMP_2325", Map("table" -> table.name, "fieldName" -> fieldName))
            case _: ArrayType => alter.failAnalysis(
              "_LEGACY_ERROR_TEMP_2326", Map("table" -> table.name, "fieldName" -> fieldName))
            case u: UserDefinedType[_] => alter.failAnalysis(
              "_LEGACY_ERROR_TEMP_2327",
              Map("table" -> table.name, "fieldName" -> fieldName, "udtSql" -> u.sql))
            case _: CalendarIntervalType | _: AnsiIntervalType => alter.failAnalysis(
              "_LEGACY_ERROR_TEMP_2328", Map("table" -> table.name, "fieldName" -> fieldName))
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
              errorClass = "_LEGACY_ERROR_TEMP_2329",
              messageParameters = Map(
                "table" -> table.name,
                "fieldName" -> fieldName,
                "oldType" -> field.dataType.simpleString,
                "newType" -> newDataType.simpleString))
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
