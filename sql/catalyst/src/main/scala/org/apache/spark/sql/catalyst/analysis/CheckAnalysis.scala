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
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BooleanSimplification
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, TypeUtils}
import org.apache.spark.sql.connector.catalog.{LookupCatalog, SupportsPartitionManagement}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, After, ColumnPosition, DeleteColumn}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

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

  protected def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
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
        s"The $name expression must evaluate to a constant value, but got " +
          limitExpr.sql)
      case e if e.dataType != IntegerType => failAnalysis(
        s"The $name expression must be integer type, but got " +
          e.dataType.catalogString)
      case e =>
        e.eval() match {
          case null => failAnalysis(
            s"The evaluated $name expression must not be null, but got ${limitExpr.sql}")
          case v: Int if v < 0 => failAnalysis(
            s"The $name expression must be equal to or greater than 0, but got $v")
          case _ => // OK
        }
    }
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {

      case p if p.analyzed => // Skip already analyzed sub-plans

      case leaf: LeafNode if leaf.output.map(_.dataType).exists(CharVarcharUtils.hasCharVarchar) =>
        throw new IllegalStateException(
          "[BUG] logical plan should not have output of char/varchar type: " + leaf)

      case u: UnresolvedNamespace =>
        u.failAnalysis(s"Namespace not found: ${u.multipartIdentifier.quoted}")

      case u: UnresolvedTable =>
        u.failAnalysis(s"Table not found: ${u.multipartIdentifier.quoted}")

      case u @ UnresolvedView(NonSessionCatalogAndIdentifier(catalog, ident), cmd, _, _) =>
        u.failAnalysis(
          s"Cannot specify catalog `${catalog.name}` for view ${ident.quoted} " +
            "because view support in v2 catalog has not been implemented yet. " +
            s"$cmd expects a view.")

      case u: UnresolvedView =>
        u.failAnalysis(s"View not found: ${u.multipartIdentifier.quoted}")

      case u: UnresolvedTableOrView =>
        val viewStr = if (u.allowTempView) "view" else "permanent view"
        u.failAnalysis(
          s"Table or $viewStr not found: ${u.multipartIdentifier.quoted}")

      case u: UnresolvedRelation =>
        u.failAnalysis(s"Table or view not found: ${u.multipartIdentifier.quoted}")

      case u: UnresolvedHint =>
        u.failAnalysis(s"Hint not found: ${u.name}")

      case InsertIntoStatement(u: UnresolvedRelation, _, _, _, _, _) =>
        u.failAnalysis(s"Table not found: ${u.multipartIdentifier.quoted}")

      // TODO (SPARK-27484): handle streaming write commands when we have them.
      case write: V2WriteCommand if write.table.isInstanceOf[UnresolvedRelation] =>
        val tblName = write.table.asInstanceOf[UnresolvedRelation].multipartIdentifier
        write.table.failAnalysis(s"Table or view not found: ${tblName.quoted}")

      case u: UnresolvedV2Relation if isView(u.originalNameParts) =>
        u.failAnalysis(
          s"Invalid command: '${u.originalNameParts.quoted}' is a view not a table.")

      case u: UnresolvedV2Relation =>
        u.failAnalysis(s"Table not found: ${u.originalNameParts.quoted}")

      case AlterTable(_, _, u: UnresolvedV2Relation, _) if isView(u.originalNameParts) =>
        u.failAnalysis(
          s"Invalid command: '${u.originalNameParts.quoted}' is a view not a table.")

      case AlterTable(_, _, u: UnresolvedV2Relation, _) =>
        failAnalysis(s"Table not found: ${u.originalNameParts.quoted}")

      case command: V2PartitionCommand =>
        command.table match {
          case r @ ResolvedTable(_, _, table, _) => table match {
            case t: SupportsPartitionManagement =>
              if (t.partitionSchema.isEmpty) {
                failAnalysis(s"Table ${r.name} is not partitioned.")
              }
            case _ =>
              failAnalysis(s"Table ${r.name} does not support partition management.")
          }
          case _ =>
        }

      // `ShowTableExtended` should have been converted to the v1 command if the table is v1.
      case _: ShowTableExtended =>
        throw QueryCompilationErrors.commandUnsupportedInV2TableError("SHOW TABLE EXTENDED")

      case operator: LogicalPlan =>
        // Check argument data types of higher-order functions downwards first.
        // If the arguments of the higher-order functions are resolved but the type check fails,
        // the argument functions will not get resolved, but we should report the argument type
        // check failure instead of claiming the argument functions are unresolved.
        operator transformExpressionsDown {
          case hof: HigherOrderFunction
              if hof.argumentsResolved && hof.checkArgumentDataTypes().isFailure =>
            hof.checkArgumentDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                hof.failAnalysis(
                  s"cannot resolve '${hof.sql}' due to argument data type mismatch: $message")
            }
        }

        operator transformExpressionsUp {
          case a: Attribute if !a.resolved =>
            val from = operator.inputSet.toSeq.map(_.qualifiedName).mkString(", ")
            // cannot resolve '${a.sql}' given input columns: [$from]
            a.failAnalysis(errorClass = "MISSING_COLUMN", messageParameters = Array(a.sql, from))

          case s: Star =>
            withPosition(s) {
              throw QueryCompilationErrors.invalidStarUsageError(operator.nodeName)
            }

          case e: Expression if e.checkInputDataTypes().isFailure =>
            e.checkInputDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.failAnalysis(
                  s"cannot resolve '${e.sql}' due to data type mismatch: $message")
            }

          case c: Cast if !c.resolved =>
            failAnalysis(s"invalid cast from ${c.child.dataType.catalogString} to " +
              c.dataType.catalogString)

          case g: Grouping =>
            failAnalysis("grouping() can only be used with GroupingSets/Cube/Rollup")
          case g: GroupingID =>
            failAnalysis("grouping_id() can only be used with GroupingSets/Cube/Rollup")

          case e: Expression if e.children.exists(_.isInstanceOf[WindowFunction]) &&
              !e.isInstanceOf[WindowExpression] =>
            val w = e.children.find(_.isInstanceOf[WindowFunction]).get
            failAnalysis(s"Window function $w requires an OVER clause.")

          case w @ WindowExpression(AggregateExpression(_, _, true, _, _), _) =>
            failAnalysis(s"Distinct window functions are not supported: $w")

          case w @ WindowExpression(wf: FrameLessOffsetWindowFunction,
            WindowSpecDefinition(_, order, frame: SpecifiedWindowFrame))
             if order.isEmpty || !frame.isOffset =>
            failAnalysis(s"${wf.prettyName} function can only be evaluated in an ordered " +
              s"row-based window frame with a single offset: $w")

          case w @ WindowExpression(e, s) =>
            // Only allow window functions with an aggregate expression or an offset window
            // function or a Pandas window UDF.
            e match {
              case _: AggregateExpression | _: FrameLessOffsetWindowFunction |
                  _: AggregateWindowFunction =>
                w
              case f: PythonUDF if PythonUDF.isWindowPandasUDF(f) =>
                w
              case _ =>
                failAnalysis(s"Expression '$e' not supported within a window function.")
            }

          case s: SubqueryExpression =>
            checkSubqueryExpression(operator, s)
            s

          case e: ExpressionWithRandomSeed if !e.seedExpression.foldable =>
            failAnalysis(
              s"Input argument to ${e.prettyName} must be a constant.")
        }

        operator match {
          case etw: EventTimeWatermark =>
            etw.eventTime.dataType match {
              case s: StructType
                if s.find(_.name == "end").map(_.dataType) == Some(TimestampType) =>
              case _: TimestampType =>
              case _ =>
                failAnalysis(
                  s"Event time must be defined on a window or a timestamp, but " +
                  s"${etw.eventTime.name} is of type ${etw.eventTime.dataType.catalogString}")
            }
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              s"filter expression '${f.condition.sql}' " +
                s"of type ${f.condition.dataType.catalogString} is not a boolean.")

          case j @ Join(_, _, _, Some(condition), _) if condition.dataType != BooleanType =>
            failAnalysis(
              s"join condition '${condition.sql}' " +
                s"of type ${condition.dataType.catalogString} is not a boolean.")

          case a @ Aggregate(groupingExprs, aggregateExprs, child) =>
            def isAggregateExpression(expr: Expression): Boolean = {
              expr.isInstanceOf[AggregateExpression] || PythonUDF.isGroupedAggPandasUDF(expr)
            }

            def checkValidAggregateExpression(expr: Expression): Unit = expr match {
              case expr: Expression if isAggregateExpression(expr) =>
                val aggFunction = expr match {
                  case agg: AggregateExpression => agg.aggregateFunction
                  case udf: PythonUDF => udf
                }
                aggFunction.children.foreach { child =>
                  child.foreach {
                    case expr: Expression if isAggregateExpression(expr) =>
                      failAnalysis(
                        s"It is not allowed to use an aggregate function in the argument of " +
                          s"another aggregate function. Please use the inner aggregate function " +
                          s"in a sub-query.")
                    case other => // OK
                  }

                  if (!child.deterministic) {
                    failAnalysis(
                      s"nondeterministic expression ${expr.sql} should not " +
                        s"appear in the arguments of an aggregate function.")
                  }
                }
              case e: Attribute if groupingExprs.isEmpty =>
                // Collect all [[AggregateExpressions]]s.
                val aggExprs = aggregateExprs.filter(_.collect {
                  case a: AggregateExpression => a
                }.nonEmpty)
                failAnalysis(
                  s"grouping expressions sequence is empty, " +
                    s"and '${e.sql}' is not an aggregate function. " +
                    s"Wrap '${aggExprs.map(_.sql).mkString("(", ", ", ")")}' in windowing " +
                    s"function(s) or wrap '${e.sql}' in first() (or first_value) " +
                    s"if you don't care which value you get."
                )
              case e: Attribute if !groupingExprs.exists(_.semanticEquals(e)) =>
                failAnalysis(
                  s"expression '${e.sql}' is neither present in the group by, " +
                    s"nor is it an aggregate function. " +
                    "Add to group by or wrap in first() (or first_value) if you don't care " +
                    "which value you get.")
              case s: ScalarSubquery
                  if s.children.nonEmpty && !groupingExprs.exists(_.semanticEquals(s)) =>
                failAnalysis(s"Correlated scalar subquery '${s.sql}' is neither " +
                  "present in the group by, nor in an aggregate function. Add it to group by " +
                  "using ordinal position or wrap it in first() (or first_value) if you don't " +
                  "care which value you get.")
              case e if groupingExprs.exists(_.semanticEquals(e)) => // OK
              case e => e.children.foreach(checkValidAggregateExpression)
            }

            def checkValidGroupingExprs(expr: Expression): Unit = {
              if (expr.find(_.isInstanceOf[AggregateExpression]).isDefined) {
                failAnalysis(
                  "aggregate functions are not allowed in GROUP BY, but found " + expr.sql)
              }

              // Check if the data type of expr is orderable.
              if (!RowOrdering.isOrderable(expr.dataType)) {
                failAnalysis(
                  s"expression ${expr.sql} cannot be used as a grouping expression " +
                    s"because its data type ${expr.dataType.catalogString} is not an orderable " +
                    s"data type.")
              }

              if (!expr.deterministic) {
                // This is just a sanity check, our analysis rule PullOutNondeterministic should
                // already pull out those nondeterministic expressions and evaluate them in
                // a Project node.
                failAnalysis(s"nondeterministic expression ${expr.sql} should not " +
                  s"appear in grouping expression.")
              }
            }

            groupingExprs.foreach(checkValidGroupingExprs)
            aggregateExprs.foreach(checkValidAggregateExpression)

          case CollectMetrics(name, metrics, _) =>
            if (name == null || name.isEmpty) {
              operator.failAnalysis(s"observed metrics should be named: $operator")
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
                    "window expressions are not allowed in observed metrics, but found: " + s.sql)
                case _ if !e.deterministic && !seenAggregate =>
                  e.failAnalysis(s"non-deterministic expression ${s.sql} can only be used " +
                    "as an argument to an aggregate function.")
                case a: AggregateExpression if seenAggregate =>
                  e.failAnalysis(
                    "nested aggregates are not allowed in observed metrics, but found: " + s.sql)
                case a: AggregateExpression if a.isDistinct =>
                  e.failAnalysis(
                    "distinct aggregates are not allowed in observed metrics, but found: " + s.sql)
                case a: AggregateExpression if a.filter.isDefined =>
                  e.failAnalysis("aggregates with filter predicate are not allowed in " +
                    "observed metrics, but found: " + s.sql)
                case _: Attribute if !seenAggregate =>
                  e.failAnalysis (s"attribute ${s.sql} can only be used as an argument to an " +
                    "aggregate function.")
                case _: AggregateExpression =>
                  e.children.foreach(checkMetric (s, _, seenAggregate = true))
                case _ =>
                  e.children.foreach(checkMetric (s, _, seenAggregate))
              }
            }
            metrics.foreach(m => checkMetric(m, m))

          case Sort(orders, _, _) =>
            orders.foreach { order =>
              if (!RowOrdering.isOrderable(order.dataType)) {
                failAnalysis(
                  s"sorting is not supported for columns of type ${order.dataType.catalogString}")
              }
            }

          case GlobalLimit(limitExpr, _) => checkLimitLikeClause("limit", limitExpr)

          case LocalLimit(limitExpr, _) => checkLimitLikeClause("limit", limitExpr)

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
                  s"""
                    |${operator.nodeName} can only be performed on tables with the same number
                    |of columns, but the first table has ${ref.length} columns and
                    |the ${ordinalNumber(ti + 1)} table has ${child.output.length} columns
                  """.stripMargin.replace("\n", " ").trim())
              }
              // Check if the data types match.
              dataTypes(child).zip(ref).zipWithIndex.foreach { case ((dt1, dt2), ci) =>
                // SPARK-18058: we shall not care about the nullability of columns
                if (TypeCoercion.findWiderTypeForTwo(dt1.asNullable, dt2.asNullable).isEmpty) {
                  failAnalysis(
                    s"""
                      |${operator.nodeName} can only be performed on tables with the compatible
                      |column types. ${dt1.catalogString} <> ${dt2.catalogString} at the
                      |${ordinalNumber(ci)} column of the ${ordinalNumber(ti + 1)} table
                    """.stripMargin.replace("\n", " ").trim())
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
                  Some(s"${column.quoted} is missing or is in a map or array")
              }
            }

            if (badReferences.nonEmpty) {
              failAnalysis(s"Invalid partitioning: ${badReferences.mkString(", ")}")
            }

            create.tableSchema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))

          case write: V2WriteCommand if write.resolved =>
            write.query.schema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))

          case alter: AlterTableCommand if alter.table.resolved =>
            checkAlterTableCommand(alter)

          case alter: AlterTable if alter.table.resolved =>
            val table = alter.table
            def findField(operation: String, fieldName: Array[String]): StructField = {
              // include collections because structs nested in maps and arrays may be altered
              val field = table.schema.findNestedField(fieldName, includeCollections = true)
              if (field.isEmpty) {
                alter.failAnalysis(
                  s"Cannot $operation missing field ${fieldName.quoted} in ${table.name} schema: " +
                  table.schema.treeString)
              }
              field.get._2
            }
            def positionArgumentExists(
                position: ColumnPosition,
                struct: StructType,
                fieldsAdded: Seq[String]): Unit = {
              position match {
                case after: After =>
                  val allFields = struct.fieldNames ++ fieldsAdded
                  if (!allFields.contains(after.column())) {
                    alter.failAnalysis(s"Couldn't resolve positional argument $position amongst " +
                      s"${allFields.mkString("[", ", ", "]")}")
                  }
                case _ =>
              }
            }
            def findParentStruct(operation: String, fieldNames: Array[String]): StructType = {
              val parent = fieldNames.init
              val field = if (parent.nonEmpty) {
                findField(operation, parent).dataType
              } else {
                table.schema
              }
              field match {
                case s: StructType => s
                case o => alter.failAnalysis(s"Cannot $operation ${fieldNames.quoted}, because " +
                  s"its parent is not a StructType. Found $o")
              }
            }
            def checkColumnNotExists(
                operation: String,
                fieldNames: Array[String],
                struct: StructType): Unit = {
              if (struct.findNestedField(fieldNames, includeCollections = true).isDefined) {
                alter.failAnalysis(s"Cannot $operation column, because ${fieldNames.quoted} " +
                  s"already exists in ${struct.treeString}")
              }
            }

            val colsToDelete = mutable.Set.empty[Seq[String]]
            // 'colsToAdd' keeps track of new columns being added. It stores a mapping from a parent
            // name of fields to field names that belong to the parent. For example, if we add
            // columns "a.b.c", "a.b.d", and "a.c", 'colsToAdd' will become
            // Map(Seq("a", "b") -> Seq("c", "d"), Seq("a") -> Seq("c")).
            val colsToAdd = mutable.Map.empty[Seq[String], Seq[String]]

            alter.changes.foreach {
              case add: AddColumn =>
                // If a column to add is a part of columns to delete, we don't need to check
                // if column already exists - applies to REPLACE COLUMNS scenario.
                if (!colsToDelete.contains(add.fieldNames())) {
                  checkColumnNotExists("add", add.fieldNames(), table.schema)
                }
                val parent = findParentStruct("add", add.fieldNames())
                val parentName = add.fieldNames().init
                val fieldsAdded = colsToAdd.getOrElse(parentName, Nil)
                positionArgumentExists(add.position(), parent, fieldsAdded)
                TypeUtils.failWithIntervalType(add.dataType())
                colsToAdd(parentName) = fieldsAdded :+ add.fieldNames().last
              case delete: DeleteColumn =>
                findField("delete", delete.fieldNames)
                // REPLACE COLUMNS has deletes followed by adds. Remember the deleted columns
                // so that add operations do not fail when the columns to add exist and they
                // are to be deleted.
                colsToDelete += delete.fieldNames
              case _ =>
              // no validation needed for set and remove property
            }

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

            failAnalysis(msg)

          case p @ Project(exprs, _) if containsMultipleGenerators(exprs) =>
            failAnalysis(
              s"""Only a single table generating function is allowed in a SELECT clause, found:
                 | ${exprs.map(_.sql).mkString(",")}""".stripMargin)

          case j: Join if !j.duplicateResolved =>
            val conflictingAttributes = j.left.outputSet.intersect(j.right.outputSet)
            failAnalysis(
              s"""
                 |Failure when resolving conflicting references in Join:
                 |$plan
                 |Conflicting attributes: ${conflictingAttributes.mkString(",")}
                 |""".stripMargin)

          case i: Intersect if !i.duplicateResolved =>
            val conflictingAttributes = i.left.outputSet.intersect(i.right.outputSet)
            failAnalysis(
              s"""
                 |Failure when resolving conflicting references in Intersect:
                 |$plan
                 |Conflicting attributes: ${conflictingAttributes.mkString(",")}
               """.stripMargin)

          case e: Except if !e.duplicateResolved =>
            val conflictingAttributes = e.left.outputSet.intersect(e.right.outputSet)
            failAnalysis(
              s"""
                 |Failure when resolving conflicting references in Except:
                 |$plan
                 |Conflicting attributes: ${conflictingAttributes.mkString(",")}
               """.stripMargin)

          // TODO: although map type is not orderable, technically map type should be able to be
          // used in equality comparison, remove this type check once we support it.
          case o if mapColumnInSetOperation(o).isDefined =>
            val mapCol = mapColumnInSetOperation(o).get
            failAnalysis("Cannot have map type columns in DataFrame which calls " +
              s"set operations(intersect, except, etc.), but the type of column ${mapCol.name} " +
              "is " + mapCol.dataType.catalogString)

          case o if o.expressions.exists(!_.deterministic) &&
            !o.isInstanceOf[Project] && !o.isInstanceOf[Filter] &&
            !o.isInstanceOf[Aggregate] && !o.isInstanceOf[Window] =>
            // The rule above is used to check Aggregate operator.
            failAnalysis(
              s"""nondeterministic expressions are only allowed in
                 |Project, Filter, Aggregate or Window, found:
                 | ${o.expressions.map(_.sql).mkString(",")}
                 |in operator ${operator.simpleString(SQLConf.get.maxToStringFields)}
               """.stripMargin)

          case _: UnresolvedHint =>
            throw QueryExecutionErrors.logicalHintOperatorNotRemovedDuringAnalysisError

          case f @ Filter(condition, _)
            if PlanHelper.specialExpressionsInUnsupportedOperator(f).nonEmpty =>
            val invalidExprSqls = PlanHelper.specialExpressionsInUnsupportedOperator(f).map(_.sql)
            failAnalysis(
              s"""
                 |Aggregate/Window/Generate expressions are not valid in where clause of the query.
                 |Expression in where clause: [${condition.sql}]
                 |Invalid expressions: [${invalidExprSqls.mkString(", ")}]""".stripMargin)

          case other if PlanHelper.specialExpressionsInUnsupportedOperator(other).nonEmpty =>
            val invalidExprSqls =
              PlanHelper.specialExpressionsInUnsupportedOperator(other).map(_.sql)
            failAnalysis(
              s"""
                 |The query operator `${other.nodeName}` contains one or more unsupported
                 |expression types Aggregate, Window or Generate.
                 |Invalid expressions: [${invalidExprSqls.mkString(", ")}]""".stripMargin
            )

          case _ => // Analysis successful!
        }
    }
    checkCollectedMetrics(plan)
    extendedCheckRules.foreach(_(plan))
    plan.foreachUp {
      case o if !o.resolved =>
        failAnalysis(s"unresolved operator ${o.simpleString(SQLConf.get.maxToStringFields)}")
      case _ =>
    }

    plan.setAnalyzed()
  }

  /**
   * Validates subquery expressions in the plan. Upon failure, returns an user facing error.
   */
  private def checkSubqueryExpression(plan: LogicalPlan, expr: SubqueryExpression): Unit = {
    def checkAggregateInScalarSubquery(
        conditions: Seq[Expression],
        query: LogicalPlan, agg: Aggregate): Unit = {
      // Make sure correlated scalar subqueries contain one row for every outer row by
      // enforcing that they are aggregates containing exactly one aggregate expression.
      val aggregates = agg.expressions.flatMap(_.collect {
        case a: AggregateExpression => a
      })
      if (aggregates.isEmpty) {
        failAnalysis("The output of a correlated scalar subquery must be aggregated")
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
        failAnalysis(
          "A GROUP BY clause in a scalar correlated subquery " +
            "cannot contain non-correlated columns: " +
            invalidCols.mkString(","))
      }
    }

    // Skip subquery aliases added by the Analyzer.
    // For projects, do the necessary mapping and skip to its child.
    def cleanQueryInScalarSubquery(p: LogicalPlan): LogicalPlan = p match {
      case s: SubqueryAlias => cleanQueryInScalarSubquery(s.child)
      case p: Project => cleanQueryInScalarSubquery(p.child)
      case child => child
    }

    // Check whether the given expressions contains the subquery expression.
    def containsExpr(expressions: Seq[Expression]): Boolean = {
      expressions.exists(_.find(_.semanticEquals(expr)).isDefined)
    }

    // Validate the subquery plan.
    checkAnalysis(expr.plan)

    expr match {
      case ScalarSubquery(query, outerAttrs, _, _) =>
        // Scalar subquery must return one column as output.
        if (query.output.size != 1) {
          failAnalysis(
            s"Scalar subquery must return only one column, but got ${query.output.size}")
        }

        if (outerAttrs.nonEmpty) {
          cleanQueryInScalarSubquery(query) match {
            case a: Aggregate => checkAggregateInScalarSubquery(outerAttrs, query, a)
            case Filter(_, a: Aggregate) => checkAggregateInScalarSubquery(outerAttrs, query, a)
            case p: LogicalPlan if p.maxRows.exists(_ <= 1) => // Ok
            case fail => failAnalysis(s"Correlated scalar subqueries must be aggregated: $fail")
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
                failAnalysis("Correlated scalar subqueries in the group by clause " +
                  s"must also be in the aggregate expressions:\n$a")
              }
            case other => failAnalysis(
              "Correlated scalar sub-queries can only be used in a " +
                s"Filter/Aggregate/Project and a few commands: $plan")
          }
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan, isScalarOrLateral = true)

      case _: LateralSubquery =>
        assert(plan.isInstanceOf[LateralJoin])
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan, isScalarOrLateral = true)

      case inSubqueryOrExistsSubquery =>
        plan match {
          case _: Filter | _: SupportsSubquery | _: Join => // Ok
          case _ =>
            failAnalysis(s"IN/EXISTS predicate sub-queries can only be used in" +
                s" Filter/Join and a few commands: $plan")
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
                  s"Multiple definitions of observed metrics named '$name': $plan")
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
      isScalarOrLateral: Boolean = false): Unit = {
    // Validate that correlated aggregate expression do not contain a mixture
    // of outer and local references.
    def checkMixedReferencesInsideAggregateExpr(expr: Expression): Unit = {
      expr.foreach {
        case a: AggregateExpression if containsOuter(a) =>
          if (a.references.nonEmpty) {
            throw QueryCompilationErrors.mixedRefsInAggFunc(a.sql)
          }
        case _ =>
      }
    }

    // Make sure a plan's subtree does not contain outer references
    def failOnOuterReferenceInSubTree(p: LogicalPlan): Unit = {
      if (hasOuterReferences(p)) {
        failAnalysis(s"Accessing outer query column is not allowed in:\n$p")
      }
    }

    // Check whether the logical plan node can host outer references.
    // A `Project` can host outer references if it is inside a scalar or a lateral subquery and
    // DecorrelateInnerQuery is enabled. Otherwise, only Filter can only outer references.
    def canHostOuter(plan: LogicalPlan): Boolean = plan match {
      case _: Filter => true
      case _: Project => isScalarOrLateral && SQLConf.get.decorrelateInnerQueryEnabled
      case _ => false
    }

    // Make sure a plan's expressions do not contain :
    // 1. Aggregate expressions that have mixture of outer and local references.
    // 2. Expressions containing outer references on plan nodes other than allowed operators.
    def failOnInvalidOuterReference(p: LogicalPlan): Unit = {
      p.expressions.foreach(checkMixedReferencesInsideAggregateExpr)
      if (!canHostOuter(p) && p.expressions.exists(containsOuter)) {
        failAnalysis(
          "Expressions referencing the outer query are not supported outside of WHERE/HAVING " +
            s"clauses:\n$p")
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
      if (predicates.nonEmpty) {
        // Report a non-supported case as an exception
        failAnalysis("Correlated column is not allowed in predicate " +
          s"${predicates.map(_.sql).mkString}:\n$p")
      }
    }

    def containsAttribute(e: Expression): Boolean = {
      e.find(_.isInstanceOf[Attribute]).isDefined
    }

    // Given a correlated predicate, check if it is either a non-equality predicate or
    // equality predicate that does not guarantee one-on-one mapping between inner and
    // outer attributes. When the correlated predicate does not contain any attribute
    // (i.e. only has outer references), it is supported and should return false. E.G.:
    //   (a = outer(c)) -> false
    //   (outer(c) = outer(d)) -> false
    //   (a > outer(c)) -> true
    //   (a + b = outer(c)) -> true
    // The last one is true because there can be multiple combinations of (a, b) that
    // satisfy the equality condition. For example, if outer(c) = 0, then both (0, 0)
    // and (-1, 1) can make the predicate evaluate to true.
    def isUnsupportedPredicate(condition: Expression): Boolean = condition match {
      // Only allow equality condition with one side being an attribute and another
      // side being an expression without attributes from the inner query. Note
      // OuterReference is a leaf node and will not be found here.
      case Equality(_: Attribute, b) => containsAttribute(b)
      case Equality(a, _: Attribute) => containsAttribute(a)
      case e @ Equality(_, _) => containsAttribute(e)
      case _ => true
    }

    val unsupportedPredicates = mutable.ArrayBuffer.empty[Expression]

    // Simplify the predicates before validating any unsupported correlation patterns in the plan.
    AnalysisHelper.allowInvokingTransformsInAnalyzer { BooleanSimplification(sub).foreachUp {
      // Approve operators allowed in a correlated subquery
      // There are 4 categories:
      // 1. Operators that are allowed anywhere in a correlated subquery, and,
      //    by definition of the operators, they either do not contain
      //    any columns or cannot host outer references.
      // 2. Operators that are allowed anywhere in a correlated subquery
      //    so long as they do not host outer references.
      // 3. Operators that need special handlings. These operators are
      //    Filter, Join, Aggregate, and Generate.
      //
      // Any operators that are not in the above list are allowed
      // in a correlated subquery only if they are not on a correlation path.
      // In other word, these operators are allowed only under a correlation point.
      //
      // A correlation path is defined as the sub-tree of all the operators that
      // are on the path from the operator hosting the correlated expressions
      // up to the operator producing the correlated values.

      // Category 1:
      // ResolvedHint, Distinct, LeafNode, Repartition, and SubqueryAlias
      case _: ResolvedHint | _: Distinct | _: LeafNode | _: Repartition | _: SubqueryAlias =>

      // Category 2:
      // These operators can be anywhere in a correlated subquery.
      // so long as they do not host outer references in the operators.
      case p: Project =>
        failOnInvalidOuterReference(p)

      case s: Sort =>
        failOnInvalidOuterReference(s)

      case r: RepartitionByExpression =>
        failOnInvalidOuterReference(r)

      case l: LateralJoin =>
        failOnInvalidOuterReference(l)

      // Category 3:
      // Filter is one of the two operators allowed to host correlated expressions.
      // The other operator is Join. Filter can be anywhere in a correlated subquery.
      case f: Filter =>
        val (correlated, _) = splitConjunctivePredicates(f.condition).partition(containsOuter)
        unsupportedPredicates ++= correlated.filter(isUnsupportedPredicate)
        failOnInvalidOuterReference(f)

      // Aggregate cannot host any correlated expressions
      // It can be on a correlation path if the correlation contains
      // only supported correlated equality predicates.
      // It cannot be on a correlation path if the correlation has
      // non-equality correlated predicates.
      case a: Aggregate =>
        failOnInvalidOuterReference(a)
        failOnUnsupportedCorrelatedPredicate(unsupportedPredicates.toSeq, a)

      // Join can host correlated expressions.
      case j @ Join(left, right, joinType, _, _) =>
        joinType match {
          // Inner join, like Filter, can be anywhere.
          case _: InnerLike =>
            failOnInvalidOuterReference(j)

          // Left outer join's right operand cannot be on a correlation path.
          // LeftAnti and ExistenceJoin are special cases of LeftOuter.
          // Note that ExistenceJoin cannot be expressed externally in both SQL and DataFrame
          // so it should not show up here in Analysis phase. This is just a safety net.
          //
          // LeftSemi does not allow output from the right operand.
          // Any correlated references in the subplan
          // of the right operand cannot be pulled up.
          case LeftOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
            failOnInvalidOuterReference(j)
            failOnOuterReferenceInSubTree(right)

          // Likewise, Right outer join's left operand cannot be on a correlation path.
          case RightOuter =>
            failOnInvalidOuterReference(j)
            failOnOuterReferenceInSubTree(left)

          // Any other join types not explicitly listed above,
          // including Full outer join, are treated as Category 4.
          case _ =>
            failOnOuterReferenceInSubTree(j)
        }

      // Generator with join=true, i.e., expressed with
      // LATERAL VIEW [OUTER], similar to inner join,
      // allows to have correlation under it
      // but must not host any outer references.
      // Note:
      // Generator with requiredChildOutput.isEmpty is treated as Category 4.
      case g: Generate if g.requiredChildOutput.nonEmpty =>
        failOnInvalidOuterReference(g)

      // Category 4: Any other operators not in the above 3 categories
      // cannot be on a correlation path, that is they are allowed only
      // under a correlation point but they and their descendant operators
      // are not allowed to have any correlated expressions.
      case p =>
        failOnOuterReferenceInSubTree(p)
    }}
  }

  /**
   * Validates the options used for alter table commands after table and columns are resolved.
   */
  private def checkAlterTableCommand(alter: AlterTableCommand): Unit = {
    def checkColumnNotExists(fieldNames: Seq[String], struct: StructType): Unit = {
      if (struct.findNestedField(fieldNames, includeCollections = true).isDefined) {
        alter.failAnalysis(s"Cannot ${alter.operation} column, because ${fieldNames.quoted} " +
          s"already exists in ${struct.treeString}")
      }
    }

    alter match {
      case AlterTableRenameColumn(table: ResolvedTable, col: ResolvedFieldName, newName) =>
        checkColumnNotExists(col.path :+ newName, table.schema)
      case a @ AlterTableAlterColumn(table: ResolvedTable, col: ResolvedFieldName, _, _, _, _) =>
        val fieldName = col.name.quoted
        if (a.dataType.isDefined) {
          val field = CharVarcharUtils.getRawType(col.field.metadata)
            .map(dt => col.field.copy(dataType = dt))
            .getOrElse(col.field)
          val newDataType = a.dataType.get
          newDataType match {
            case _: StructType =>
              alter.failAnalysis(s"Cannot update ${table.name} field $fieldName type: " +
                "update a struct by updating its fields")
            case _: MapType =>
              alter.failAnalysis(s"Cannot update ${table.name} field $fieldName type: " +
                s"update a map by updating $fieldName.key or $fieldName.value")
            case _: ArrayType =>
              alter.failAnalysis(s"Cannot update ${table.name} field $fieldName type: " +
                s"update the element by updating $fieldName.element")
            case u: UserDefinedType[_] =>
              alter.failAnalysis(s"Cannot update ${table.name} field $fieldName type: " +
                s"update a UserDefinedType[${u.sql}] by updating its fields")
            case _: CalendarIntervalType | _: YearMonthIntervalType | _: DayTimeIntervalType =>
              alter.failAnalysis(s"Cannot update ${table.name} field $fieldName to interval type")
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
            alter.failAnalysis(s"Cannot update ${table.name} field $fieldName: " +
              s"${field.dataType.simpleString} cannot be cast to ${newDataType.simpleString}")
          }
        }
        if (a.nullable.isDefined) {
          if (!a.nullable.get && col.field.nullable) {
            alter.failAnalysis(s"Cannot change nullable column to non-nullable: $fieldName")
          }
        }
      case _ =>
    }
  }
}
