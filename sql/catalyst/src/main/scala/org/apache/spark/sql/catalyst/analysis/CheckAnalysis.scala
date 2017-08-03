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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * Throws user facing errors when passed invalid queries that fail to analyze.
 */
trait CheckAnalysis extends PredicateHelper {

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

  private def checkLimitClause(limitExpr: Expression): Unit = {
    limitExpr match {
      case e if !e.foldable => failAnalysis(
        "The limit expression must evaluate to a constant value, but got " +
          limitExpr.sql)
      case e if e.dataType != IntegerType => failAnalysis(
        s"The limit expression must be integer type, but got " +
          e.dataType.simpleString)
      case e if e.eval().asInstanceOf[Int] < 0 => failAnalysis(
        "The limit expression must be equal to or greater than 0, but got " +
          e.eval().asInstanceOf[Int])
      case e => // OK
    }
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    plan.foreachUp {
      case p if p.analyzed => // Skip already analyzed sub-plans

      case u: UnresolvedRelation =>
        u.failAnalysis(s"Table or view not found: ${u.tableIdentifier}")

      case operator: LogicalPlan =>
        operator transformExpressionsUp {
          case a: Attribute if !a.resolved =>
            val from = operator.inputSet.map(_.name).mkString(", ")
            a.failAnalysis(s"cannot resolve '${a.sql}' given input columns: [$from]")

          case e: Expression if e.checkInputDataTypes().isFailure =>
            e.checkInputDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.failAnalysis(
                  s"cannot resolve '${e.sql}' due to data type mismatch: $message")
            }

          case c: Cast if !c.resolved =>
            failAnalysis(
              s"invalid cast from ${c.child.dataType.simpleString} to ${c.dataType.simpleString}")

          case g: Grouping =>
            failAnalysis("grouping() can only be used with GroupingSets/Cube/Rollup")
          case g: GroupingID =>
            failAnalysis("grouping_id() can only be used with GroupingSets/Cube/Rollup")

          case w @ WindowExpression(AggregateExpression(_, _, true, _), _) =>
            failAnalysis(s"Distinct window functions are not supported: $w")

          case w @ WindowExpression(_: OffsetWindowFunction, WindowSpecDefinition(_, order,
               SpecifiedWindowFrame(frame,
                 FrameBoundary(l),
                 FrameBoundary(h))))
             if order.isEmpty || frame != RowFrame || l != h =>
            failAnalysis("An offset window function can only be evaluated in an ordered " +
              s"row-based window frame with a single offset: $w")

          case w @ WindowExpression(e, s) =>
            // Only allow window functions with an aggregate expression or an offset window
            // function.
            e match {
              case _: AggregateExpression | _: OffsetWindowFunction | _: AggregateWindowFunction =>
              case _ =>
                failAnalysis(s"Expression '$e' not supported within a window function.")
            }
            // Make sure the window specification is valid.
            s.validate match {
              case Some(m) =>
                failAnalysis(s"Window specification $s is not valid because $m")
              case None => w
            }

          case s @ ScalarSubquery(query, conditions, _) =>
            checkAnalysis(query)

            // If no correlation, the output must be exactly one column
            if (conditions.isEmpty && query.output.size != 1) {
              failAnalysis(
                s"Scalar subquery must return only one column, but got ${query.output.size}")
            } else if (conditions.nonEmpty) {
              def checkAggregate(agg: Aggregate): Unit = {
                // Make sure correlated scalar subqueries contain one row for every outer row by
                // enforcing that they are aggregates containing exactly one aggregate expression.
                // The analyzer has already checked that subquery contained only one output column,
                // and added all the grouping expressions to the aggregate.
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
              def cleanQuery(p: LogicalPlan): LogicalPlan = p match {
                case s: SubqueryAlias => cleanQuery(s.child)
                case p: Project => cleanQuery(p.child)
                case child => child
              }

              cleanQuery(query) match {
                case a: Aggregate => checkAggregate(a)
                case Filter(_, a: Aggregate) => checkAggregate(a)
                case fail => failAnalysis(s"Correlated scalar subqueries must be Aggregated: $fail")
              }
            }
            s

          case s: SubqueryExpression =>
            checkAnalysis(s.plan)
            s
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
                  s"${etw.eventTime.name} is of type ${etw.eventTime.dataType.simpleString}")
            }
          case f: Filter if f.condition.dataType != BooleanType =>
            failAnalysis(
              s"filter expression '${f.condition.sql}' " +
                s"of type ${f.condition.dataType.simpleString} is not a boolean.")

          case Filter(condition, _) if hasNullAwarePredicateWithinNot(condition) =>
            failAnalysis("Null-aware predicate sub-queries cannot be used in nested " +
              s"conditions: $condition")

          case j @ Join(_, _, _, Some(condition)) if condition.dataType != BooleanType =>
            failAnalysis(
              s"join condition '${condition.sql}' " +
                s"of type ${condition.dataType.simpleString} is not a boolean.")

          case Aggregate(groupingExprs, aggregateExprs, child) =>
            def checkValidAggregateExpression(expr: Expression): Unit = expr match {
              case aggExpr: AggregateExpression =>
                aggExpr.aggregateFunction.children.foreach { child =>
                  child.foreach {
                    case agg: AggregateExpression =>
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
                    s"because its data type ${expr.dataType.simpleString} is not an orderable " +
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

          case Sort(orders, _, _) =>
            orders.foreach { order =>
              if (!RowOrdering.isOrderable(order.dataType)) {
                failAnalysis(
                  s"sorting is not supported for columns of type ${order.dataType.simpleString}")
              }
            }

          case GlobalLimit(limitExpr, _) => checkLimitClause(limitExpr)

          case LocalLimit(limitExpr, _) => checkLimitClause(limitExpr)

          case p if p.expressions.exists(ScalarSubquery.hasCorrelatedScalarSubquery) =>
            p match {
              case _: Filter | _: Aggregate | _: Project => // Ok
              case other => failAnalysis(
                s"Correlated scalar sub-queries can only be used in a Filter/Aggregate/Project: $p")
            }

          case p if p.expressions.exists(SubqueryExpression.hasInOrExistsSubquery) =>
            p match {
              case _: Filter => // Ok
              case _ => failAnalysis(s"Predicate sub-queries can only be used in a Filter: $p")
            }

          case _: Union | _: SetOperation if operator.children.length > 1 =>
            def dataTypes(plan: LogicalPlan): Seq[DataType] = plan.output.map(_.dataType)
            def ordinalNumber(i: Int): String = i match {
              case 0 => "first"
              case 1 => "second"
              case i => s"${i}th"
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

          case _ => // Fallbacks to the following checks
        }

        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty =>
            val missingAttributes = o.missingInput.mkString(",")
            val input = o.inputSet.mkString(",")

            failAnalysis(
              s"resolved attribute(s) $missingAttributes missing from $input " +
                s"in operator ${operator.simpleString}")

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
              "is " + mapCol.dataType.simpleString)

          case o if o.expressions.exists(!_.deterministic) &&
            !o.isInstanceOf[Project] && !o.isInstanceOf[Filter] &&
            !o.isInstanceOf[Aggregate] && !o.isInstanceOf[Window] =>
            // The rule above is used to check Aggregate operator.
            failAnalysis(
              s"""nondeterministic expressions are only allowed in
                 |Project, Filter, Aggregate or Window, found:
                 | ${o.expressions.map(_.sql).mkString(",")}
                 |in operator ${operator.simpleString}
               """.stripMargin)

          case _: UnresolvedHint =>
            throw new IllegalStateException(
              "Internal error: logical hint operator should have been removed during analysis")

          case _ => // Analysis successful!
        }
    }
    extendedCheckRules.foreach(_(plan))
    plan.foreachUp {
      case o if !o.resolved => failAnalysis(s"unresolved operator ${o.simpleString}")
      case _ =>
    }

    plan.foreach(_.setAnalyzed())
  }
}
