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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ComputeCurrentTime
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.DefaultColumnAnalyzer
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

/**
 * This object is responsible for processing unresolved table specifications in commands with
 * OPTIONS lists. The parser produces such lists as maps from strings to unresolved expressions.
 * After otherwise resolving such expressions in the analyzer, here we convert them to resolved
 * table specifications wherein these OPTIONS list values are represented as strings instead, for
 * convenience.
 */
object ResolveTableSpec extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val preparedPlan = if (SQLConf.get.legacyEvalCurrentTime && plan.containsPattern(COMMAND)) {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        ComputeCurrentTime(ResolveTimeZone(plan))
      }
    } else {
      plan
    }

    preparedPlan.resolveOperatorsWithPruning(_.containsAnyPattern(COMMAND), ruleId) {
      case t: CreateTable =>
        resolveTableSpec(t, t.tableSpec,
          fakeRelationFromColumns(t.columns), s => t.copy(tableSpec = s))
      case t: CreateTableAsSelect =>
        resolveTableSpec(t, t.tableSpec, None, s => t.copy(tableSpec = s))
      case t: ReplaceTable =>
        resolveTableSpec(t, t.tableSpec,
          fakeRelationFromColumns(t.columns), s => t.copy(tableSpec = s))
      case t: ReplaceTableAsSelect =>
        resolveTableSpec(t, t.tableSpec, None, s => t.copy(tableSpec = s))
    }
  }

  private def fakeRelationFromColumns(columns: Seq[ColumnDefinition]): Option[LogicalPlan] = {
    val attributeList = columns.map { col =>
      AttributeReference(col.name, col.dataType)()
    }
    Some(LocalRelation(attributeList))
  }

  private def analyzeConstraints(
      constraints: Seq[TableConstraint],
      fakeRelation: LogicalPlan): Seq[TableConstraint] = {
    val analyzedExpressions = constraints.map {
      case c: CheckConstraint =>
        val alias = Alias(c.child, c.name)()
        val project = Project(Seq(alias), fakeRelation)
        val analyzed = DefaultColumnAnalyzer.execute(project)
        DefaultColumnAnalyzer.checkAnalysis0(analyzed)

        val analyzedExpression = (analyzed collectFirst {
          case Project(Seq(Alias(e: Expression, _)), _) => e
        }).get
        if (!analyzedExpression.deterministic) {
          analyzedExpression.failAnalysis(
            errorClass = "INVALID_CHECK_CONSTRAINT.NONDETERMINISTIC",
            messageParameters = Map.empty
          )
        }
        c.withNewChildren(Seq(analyzedExpression)).asInstanceOf[CheckConstraint]
      case other => other
    }
    analyzedExpressions
  }

  /** Helper method to resolve the table specification within a logical plan. */
  private def resolveTableSpec(
      input: LogicalPlan,
      tableSpec: TableSpecBase,
      fakeRelation: Option[LogicalPlan],
      withNewSpec: TableSpecBase => LogicalPlan): LogicalPlan = tableSpec match {
    case u: UnresolvedTableSpec if u.optionExpression.resolved =>
      val newOptions: Seq[(String, String)] = u.optionExpression.options.map {
        case (key: String, null) =>
          (key, null)
        case (key: String, value: Expression) =>
          val newValue: String = try {
            val dt = value.dataType
            value match {
              case Literal(null, _) =>
                null
              case _
                if dt.isInstanceOf[ArrayType] ||
                  dt.isInstanceOf[StructType] ||
                  dt.isInstanceOf[MapType] =>
                throw QueryCompilationErrors.optionMustBeConstant(key)
              case _ =>
                val result = value.eval()
                Literal(result, dt).toString
            }
          } catch {
            case e @ (_: SparkThrowable | _: java.lang.RuntimeException) =>
              throw QueryCompilationErrors.optionMustBeConstant(key, Some(e))
          }
          (key, newValue)
      }
      // Analyze constraints for the table spec:
      // If a fake relation is provided (for CREATE/REPLACE TABLE with column definitions),
      // analyze the constraints using the fake relation as context.
      // Otherwise, use the original constraints unmodified
      val newConstraints = if (fakeRelation.isDefined) {
        analyzeConstraints(u.constraints, fakeRelation.get)
      } else {
        u.constraints
      }
      val newTableSpec = TableSpec(
        properties = u.properties,
        provider = u.provider,
        options = newOptions.toMap,
        location = u.location,
        comment = u.comment,
        collation = u.collation,
        serde = u.serde,
        external = u.external,
        constraints = newConstraints.map(_.asConstraint(isCreateTable = true)))
      withNewSpec(newTableSpec)
    case _ =>
      input
  }
}
