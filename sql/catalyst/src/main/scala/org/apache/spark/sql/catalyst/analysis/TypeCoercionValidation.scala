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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, SetOperation, Union}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

object TypeCoercionValidation extends QueryErrorsBase {
  private val DATA_TYPE_MISMATCH_ERROR = TreeNodeTag[Unit]("dataTypeMismatchError")

  def failOnTypeCheckResult(e: Expression, operator: Option[LogicalPlan] = None): Nothing = {
    e.checkInputDataTypes() match {
      case checkRes: TypeCheckResult.DataTypeMismatch =>
        e.setTagValue(DATA_TYPE_MISMATCH_ERROR, ())
        e.dataTypeMismatch(e, checkRes)
      case TypeCheckResult.TypeCheckFailure(message) =>
        e.setTagValue(DATA_TYPE_MISMATCH_ERROR, ())
        val extraHint = operator.map(getHintForExpressionCoercion(_)).getOrElse("")
        e.failAnalysis(
          errorClass = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
          messageParameters = Map("sqlExpr" -> toSQLExpr(e), "msg" -> message, "hint" -> extraHint)
        )
      case checkRes: TypeCheckResult.InvalidFormat =>
        e.invalidFormat(checkRes)
    }
  }

  def getHintForOperatorCoercion(plan: LogicalPlan): String = {
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
          nonAnsiPlan.children.tail.zipWithIndex.foreach {
            case (child, ti) =>
              // Check if the data types match.
              dataTypes(child).zip(ref).zipWithIndex.foreach {
                case ((dt1, dt2), ci) =>
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

  def getDataTypesAreCompatibleFn(plan: LogicalPlan): (DataType, DataType) => Boolean = {
    val isUnion = plan.isInstanceOf[Union]
    if (isUnion) { (dt1: DataType, dt2: DataType) =>
      DataType.equalsStructurally(dt1, dt2, true)
    } else {
      // SPARK-18058: we shall not care about the nullability of columns
      (dt1: DataType, dt2: DataType) =>
        TypeCoercion.findWiderTypeForTwo(dt1.asNullable, dt2.asNullable).nonEmpty
    }
  }

  private def getHintForExpressionCoercion(plan: LogicalPlan): String = {
    if (!SQLConf.get.ansiEnabled) {
      ""
    } else {
      val nonAnsiPlan = getDefaultTypeCoercionPlan(plan)
      var issueFixedIfAnsiOff = true
      getAllExpressions(nonAnsiPlan).foreach(_.foreachUp {
        case e: Expression
            if e.getTagValue(DATA_TYPE_MISMATCH_ERROR).isDefined &&
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

  private def getAllExpressions(plan: LogicalPlan): Seq[Expression] = {
    plan match {
      // We only resolve `groupingExpressions` if `aggregateExpressions` is resolved first (See
      // `ResolveReferencesInAggregate`). We should check errors in `aggregateExpressions` first.
      case a: Aggregate => a.aggregateExpressions ++ a.groupingExpressions
      case _ => plan.expressions
    }
  }
}
