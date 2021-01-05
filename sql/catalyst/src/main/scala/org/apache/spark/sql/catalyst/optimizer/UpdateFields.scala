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

package org.apache.spark.sql.catalyst.optimizer

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Expression, UpdateFields, WithField}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf


/**
 * Optimizes [[UpdateFields]] expression chains.
 */
object OptimizeUpdateFields extends Rule[LogicalPlan] {
  private def canOptimize(names: Seq[String]): Boolean = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      names.distinct.length != names.length
    } else {
      names.map(_.toLowerCase(Locale.ROOT)).distinct.length != names.length
    }
  }

  val optimizeUpdateFields: PartialFunction[Expression, Expression] = {
    case UpdateFields(structExpr, fieldOps)
      if fieldOps.forall(_.isInstanceOf[WithField]) &&
        canOptimize(fieldOps.map(_.asInstanceOf[WithField].name)) =>
      val caseSensitive = SQLConf.get.caseSensitiveAnalysis

      val withFields = fieldOps.map(_.asInstanceOf[WithField])
      val names = withFields.map(_.name)
      val values = withFields.map(_.valExpr)

      val newNames = mutable.ArrayBuffer.empty[String]
      val newValues = mutable.ArrayBuffer.empty[Expression]

      if (caseSensitive) {
        names.zip(values).reverse.foreach { case (name, value) =>
          if (!newNames.contains(name)) {
            newNames += name
            newValues += value
          }
        }
      } else {
        val nameSet = mutable.HashSet.empty[String]
        names.zip(values).reverse.foreach { case (name, value) =>
          val lowercaseName = name.toLowerCase(Locale.ROOT)
          if (!nameSet.contains(lowercaseName)) {
            newNames += name
            newValues += value
            nameSet += lowercaseName
          }
        }
      }

      val newWithFields = newNames.reverse.zip(newValues.reverse).map(p => WithField(p._1, p._2))
      UpdateFields(structExpr, newWithFields.toSeq)

    case UpdateFields(UpdateFields(struct, fieldOps1), fieldOps2) =>
      UpdateFields(struct, fieldOps1 ++ fieldOps2)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions(optimizeUpdateFields)
}

/**
 * Replaces [[UpdateFields]] expression with an evaluable expression.
 */
object ReplaceUpdateFieldsExpression extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case u: UpdateFields => u.evalExpr
  }
}
