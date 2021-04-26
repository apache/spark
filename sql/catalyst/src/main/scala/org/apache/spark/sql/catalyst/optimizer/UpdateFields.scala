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
      val newValues = mutable.HashMap.empty[String, Expression]
      // Used to remember the casing of the last instance
      val nameMap = mutable.HashMap.empty[String, String]

      names.zip(values).foreach { case (name, value) =>
        val normalizedName = if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
        if (nameMap.contains(normalizedName)) {
          newValues += normalizedName -> value
        } else {
          newNames += normalizedName
          newValues += normalizedName -> value
        }
        nameMap += normalizedName -> name
      }

      val newWithFields = newNames.map(n => WithField(nameMap(n), newValues(n)))
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
