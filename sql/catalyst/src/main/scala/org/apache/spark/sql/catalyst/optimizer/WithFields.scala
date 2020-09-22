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

import org.apache.spark.sql.catalyst.expressions.{Expression, WithFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf


/**
 * Optimizes [[WithFields]] expression chains.
 */
object OptimizeWithFields extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case WithFields(structExpr, names, values)
        if names.map(_.toLowerCase(Locale.ROOT)).distinct.length != names.length =>
      val caseSensitive = SQLConf.get.caseSensitiveAnalysis

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

      WithFields(structExpr, names = newNames.reverse.toSeq, valExprs = newValues.reverse.toSeq)

    case WithFields(WithFields(struct, names1, valExprs1), names2, valExprs2) =>
      WithFields(struct, names1 ++ names2, valExprs1 ++ valExprs2)
  }
}

/**
 * Replaces [[WithFields]] expression with an evaluable expression.
 */
object ReplaceWithFieldsExpression extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case w: WithFields => w.evalExpr
  }
}
