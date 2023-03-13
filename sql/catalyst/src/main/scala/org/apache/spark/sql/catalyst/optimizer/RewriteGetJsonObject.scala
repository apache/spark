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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetJsonObject, JsonTuple, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.GET_JSON_OBJECT
import org.apache.spark.sql.types.StringType

/**
 * This rule rewrites multiple GetJsonObjects to a JsonTuple if their json expression is the same.
 */
object RewriteGetJsonObject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(GET_JSON_OBJECT), ruleId) {
    case p: Project =>
      val getJsonObjects = p.projectList.flatMap {
        _.collect {
          case gjo: GetJsonObject if gjo.rewrittenPathName.nonEmpty => gjo
        }
      }

      val groupedGetJsonObjects = getJsonObjects.groupBy(_.json).filter(_._2.size > 1)
      if (groupedGetJsonObjects.nonEmpty) {
        var newChild = p.child
        val keyValues: mutable.Map[Expression, AttributeReference] = mutable.Map.empty
        groupedGetJsonObjects.foreach {
          case (json, getJsonObjects) =>
            val generatorOutput = getJsonObjects.map { j =>
              val attr = AttributeReference(j.rewrittenPathName.get.toString, StringType)()
              keyValues.put(j.canonicalized, attr)
              attr
            }
            newChild = Generate(
              JsonTuple(json +: getJsonObjects.map(_.rewrittenPathName.get)),
              Nil,
              outer = false,
              Some(json.sql),
              generatorOutput,
              newChild)
        }

        val newProjectList = p.projectList.map { p =>
          p.transformDown {
            case gjo: GetJsonObject => keyValues.getOrElse(gjo.canonicalized, gjo)
          }.asInstanceOf[NamedExpression]
        }
        p.copy(newProjectList, newChild)
      } else {
        p
      }
  }
}
