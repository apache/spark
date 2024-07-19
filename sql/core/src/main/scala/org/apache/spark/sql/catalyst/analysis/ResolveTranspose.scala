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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Transpose}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPOSE
import org.apache.spark.sql.types.{DataType, StringType}


class ResolveTranspose(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def leastCommonType(dataTypes: Seq[DataType]): DataType = {
    dataTypes.reduce(TypeCoercion.findTightestCommonType(_, _).getOrElse(StringType))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(TRANSPOSE)) {
    case transpose @ Transpose(indexColumn, child, _, _) =>
      // Cast the index column to StringType
      val indexColumnAsString = Cast(indexColumn, StringType)
      val aliasIndexColumnAsString = Alias(indexColumnAsString, "indexColumnAsString")()

      // Collect index column values (as new column names in transposed frame)
      val projectPlan = Project(Seq(aliasIndexColumnAsString.asInstanceOf[NamedExpression]), child)
      val queryExecution = sparkSession.sessionState.executePlan(projectPlan)
      val collectedValues = queryExecution.toRdd.collect().map(row => row.getString(0)).toSeq

      // Determine the least common type of the non-index columns
      val nonIndexColumns = child.output.filterNot(
        _.name == indexColumn.asInstanceOf[Attribute].name)
      val nonIndexTypes = nonIndexColumns.map(_.dataType)
      val commonType = leastCommonType(nonIndexTypes)

      // Cast non-index columns to the least common type
      val castedChild = child.transformExpressions {
        case a: Attribute if nonIndexColumns.map(_.name).contains(a.name) =>
          Cast(a, commonType)
      }

      // Collect original non-index column names
      val originalColNames = nonIndexColumns.map(_.name)

      // Prune the index column from the resulting plan
      val prunedProject = Project(nonIndexColumns.map(_.asInstanceOf[NamedExpression]), castedChild)

      // Construct output attributes
      val indexColAttr = AttributeReference(
        indexColumnAsString.name, indexColumnAsString.dataType)()
      val valueAttrs = collectedValues.map { value =>
        AttributeReference(
          value,
          value.getClass.getDeclaredField("dataType").get(value).asInstanceOf[DataType]
        )()
      }
      val outputAttributes = indexColAttr +: valueAttrs

      Transpose(
        indexColumn,
        prunedProject,
        originalColNames = originalColNames,
        output = outputAttributes
      )
  }
}
