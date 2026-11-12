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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType, StructType}

/**
 * Object used to transform the given [[Unpivot]] node to an [[Expand]] node.
 */
object UnpivotTransformer extends SQLConfHelper {

  /**
   * Construct an [[Expand]] node from the given [[Unpivot]] node. Do that by:
   *  1. Constructing expressions for [[Expand]] out of [[aliases]] and [[values]].
   *  2. Constructing output attributes.
   *  3. Creating the [[Expand]] node using the expressions, outputs and the [[Unpivot.child]].
   */
  def apply(
      ids: Seq[NamedExpression],
      values: Seq[Seq[NamedExpression]],
      aliases: Option[Seq[Option[String]]],
      variableColumnName: String,
      valueColumnNames: Seq[String],
      child: LogicalPlan): Expand = {

    val expressions: Seq[Seq[Expression]] =
      values.zip(aliases.getOrElse(values.map(_ => None))).map {
        case (values, Some(alias)) => (ids :+ Literal(alias)) ++ values
        case (Seq(value), None) => (ids :+ Literal(value.name)) :+ value
        case (values, None) =>
          val stringOfValues = values
            .map { value =>
              value.name
            }
            .mkString("_")
          (ids :+ Literal(stringOfValues)) ++ values
      }

    val variableAttribute =
      AttributeReference(variableColumnName, StringType, nullable = false)()
    val valueAttributes = valueColumnNames.zipWithIndex.map {
      case (valueColumnName, index) =>
        AttributeReference(
          valueColumnName,
          valueDataType(values.map(_(index).dataType)),
          values.map(_(index)).exists(_.nullable)
        )()
    }

    val output = (ids.map(_.toAttribute) :+ variableAttribute) ++ valueAttributes

    Expand(expressions, output, child)
  }

  /**
   * Get the data type of a value column out of the data types of its values, which are the same
   * up to nullability. Like [[Union]], merge the nested nullability of all values without casting
   * them, keeping the field names and metadata of the first value. In the legacy mode, take the
   * data type of the first value.
   */
  private def valueDataType(dataTypes: Seq[DataType]): DataType = {
    if (conf.getConf(SQLConf.LEGACY_UNPIVOT_USE_FIRST_VALUE_NESTED_NULLABILITY)) {
      dataTypes.head
    } else {
      dataTypes.reduce(StructType.unionLikeMerge)
    }
  }
}
