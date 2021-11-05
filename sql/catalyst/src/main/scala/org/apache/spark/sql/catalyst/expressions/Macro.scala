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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * @param name the macro name
 * @param body expression wrapper
 * @param fields columns definition
 * @param input the attribute to compute
 * @param idxMapUnresolvedFields maps to navigate to fields to replace
 */
@ExpressionDescription(
  usage = "create temporary macro ....",
  examples = "create temporary macro wrapperUpper(id string) upper(id)",
  since = "3.3.0")
case class Macro(name: String,
                 body: Expression,
                 fields: Seq[StructField],
                 input: Seq[Expression],
                 idxMapUnresolvedFields: Map[Int, UnresolvedAttribute]
                 ) extends UnaryExpression
                   with NullIntolerant
                   with CodegenFallback {

  if (fields.size != input.size) {
    throw new
        AnalysisException(s"Input column size is ${input.size} " +
          s"but function definition input size is ${fields.size}")
  }

  fields.zip(input).zipWithIndex.foreach{
    tuple => {
      if (!tuple._1._1.dataType.acceptsType(tuple._1._2.dataType)) {
        throw new AnalysisException(s"Expect dataType is ${tuple._1._1.dataType} " +
          s"but found ${tuple._1._2.dataType} , column index: ${tuple._2}")
      }
    }
  }

  lazy val childExpr: Expression = {
    val inter = body.transformUp {
      case unresolvedAttribute: UnresolvedAttribute =>
        val name = unresolvedAttribute.name
        val filters = idxMapUnresolvedFields.filter(tuple => tuple._2.name == name)
        if(filters.nonEmpty) {
          input(filters.head._1)
        } else {
          throw new AnalysisException(s"Not found $unresolvedAttribute")
        }

      case other => other
    }
    inter
  }

  override def child: Expression = childExpr

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = child.dataType

  override def prettyName: String = name

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(body = newChild )
}
