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

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute


/**
 *
 * @param child the computation being performed
 * @param windowSpec the window spec definition
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.
 */
case class WindowExpression(child: Expression, name: String, windowSpec: WindowSpec)
    (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
  extends NamedExpression with trees.UnaryNode[Expression] {

  override type EvaluatedType = Any

  override def eval(input: Row) = child.eval(input)

  override def dataType = child.dataType
  override def nullable = child.nullable

  override def toAttribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable)(exprId, qualifiers)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String = s"$child $windowSpec AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs = exprId :: qualifiers :: Nil
}

case class WindowSpec(windowPartition: WindowPartition, windowFrame: Option[WindowFrame])

case class WindowPartition(partitionBy: Seq[Expression], sortBy: Seq[SortOrder])

sealed trait FrameType
case object ValueFrame extends FrameType
case object RowsFrame extends FrameType

case class WindowFrame(frameType: FrameType, preceding: Int, following: Int)
