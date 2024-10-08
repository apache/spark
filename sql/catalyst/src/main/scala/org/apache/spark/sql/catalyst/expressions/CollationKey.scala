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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.types.StringTypeWithCaseAccentSensitivity
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class CollationKey(expr: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCaseAccentSensitivity(/* supportsTrimCollation = */ true))
  override def dataType: DataType = BinaryType

  final lazy val collationId: Int = expr.dataType match {
    case st: StringType =>
      st.collationId
  }

  override def nullSafeEval(input: Any): Any =
    CollationFactory.getCollationKeyBytes(input.asInstanceOf[UTF8String], collationId)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"CollationFactory.getCollationKeyBytes($c, $collationId)")
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(expr = newChild)
  }

  override def child: Expression = expr
}
