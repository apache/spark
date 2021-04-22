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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.DataType

case class Try(child: Expression) extends UnaryExpression {
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    ev.copy(code = code"""
      boolean ${ev.isNull} = true;
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      try {
        ${childGen.code}
        ${ev.isNull} = ${childGen.isNull};
        ${ev.value} = ${childGen.value};
      } catch (Exception e) {
      }"""
    )
  }

  override def eval(input: InternalRow): Any =
    try {
      child.eval(input)
    } catch {
      case _: Exception =>
        null
    }

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def sql: String = s"try${child.sql}"

  override def toString: String = s"try${child.toString}"
}
