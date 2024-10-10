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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{ArrayType, DataType}

trait TaggingExpression extends UnaryExpression {
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = child.genCode(ctx)

  override def eval(input: InternalRow): Any = child.eval(input)
}

case class KnownNullable(child: Expression) extends TaggingExpression {
  override def nullable: Boolean = true

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override protected def withNewChildInternal(newChild: Expression): KnownNullable =
    copy(child = newChild)
}

case class KnownNotNull(child: Expression) extends TaggingExpression {
  override def nullable: Boolean = false

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx).copy(isNull = FalseLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): KnownNotNull =
    copy(child = newChild)
}

case class KnownNotContainsNull(child: Expression) extends TaggingExpression {
  override def dataType: DataType =
    child.dataType.asInstanceOf[ArrayType].copy(containsNull = false)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override protected def withNewChildInternal(newChild: Expression): KnownNotContainsNull =
    copy(child = newChild)
}

case class KnownFloatingPointNormalized(child: Expression) extends TaggingExpression {
  override protected def withNewChildInternal(newChild: Expression): KnownFloatingPointNormalized =
    copy(child = newChild)
}
