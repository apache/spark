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
import org.apache.spark.sql.catalyst.util.{CollationFactory, UnsafeRowUtils}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

case class CollationKey(expr: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))
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

object CollationKey {
  /**
   * Recursively process the expression in order to recursively replace non-binary collated strings
   * with their associated collation key.
   */
  def injectCollationKey(expr: Expression): Expression = {
    injectCollationKey(expr, expr.dataType)
  }

  private def injectCollationKey(expr: Expression, dt: DataType): Expression = {
    dt match {
      // For binary stable expressions, no special handling is needed.
      case _ if UnsafeRowUtils.isBinaryStable(dt) =>
        expr

      // Inject CollationKey for non-binary collated strings.
      case _: StringType =>
        CollationKey(expr)

      // Recursively process struct fields for non-binary structs.
      case StructType(fields) =>
        val transformed = fields.zipWithIndex.map { case (f, i) =>
          val originalField = GetStructField(expr, i, Some(f.name))
          val injected = injectCollationKey(originalField, f.dataType)
          (f, injected, injected.fastEquals(originalField))
        }
        val anyChanged = transformed.exists { case (_, _, same) => !same }
        if (!anyChanged) {
          expr
        } else {
          val struct = CreateNamedStruct(
            transformed.flatMap { case (f, injected, _) =>
              Seq(Literal(f.name), injected)
            }.toImmutableArraySeq)
          if (expr.nullable) {
            If(IsNull(expr), Literal(null, struct.dataType), struct)
          } else {
            struct
          }
        }

      // Recursively process array elements for non-binary arrays.
      case ArrayType(et, containsNull) =>
        val param: NamedExpression = NamedLambdaVariable("a", et, containsNull)
        val funcBody: Expression = injectCollationKey(param, et)
        if (!funcBody.fastEquals(param)) {
          ArrayTransform(expr, LambdaFunction(funcBody, Seq(param)))
        } else {
          expr
        }

      // Joins are not supported on maps, so there's no special handling for MapType.
      case _ =>
        expr
    }
  }
}
