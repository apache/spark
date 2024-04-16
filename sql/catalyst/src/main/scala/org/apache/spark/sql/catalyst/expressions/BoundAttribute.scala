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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"input[$ordinal, ${dataType.simpleString}, $nullable]"

  private val accessor: (InternalRow, Int) => Any = InternalRow.getAccessor(dataType, nullable)

  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: InternalRow): Any = {
    accessor(input, ordinal)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (ctx.currentVars != null && ctx.currentVars(ordinal) != null) {
      val oev = ctx.currentVars(ordinal)
      ev.isNull = oev.isNull
      ev.value = oev.value
      ev.copy(code = oev.code)
    } else {
      assert(ctx.INPUT_ROW != null, "INPUT_ROW and currentVars cannot both be null.")
      val javaType = JavaCode.javaType(dataType)
      val value = CodeGenerator.getValue(ctx.INPUT_ROW, dataType, ordinal.toString)
      if (nullable) {
        ev.copy(code =
          code"""
             |boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinal);
             |$javaType ${ev.value} = ${ev.isNull} ?
             |  ${CodeGenerator.defaultValue(dataType)} : ($value);
           """.stripMargin)
      } else {
        ev.copy(code = code"$javaType ${ev.value} = $value;", isNull = FalseLiteral)
      }
    }
  }
}

object BindReferences extends Logging {

  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      val ordinal = input.indexOf(a.exprId)
      if (ordinal == -1) {
        if (allowFailures) {
          a
        } else {
          throw SparkException.internalError(
            s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
      } else {
        BoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
  def bindReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    expressions.map(BindReferences.bindReference(_, input))
  }
}
