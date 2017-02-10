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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class TypedScalaUDF[T1, R](
    function: (T1) => R,
    t1Encoder: ExpressionEncoder[T1],
    rEncoder: ExpressionEncoder[R],
    child1: Expression,
    inputTypes: Seq[DataType] = Nil
  ) extends Expression with ImplicitCastInputTypes with NonSQLExpression {

  override def children: Seq[Expression] = Seq(child1)

  // something is wrong with encoders and Option so this is used to handle that
  private val t1IsOption = t1Encoder.clsTag == implicitly[ClassTag[Option[_]]]
  private val rIsOption = rEncoder.clsTag == implicitly[ClassTag[Option[_]]]

  override val dataType: DataType = if (rEncoder.flat || rIsOption) {
    rEncoder.schema.head.dataType
  } else {
    rEncoder.schema
  }

  override val nullable: Boolean = if (rEncoder.flat) {
    rEncoder.schema.head.nullable
  } else {
    true
  }

  val boundT1Encoder: ExpressionEncoder[T1] = t1Encoder.resolveAndBind()

  def internalRow(x: Any): InternalRow = InternalRow(x)

  override def eval(input: InternalRow): Any = {
    val eval1 = child1.eval(input)
    // println("eval1 " + eval1)
    val rowIn = if (t1Encoder.flat || t1IsOption) {
      InternalRow(eval1)
    } else {
      eval1.asInstanceOf[InternalRow]
    }
    // println("rowIn" + rowIn)
    val t1 = boundT1Encoder.fromRow(rowIn)
    // println("t1 " + t1)
    val r = function(t1)
    // println("r " + r)
    val rowOut = rEncoder.toRow(r).copy() // not entirely sure why i need the copy but i do
    // println("rowOut " + rowOut)
    val result = if (rEncoder.flat || rIsOption) rowOut.get(0, dataType) else rowOut
    // println("result " + result)
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val typedScalaUDF = ctx.addReferenceObj("typedScalaUDF", this,
      classOf[TypedScalaUDF[_, _]].getName)

    // codegen for children expressions
    val eval1 = child1.genCode(ctx)
    val evalCode1 = eval1.code.mkString

    // codegen for this expression
    val rowClass = classOf[InternalRow].getName
    val rowInTerm = ctx.freshName("rowIn")
    val rowIn = if (t1Encoder.flat || t1IsOption) {
      s"""$rowClass $rowInTerm = ${eval1.isNull} ?
            ${typedScalaUDF}.internalRow(null) :
            ${typedScalaUDF}.internalRow(${eval1.value});"""
    } else {
      s"""$rowClass $rowInTerm =($rowClass) ${eval1.value};"""
    }
    val t1Term = ctx.freshName("t1")
    val t1 = s"Object $t1Term = ${typedScalaUDF}.boundT1Encoder().fromRow($rowInTerm);"
    val rTerm = ctx.freshName("r")
    val r = s"Object $rTerm = ${typedScalaUDF}.function().apply($t1Term);"
    val rowOutTerm = ctx.freshName("rowOut")
    val rowOut = s"$rowClass $rowOutTerm = ${typedScalaUDF}.rEncoder().toRow($rTerm);"
    val resultTerm = ctx.freshName("result")
    val result = if (rEncoder.flat || rIsOption) {
      s"Object $resultTerm = ${rowOutTerm}.get(0, ${typedScalaUDF}.dataType());"
    } else {
      s"Object $resultTerm = ${rowOutTerm};"
    }

    // put it all in place
    ev.copy(code = s"""
      $evalCode1
      // System.out.println("eval1 is " + ${eval1.value});
      $rowIn
      // System.out.println("rowIn is " + $rowInTerm);
      $t1
      // System.out.println("t1 is " + $t1Term);
      $r
      // System.out.println("r is " + $rTerm);
      $rowOut
      // System.out.println("rowOut is " + $rowOutTerm);
      $result
      // System.out.println("result is " + $resultTerm);

      boolean ${ev.isNull} = $resultTerm == null;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = (${ctx.boxedType(dataType)}) $resultTerm;
      }
    """)
  }

  override def toString: String = s"TypedUDF(${children.mkString(", ")})"
}
