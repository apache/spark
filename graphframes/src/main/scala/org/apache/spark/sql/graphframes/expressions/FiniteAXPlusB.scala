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

package org.apache.spark.sql.graphframes.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.TernaryExpression
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType

case class FiniteAXPlusB(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with CodegenFallback {
  override def dataType: DataType = LongType

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): Expression = copy(newFirst, newSecond, newThird)

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    val a = input1.asInstanceOf[Long]
    val x = input2.asInstanceOf[Long]
    val b = input3.asInstanceOf[Long]

    FiniteAXPlusB.axpb(a, x, b)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val a = ctx.freshName("a")
    val x = ctx.freshName("x")
    val b = ctx.freshName("b")
    val r = ctx.freshName("r")

    val aGenCode = first.genCode(ctx)
    val xGenCode = second.genCode(ctx)
    val bGenCode = third.genCode(ctx)

    ev.copy(code = code"""
      ${aGenCode.code}
      ${xGenCode.code}
      ${bGenCode.code}
      long $a = ${aGenCode.value};
      long $x = ${xGenCode.value};
      long $b = ${bGenCode.value};
      long $r = 0L;
      long irrpoly = 0x1bL;
      while ($x != 0L) {
        if (($x & 1L) != 0L) {
          $r ^= $a;
        }
        $x = ($x >>> 1) & 0x7fffffffffffffffL;
        if (($a & (1L << 63)) != 0L) {
          $a = ($a << 1) ^ irrpoly;
        } else {
          $a <<= 1;
        }
      }
      boolean ${ev.isNull} = false;
      long ${ev.value} = $r ^ $b;
    """)
  }
}

object FiniteAXPlusB extends Serializable {
  def axpb(a: Long, x: Long, b: Long): Long = {
    var r = 0L
    val irrpoly = 0x1bL
    var currentA = a
    var currentX = x
    while (currentX != 0L) {
      if ((currentX & 1L) != 0L) {
        r ^= currentA
      }
      currentX = (currentX >>> 1) & 0x7fffffffffffffffL
      if ((currentA & (1L << 63)) != 0L) {
        currentA = (currentA << 1) ^ irrpoly
      } else {
        currentA <<= 1
      }
    }
    r ^ b
  }
}
