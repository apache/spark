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

import org.apache.spark.sql.catalyst.expressions.BinaryExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType

/**
 * Mandal, Aritra, and Mohammad Al Hasan. "A distributed k-core decomposition algorithm on spark."
 * 2017 IEEE International Conference on Big Data (Big Data). IEEE, 2017.
 *
 * @param left
 *   array of nbrs cores
 * @param right
 *   core of the vertex
 */
private[spark] case class KCoreMerge(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback {
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(newLeft, newRight)

  override def dataType: DataType = IntegerType

  /**
   * Each node initializes its core value with the degree of itself. Each node (say u) then sends
   * messages to its neighbors v in N(u) with the current estimate of its (u's) core value. For an
   * undirected graph with m edges, there can be at most a total of 2m messages that have been
   * sent during a message passing session. Upon receiving all the messages from its neighbors,
   * the vertex u computes the largest value l such that the number of neighbors of u whose
   * current core value estimate is `l` or larger is equal or higher than `l`
   */
  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val arrayOfElements = input1.asInstanceOf[ArrayData].toIntArray()
    val currentCore = input2.asInstanceOf[Int]

    val counts = arrayOfElements.foldLeft(new Array[Int](currentCore + 1))((acc, el) =>
      if (el > currentCore) {
        acc(currentCore) = acc(currentCore) + 1
        acc
      } else {
        acc(el) = acc(el) + 1
        acc
      })

    var currentWeight = 0
    for (i <- currentCore to 1 by -1) {
      currentWeight += counts(i)
      if (i <= currentWeight) {
        return i
      }
    }

    return 0
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayOfElements = ctx.freshName("arrayOfElements")
    val currentCore = ctx.freshName("currentCore")
    val counts = ctx.freshName("counts")
    val currentWeight = ctx.freshName("currentWeight")
    val el = ctx.freshName("el")
    val i = ctx.freshName("i")

    val leftGenCode = left.genCode(ctx)
    val rightGenCode = right.genCode(ctx)
    ev.copy(code"""
         |${leftGenCode.code}
         |${rightGenCode.code}
         |int ${ev.value} = 0;
         |boolean ${ev.isNull} = false;
         |int[] $arrayOfElements = ${leftGenCode.value}.toIntArray();
         |int $currentCore = ${rightGenCode.value};
         |
         |int[] $counts = new int[$currentCore + 1];
         |for (int $i = 0; $i < $arrayOfElements.length; $i++) {
         |  int $el = $arrayOfElements[$i];
         |  if ($el > $currentCore) {
         |    $counts[$currentCore] += 1;
         |  } else {
         |    $counts[$el] += 1;
         |  }
         |}
         |
         |int $currentWeight = 0;
         |for (int $i = $currentCore; $i >= 1; $i--) {
         |  $currentWeight += $counts[$i];
         |  if ($i <= $currentWeight) {
         |    ${ev.value} = $i;
         |    break;
         |  }
         |}
       """.stripMargin)
  }
}
