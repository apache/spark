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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{StringType, NumericType}

/**
 * Generates bytecode for an [[Ordering]] of [[Row Rows]] for a given set of
 * [[Expression Expressions]].
 */
object GenerateOrdering extends CodeGenerator[Seq[SortOrder], Ordering[Row]] with Logging {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

 protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    in.map(ExpressionCanonicalizer(_).asInstanceOf[SortOrder])

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(ordering: Seq[SortOrder]): Ordering[Row] = {
    val a = newTermName("a")
    val b = newTermName("b")
    val comparisons = ordering.zipWithIndex.map { case (order, i) =>
      val evalA = expressionEvaluator(order.child)
      val evalB = expressionEvaluator(order.child)

      val compare = order.child.dataType match {
        case _: NumericType =>
          q"""
          val comp = ${evalA.primitiveTerm} - ${evalB.primitiveTerm}
          if(comp != 0) {
            return ${if (order.direction == Ascending) q"comp.toInt" else q"-comp.toInt"}
          }
          """
        case StringType =>
          if (order.direction == Ascending) {
            q"""return ${evalA.primitiveTerm}.compare(${evalB.primitiveTerm})"""
          } else {
            q"""return ${evalB.primitiveTerm}.compare(${evalA.primitiveTerm})"""
          }
      }

      q"""
        i = $a
        ..${evalA.code}
        i = $b
        ..${evalB.code}
        if (${evalA.nullTerm} && ${evalB.nullTerm}) {
          // Nothing
        } else if (${evalA.nullTerm}) {
          return ${if (order.direction == Ascending) q"-1" else q"1"}
        } else if (${evalB.nullTerm}) {
          return ${if (order.direction == Ascending) q"1" else q"-1"}
        } else {
          $compare
        }
      """
    }

    val q"class $orderingName extends $orderingType { ..$body }" = reify {
      class SpecificOrdering extends Ordering[Row] {
        val o = ordering
      }
    }.tree.children.head

    val code = q"""
      class $orderingName extends $orderingType {
        ..$body
        def compare(a: $rowType, b: $rowType): Int = {
          var i: $rowType = null // Holds current row being evaluated.
          ..$comparisons
          return 0
        }
      }
      new $orderingName()
      """
    logDebug(s"Generated Ordering: $code")
    toolBox.eval(code).asInstanceOf[Ordering[Row]]
  }
}
