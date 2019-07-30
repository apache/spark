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

package org.apache.spark.sql.catalyst.implies

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.mv.Implies

case class ExprContainer(expr1: String, expr2: String)

class ImpliesSuite extends SparkFunSuite {
  val parser: ParserInterface = CatalystSqlParser

  private val prefix = "select * from tbl where "

  private val positiveTestExprs: Seq[ExprContainer] Tuple2 Boolean = Seq(
    ExprContainer("(a > 10)",
                  "(a > 5)"),
    ExprContainer("(a < 10)",
                  "(a < 11)"),
    ExprContainer("(a > 10 and c < 5) or (a > 20)",
                  "(a > 0 or c < 5)"),
    ExprContainer("(a > 100) and (a > 20 or b > 100)",
                  "(a > 20)")
  ) -> true

  private val negativeTestExprs: Seq[ExprContainer] Tuple2 Boolean = Seq(
    ExprContainer("(a > 100)",
                  "(a > 200)"),
    ExprContainer("(a < 100)",
                  "(a < 20)"),
    ExprContainer("(a < 100) and (a < 20 or b > 100)",
                  "(a < 20)")
  ) -> false

  Seq(positiveTestExprs, negativeTestExprs).foreach {
    case (exprs, assertValue) =>
      exprs.foreach {
        case ExprContainer(expr1, expr2) =>
          validate(expr1, expr2, assertValue)
        case _ =>
      }
  }

  def validate(exprText1: String, exprText2: String, toAssert: Boolean = true): Unit = {
    test(s"Test implies expr1=$exprText1, expr2=$exprText2") {
      val exprs1 = extractExpression(exprText1)
      val exprs2 = extractExpression(exprText2)
      assert(exprs1.isDefined)
      assert(exprs2.isDefined)
      val e1 = exprs1.get
      val e2 = exprs2.get
      assert(new Implies(e1, e2).implies == toAssert)
    }
  }

  private def extractExpression(predicate: String) : Option[Seq[Expression]] = {

    val sql = s"$prefix $predicate"
    val plan: LogicalPlan = parser.parsePlan(sql)
    plan.find(_.isInstanceOf[Filter]).flatMap {
      filter =>
        filter match {
          case f: Filter => Option(Seq(f.condition))
          case _ => None
        }
    }
  }
}

