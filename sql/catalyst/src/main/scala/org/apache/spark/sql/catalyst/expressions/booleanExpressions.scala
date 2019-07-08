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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.BooleanType

/**
 * String to indicate which boolean test selected.
 */
object BooleanTest {
  val TRUE = "TRUE"
  val FALSE = "FALSE"
  val UNKNOWN = "UNKNOWN"

  def calculate(input: Any, booleanValue: String): Boolean = {
    booleanValue match {
      case TRUE => input == true
      case FALSE => input == false
      case UNKNOWN => input == null
      case _ => false
    }
  }
}

/**
 * Test the value of an expression is true, false, or unknown.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, booleanValue) - Returns true if `expr` equals booleanValue, " +
    "or false otherwise.",
  arguments = """
    Arguments:
      * expr - a boolean expression
      * booleanValue - a boolean value represented by a string. booleanValue must be one
          of TRUE, FALSE and UNKNOWN.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1, true);
       false
  """)
case class BooleanTest(child: Expression, booleanValue: String)
  extends UnaryExpression with Predicate {

  override def eval(input: InternalRow): Any = {
    BooleanTest.calculate(child.eval(input), booleanValue)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, input =>
      s"""
        org.apache.spark.sql.catalyst.expressions.BooleanTest.calculate($input, "$booleanValue")
      """
    )
  }

  override def sql: String = s"(${child.sql} IS $booleanValue)"
}

