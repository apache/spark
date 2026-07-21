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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * A test-only boolean expression that simulates a throwable UDF such as assert_true: it is
 * deterministic but marked [[Expression.throwable]], returns true when the child evaluates
 * to true, and throws when the child evaluates to false or null.
 *
 * Built-in throwable functions such as raise_error are not marked throwable today, so this
 * expression is used instead to verify that streamed-side join condition hoisting
 * ([[StreamedSideJoinCondition]]) never relocates a throwable conjunct to the pre-probe
 * guard, where it would be evaluated for streamed rows that have no buffered match and
 * would never evaluate it otherwise.
 */
case class TestThrowableUDF(child: Expression) extends UnaryExpression {

  override lazy val throwable: Boolean = true

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val v = child.eval(input)
    if (v != null && v.asInstanceOf[Boolean]) {
      true
    } else {
      throw new RuntimeException("TestThrowableUDF evaluated to false/null")
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    ExprCode(
      code = code"""
         |${childGen.code}
         |if (${childGen.isNull} || !${childGen.value}) {
         |  throw new RuntimeException("TestThrowableUDF evaluated to false/null");
         |}
         |""".stripMargin,
      isNull = FalseLiteral,
      value = TrueLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
