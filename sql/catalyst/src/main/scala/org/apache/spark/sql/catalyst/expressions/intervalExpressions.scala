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
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType, DoubleType}
import org.apache.spark.unsafe.types.CalendarInterval

abstract class IntervalNumOperation(
    interval: Expression,
    num: Expression,
    operation: (CalendarInterval, Double) => CalendarInterval,
    operationName: String)
  extends BinaryExpression with ImplicitCastInputTypes with Serializable {
  override def left: Expression = interval
  override def right: Expression = num

  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType, DoubleType)
  override def dataType: DataType = CalendarIntervalType

  override def nullable: Boolean = true

  override def nullSafeEval(interval: Any, num: Any): Any = {
    try {
      operation(interval.asInstanceOf[CalendarInterval], num.asInstanceOf[Double])
    } catch {
      case _: java.lang.ArithmeticException => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (interval, num) => {
      s"""
        try {
          ${ev.value} = $interval.$operationName($num);
        } catch (java.lang.ArithmeticException e) {
          ${ev.isNull} = true;
        }
      """
    })
  }

  override def prettyName: String = operationName + "_interval"
}

case class MultiplyInterval(interval: Expression, num: Expression)
  extends IntervalNumOperation(
    interval,
    num,
    (i: CalendarInterval, n: Double) => i.multiply(n),
    "multiply")

case class DivideInterval(interval: Expression, num: Expression)
  extends IntervalNumOperation(
    interval,
    num,
    (i: CalendarInterval, n: Double) => i.divide(n),
    "divide")
