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
import org.apache.spark.sql.types.{DataType, IntegerType}

class TryCastSuite extends AnsiCastSuiteBase {
  override protected def cast(v: Any, targetType: DataType, timeZoneId: Option[String]) = {
    v match {
      case lit: Expression => TryCast(lit, targetType, timeZoneId)
      case _ => TryCast(Literal(v), targetType, timeZoneId)
    }
  }

  override def isTryCast: Boolean = true

  override protected def setConfigurationHint: String = ""

  override def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      inputRow: InternalRow,
      expectedErrMsg: String): Unit = {
    checkEvaluation(expression, null, inputRow)
  }

  override def checkCastToBooleanError(l: Literal, to: DataType, tryCastResult: Any): Unit = {
    checkEvaluation(cast(l, to), tryCastResult, InternalRow(l.value))
  }

  override def checkCastToNumericError(l: Literal, to: DataType,
      expectedDataTypeInErrorMsg: DataType, tryCastResult: Any): Unit = {
    checkEvaluation(cast(l, to), tryCastResult, InternalRow(l.value))
  }

  test("try_cast: to_string") {
    assert(TryCast(Literal("1"), IntegerType).toString == "try_cast(1 as int)")
  }
}
