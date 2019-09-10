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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.DataType

/**
 * This bound reference points to a parameterized slot in an input tuple. It is used in
 * common sub-expression elimination. When some common sub-expressions have same structural
 * but different slots of input tuple, we replace `BoundReference` with this parameterized
 * version. The slot position is parameterized and is given at runtime.
 */
case class ParameterizedBoundReference(ordinalParam: String, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"input[$ordinalParam, ${dataType.simpleString}, $nullable]"

  override def eval(input: InternalRow): Any = {
    throw new UnsupportedOperationException(
      "ParameterizedBoundReference does not implement eval")
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    assert(ctx.currentVars == null && ctx.INPUT_ROW != null,
      "ParameterizedBoundReference can not be used in whole-stage codegen yet.")
    val javaType = JavaCode.javaType(dataType)
    val value = CodeGenerator.getValue(ctx.INPUT_ROW, dataType, ordinalParam)
    if (nullable) {
      ev.copy(code =
        code"""
              |boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinalParam);
              |$javaType ${ev.value} = ${ev.isNull} ?
              |  ${CodeGenerator.defaultValue(dataType)} : ($value);
         """.stripMargin)
    } else {
      ev.copy(code = code"$javaType ${ev.value} = $value;", isNull = FalseLiteral)
    }
  }
}
