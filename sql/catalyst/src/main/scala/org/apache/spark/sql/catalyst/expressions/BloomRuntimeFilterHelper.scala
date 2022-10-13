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

import java.io.ByteArrayInputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, JavaCode, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.sketch.BloomFilter

trait BloomRuntimeFilterHelper {

  def internalEval(input: InternalRow, bloomFilter: BloomFilter,
    evalExpression: Expression): Any = {
    if (bloomFilter == null) {
      null
    } else {
      val value = evalExpression.eval(input)
      if (value == null) null else bloomFilter.mightContainLong(value.asInstanceOf[Long])
    }
  }

  def internalDoGenCode(ctx: CodegenContext, ev: ExprCode,
    bloomFilter: BloomFilter, evalExpression: Expression, dataType: DataType): ExprCode = {
    if (bloomFilter == null) {
      ev.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
    } else {
      val bf = ctx.addReferenceObj("bloomFilter", bloomFilter, classOf[BloomFilter].getName)
      val valueEval = evalExpression.genCode(ctx)
      ev.copy(code = code"""
      ${valueEval.code}
      boolean ${ev.isNull} = ${valueEval.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = $bf.mightContainLong((Long)${valueEval.value});
      }""")
    }
  }

  def deserialize(bytes: Array[Byte]): BloomFilter = {
    val in = new ByteArrayInputStream(bytes)
    val bloomFilter = BloomFilter.readFrom(in)
    in.close()
    bloomFilter
  }

}
