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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

/**
 * An internal scalar function that returns the membership check result (either true or false)
 * for values of `valueExpression` in the Bloom filter represented by `bloomFilterExpression`.
 * Not that since the function is "might contain", always returning true regardless is not
 * wrong.
 * Note that this expression requires that `bloomFilterExpression` is either a constant value or
 * an uncorrelated scalar subquery. This is sufficient for the Bloom filter join rewrite.
 *
 * @param bloomFilterExpression the Binary data of Bloom filter.
 * @param valueExpression the Long value to be tested for the membership of `bloomFilterExpression`.
 */
case class BloomFilterMightContain(
    bloomFilterExpression: Expression,
    valueExpression: Expression) extends BinaryExpression {

  override def nullable: Boolean = true
  override def left: Expression = bloomFilterExpression
  override def right: Expression = valueExpression
  override def prettyName: String = "might_contain"
  override def dataType: DataType = BooleanType

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = (left.dataType, right.dataType) match {
      case (BinaryType, NullType) | (NullType, LongType) | (NullType, NullType) |
           (BinaryType, LongType) => TypeCheckResult.TypeCheckSuccess
      case _ => TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
        s"been ${BinaryType.simpleString} followed by a value with ${LongType.simpleString}, " +
        s"but it's [${left.dataType.catalogString}, ${right.dataType.catalogString}].")
    }
    if (typeCheckResult.isFailure) {
      return typeCheckResult
    }
    bloomFilterExpression match {
      case e : Expression if e.foldable => TypeCheckResult.TypeCheckSuccess
      case subquery : PlanExpression[_] if !subquery.containsPattern(OUTER_REFERENCE) =>
        TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"The Bloom filter binary input to $prettyName " +
          s"should be either a constant value or a scalar subquery expression")
    }
  }

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression,
      valueExpression = newValueExpression)

  // The bloom filter created from `bloomFilterExpression`.
  @transient private var bloomFilter: BloomFilter = _

  override def nullSafeEval(bloomFilterBytes: Any, value: Any): Any = {
    if (bloomFilter == null) {
      bloomFilter = deserialize(bloomFilterBytes.asInstanceOf[Array[Byte]])
    }
    bloomFilter.mightContainLong(value.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val thisObj = ctx.addReferenceObj("thisObj", this)
    nullSafeCodeGen(ctx, ev, (bloomFilterBytes, value) => {
      s"\n${ev.value} = (Boolean) $thisObj.nullSafeEval($bloomFilterBytes, $value);\n"
    })
  }

  final def deserialize(bytes: Array[Byte]): BloomFilter = {
    val in = new ByteArrayInputStream(bytes)
    val bloomFilter = BloomFilter.readFrom(in)
    in.close()
    bloomFilter
  }

}
