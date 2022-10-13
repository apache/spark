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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.types._

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
    valueExpression: Expression) extends BinaryExpression with BloomRuntimeFilterHelper {

  override def nullable: Boolean = true
  override def left: Expression = bloomFilterExpression
  override def right: Expression = valueExpression
  override def prettyName: String = "might_contain"
  override def dataType: DataType = BooleanType

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (BinaryType, NullType) | (NullType, LongType) | (NullType, NullType) |
           (BinaryType, LongType) =>
        bloomFilterExpression match {
          case e : Expression if e.foldable => TypeCheckResult.TypeCheckSuccess
          case subquery : PlanExpression[_] if !subquery.containsPattern(OUTER_REFERENCE) =>
            TypeCheckResult.TypeCheckSuccess
          case _ =>
            TypeCheckResult.TypeCheckFailure(s"The Bloom filter binary input to $prettyName " +
              "should be either a constant value or a scalar subquery expression")
        }
      case _ => TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
        s"been ${BinaryType.simpleString} followed by a value with ${LongType.simpleString}, " +
        s"but it's [${left.dataType.catalogString}, ${right.dataType.catalogString}].")
    }
  }

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression,
      valueExpression = newValueExpression)

  // The bloom filter created from `bloomFilterExpression`.
  @transient private lazy val bloomFilter = {
    val bytes = bloomFilterExpression.eval().asInstanceOf[Array[Byte]]
    if (bytes == null) null else deserialize(bytes)
  }

  override def eval(input: InternalRow): Any = {
    internalEval(input, bloomFilter, valueExpression)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    internalDoGenCode(ctx, ev, bloomFilter, valueExpression, dataType)
  }

}
