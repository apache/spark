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

import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

/**
 * An internal function that returns aggregate operations(min, max and bloom filter) result
 * for `structTypeExpression`, min and max results are employed to prune KE segments.
 * So this design will only be available for KE, and the related issue is KE-29673.
 * Same with the `BloomFilterMightContain` expression, this expression requires that
 * `structTypeExpression` is either a constant value or an uncorrelated scalar sub-query.
 *
 * @param structTypeExpression the struct type including aggregate operations.
 * @param valueExpression the application side target column expression.
 * @param applicationSideAttrRef the attribute reference for `valueExpression`, this parameter will
 *        be used to construct `rangeRow` iff `valueExpression` is transformed
 *        to non AttributeReference type.
 */
case class BloomAndRangeFilterExpression(
    structTypeExpression: Expression,
    valueExpression: Expression,
    applicationSideAttrRef: AttributeReference)
  extends BinaryExpression with BloomRuntimeFilterHelper {

  val MIN_INDEX = 0
  val MAX_INDEX = 1
  val BINARY_INDEX = 2

  override def nullable: Boolean = true
  override def left: Expression = structTypeExpression
  override def right: Expression = valueExpression
  override def prettyName: String = "bloom_and_range_filter"
  override def dataType: DataType = BooleanType
  def decoratedRight: Expression = new XxHash64(Seq(right))

  override def checkInputDataTypes(): TypeCheckResult = {
    left.dataType match {
      case StructType(_) =>
        structTypeExpression match {
          case e : Expression if e.foldable => TypeCheckResult.TypeCheckSuccess
          case subquery : PlanExpression[_] if !subquery.containsPattern(OUTER_REFERENCE) =>
            TypeCheckResult.TypeCheckSuccess
          case _ =>
            TypeCheckResult.TypeCheckFailure(
              s"The bloom and range filter binary input to $prettyName " +
              "should be either a constant value or a scalar sub-query expression")
        }
      case _ => TypeCheckResult.TypeCheckFailure(
        s"Input to function $prettyName should be a StructType, " +
          s"which includes aggregate operations for min, max and bloom filter, " +
          s"but it's a [${left.dataType.catalogString}]")
    }
  }

  override protected def withNewChildrenInternal(
    newStructTypeExpression: Expression,
    newValueExpression: Expression): BloomAndRangeFilterExpression =
    copy(structTypeExpression = newStructTypeExpression, valueExpression = newValueExpression)

  @transient private lazy val subQueryRowResult = {
    structTypeExpression.eval().asInstanceOf[UnsafeRow]
  }

  @transient lazy val rangeRow: Seq[Expression] = {
    val structFields = left.dataType.asInstanceOf[StructType].fields
    val minDataType = structFields(MIN_INDEX).dataType
    val min = subQueryRowResult.get(MIN_INDEX, minDataType)
    val maxDataType = structFields(MAX_INDEX).dataType
    val max = subQueryRowResult.get(MAX_INDEX, maxDataType)
    if(min != null && max != null) {
      val attrRef = valueExpression match {
        case reference: AttributeReference =>
          reference
        case _ =>
          applicationSideAttrRef
      }
      val gteExpress = GreaterThanOrEqual(attrRef, Literal(convertToScala(min, minDataType)))
      val lteExpress = LessThanOrEqual(attrRef, Literal(convertToScala(max, maxDataType)))
      Seq(gteExpress, lteExpress)
    } else {
      Seq()
    }
  }

  @transient private lazy val bloomFilter = {
    val bytes = subQueryRowResult.getBinary(BINARY_INDEX)
    if(bytes == null) null else deserialize(bytes)
  }

  override def eval(input: InternalRow): Any = {
    internalEval(input, bloomFilter, decoratedRight)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    internalDoGenCode(ctx, ev, bloomFilter, decoratedRight, dataType)
  }

}
