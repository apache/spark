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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


// Compute the population standard deviation of a column
case class StddevPop(child: Expression) extends StddevAgg(child) {
  override def isSample: Boolean = false
  override def prettyName: String = "stddev_pop"
}


// Compute the sample standard deviation of a column
case class StddevSamp(child: Expression) extends StddevAgg(child) {
  override def isSample: Boolean = true
  override def prettyName: String = "stddev_samp"
}


// Compute standard deviation based on online algorithm specified here:
// http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
abstract class StddevAgg(child: Expression) extends DeclarativeAggregate {

  def isSample: Boolean

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select stddev(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(NumericType, NullType))

  private val resultType = DoubleType

  private val count = AttributeReference("count", resultType)()
  private val avg = AttributeReference("avg", resultType)()
  private val mk = AttributeReference("mk", resultType)()

  override val aggBufferAttributes = count :: avg :: mk :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* count = */ Cast(Literal(0), resultType),
    /* avg = */ Cast(Literal(0), resultType),
    /* mk = */ Cast(Literal(0), resultType)
  )

  override val updateExpressions: Seq[Expression] = {
    val value = Cast(child, resultType)
    val newCount = count + Cast(Literal(1), resultType)

    // update average
    // avg = avg + (value - avg)/count
    val newAvg = avg + (value - avg) / newCount

    // update sum ofference from mean
    // Mk = Mk + (value - preAvg) * (value - updatedAvg)
    val newMk = mk + (value - avg) * (value - newAvg)

    Seq(
      /* count = */ If(IsNull(child), count, newCount),
      /* avg = */ If(IsNull(child), avg, newAvg),
      /* mk = */ If(IsNull(child), mk, newMk)
    )
  }

  override val mergeExpressions: Seq[Expression] = {

    // count merge
    val newCount = count.left + count.right

    // average merge
    val newAvg = ((avg.left * count.left) + (avg.right * count.right)) / newCount

    // update sum of square differences
    val newMk = {
      val avgDelta = avg.right - avg.left
      val mkDelta = (avgDelta * avgDelta) * (count.left * count.right) / newCount
      mk.left + mk.right + mkDelta
    }

    Seq(
      /* count = */ If(IsNull(count.left), count.right,
                       If(IsNull(count.right), count.left, newCount)),
      /* avg = */ If(IsNull(avg.left), avg.right,
                     If(IsNull(avg.right), avg.left, newAvg)),
      /* mk = */ If(IsNull(mk.left), mk.right,
                    If(IsNull(mk.right), mk.left, newMk))
    )
  }

  override val evaluateExpression: Expression = {
    // when count == 0, return null
    // when count == 1, return 0
    // when count >1
    // stddev_samp = sqrt (mk/(count -1))
    // stddev_pop = sqrt (mk/count)
    val varCol =
      if (isSample) {
        mk / Cast(count - Cast(Literal(1), resultType), resultType)
      } else {
        mk / count
      }

    If(EqualTo(count, Cast(Literal(0), resultType)), Cast(Literal(null), resultType),
      If(EqualTo(count, Cast(Literal(1), resultType)), Cast(Literal(0), resultType),
        Cast(Sqrt(varCol), resultType)))
  }
}
