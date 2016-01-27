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

// Compute standard deviation based on online algorithm specified here:
// http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
abstract class StddevAgg(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  protected val resultType = DoubleType

  protected val count = AttributeReference("count", resultType, nullable = false)()
  protected val avg = AttributeReference("avg", resultType, nullable = false)()
  protected val mk = AttributeReference("mk", resultType, nullable = false)()

  override val aggBufferAttributes = count :: avg :: mk :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* count = */ Literal(0.0),
    /* avg = */ Literal(0.0),
    /* mk = */ Literal(0.0)
  )

  override val updateExpressions: Seq[Expression] = {
    val newCount = count + Literal(1.0)

    // update average
    // avg = avg + (value - avg)/count
    val newAvg = avg + (child - avg) / newCount

    // update sum ofference from mean
    // Mk = Mk + (value - preAvg) * (value - updatedAvg)
    val newMk = mk + (child - avg) * (child - newAvg)

    if (child.nullable) {
      Seq(
        /* count = */ If(IsNull(child), count, newCount),
        /* avg = */ If(IsNull(child), avg, newAvg),
        /* mk = */ If(IsNull(child), mk, newMk)
      )
    } else {
      Seq(
        /* count = */ newCount,
        /* avg = */ newAvg,
        /* mk = */ newMk
      )
    }
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
      /* count = */ newCount,
      /* avg = */ newAvg,
      /* mk = */ newMk
    )
  }
}

// Compute the population standard deviation of a column
case class StddevPop(child: Expression) extends StddevAgg(child) {

  override val evaluateExpression: Expression = {
    // when count == 0, return null
    // when count >0, sqrt (mk/count)
    If(EqualTo(count, Literal(0.0)), Literal.create(null, resultType),
      Sqrt(mk / count))
  }

  override def prettyName: String = "stddev_pop"
}

// Compute the sample standard deviation of a column
case class StddevSamp(child: Expression) extends StddevAgg(child) {
  override val evaluateExpression: Expression = {
    // when count == 0, return null
    // when count == 1, return Na
    // when count >1, sqrt(mk/(count -1))
    If(EqualTo(count, Literal(0.0)), Literal.create(null, resultType),
      If(EqualTo(count, Literal(1.0)), Literal(Double.NaN),
        Sqrt(mk / (count - Literal(1.0)))))
  }

  override def prettyName: String = "stddev_samp"
}

// Compute the population variance of a column
case class VariancePop(child: Expression) extends StddevAgg(child) {

  override val evaluateExpression: Expression = {
    // when count == 0, return null
    // when count >1, sqrt (mk/count)

    If(EqualTo(count, Literal(0.0)), Literal.create(null, resultType),
      mk / count)
  }

  override def prettyName: String = "var_pop"
}

// Compute the sample variance of a column
case class VarianceSamp(child: Expression) extends StddevAgg(child) {
  override val evaluateExpression: Expression = {
    // when count == 0, return null
    // when count == 1, return Na
    // when count >1N, sqrt (mk/(count -1))
    If(EqualTo(count, Literal(0.0)), Literal.create(null, resultType),
      If(EqualTo(count, Literal(1.0)), Literal(Double.NaN),
        mk / (count - Literal(1.0))))
  }

  override def prettyName: String = "var_samp"
}
