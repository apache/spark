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

/**
 * Compute the covariance between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 *
 */
abstract class Covariance(x: Expression, y: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = Seq(x, y)
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  protected val count = AttributeReference("count", DoubleType, nullable = false)()
  protected val xAvg = AttributeReference("xAvg", DoubleType, nullable = false)()
  protected val yAvg = AttributeReference("yAvg", DoubleType, nullable = false)()
  protected val ck = AttributeReference("ck", DoubleType, nullable = false)()

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(count, xAvg, yAvg, ck)

  override val initialValues: Seq[Expression] = Seq(
    /* count = */ Literal(0.0),
    /* xAvg = */ Literal(0.0),
    /* yAvg = */ Literal(0.0),
    /* ck = */ Literal(0.0)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    val n = count + Literal(1.0)
    val dx = x - xAvg
    val dy = y - yAvg
    val dyN = dy / n
    val newXAvg = xAvg + dx / n
    val newYAvg = yAvg + dyN
    val newCk = ck + dx * (dy - dyN)

    val isNull = Or(IsNull(x), IsNull(y))
    if (x.nullable || y.nullable) {
      Seq(
        /* count = */ If(isNull, count, n),
        /* xAvg = */ If(isNull, xAvg, newXAvg),
        /* yAvg = */ If(isNull, yAvg, newYAvg),
        /* ck = */ If(isNull, ck, newCk)
      )
    } else {
      Seq(
        /* count = */ n,
        /* xAvg = */ newXAvg,
        /* yAvg = */ newYAvg,
        /* ck = */ newCk
      )
    }
  }

  override val mergeExpressions: Seq[Expression] = {

    val n1 = count.left
    val n2 = count.right
    val n = n1 + n2
    val dx = xAvg.right - xAvg.left
    val dxN = If(EqualTo(n, Literal(0.0)), Literal(0.0), dx / n)
    val dy = yAvg.right - yAvg.left
    val dyN = If(EqualTo(n, Literal(0.0)), Literal(0.0), dy / n)
    val newXAvg = xAvg.left + dxN * n2
    val newYAvg = yAvg.left + dyN * n2
    val newCk = ck.left + ck.right + dx * dyN * n1 * n2

    Seq(
      /* count = */ n,
      /* xAvg = */ newXAvg,
      /* yAvg = */ newYAvg,
      /* ck = */ newCk
    )
  }
}

case class CovPopulation(left: Expression, right: Expression) extends Covariance(left, right) {
  override val evaluateExpression: Expression = {
    If(EqualTo(count, Literal(0.0)), Literal.create(null, DoubleType),
      ck / count)
  }
  override def prettyName: String = "covar_pop"
}


case class CovSample(left: Expression, right: Expression) extends Covariance(left, right) {
  override val evaluateExpression: Expression = {
    If(EqualTo(count, Literal(0.0)), Literal.create(null, DoubleType),
      If(EqualTo(count, Literal(1.0)), Literal(Double.NaN),
        ck / (count - Literal(1.0))))
  }
  override def prettyName: String = "covar_samp"
}
