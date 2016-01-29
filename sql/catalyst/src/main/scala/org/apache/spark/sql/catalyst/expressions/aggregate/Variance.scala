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

import org.apache.spark.sql.catalyst.expressions._

case class VarianceSamp(child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends CentralMomentAgg(child) {

  def this(child: Expression) = this(child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "var_samp"

  override protected val momentOrder = 2

  override def getStatistic(n: Double, mean: Double, moments: Array[Double]): Any = {
    require(moments.length == momentOrder + 1,
      s"$prettyName requires ${momentOrder + 1} central moment, received: ${moments.length}")

    if (n == 0.0) {
      null
    } else if (n == 1.0) {
      Double.NaN
    } else {
      moments(2) / (n - 1.0)
    }
  }
}

case class VariancePop(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends CentralMomentAgg(child) {

  def this(child: Expression) = this(child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "var_pop"

  override protected val momentOrder = 2

  override def getStatistic(n: Double, mean: Double, moments: Array[Double]): Any = {
    require(moments.length == momentOrder + 1,
      s"$prettyName requires ${momentOrder + 1} central moment, received: ${moments.length}")

    if (n == 0.0) {
      null
    } else {
      moments(2) / n
    }
  }
}
