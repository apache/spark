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

case class Kurtosis(child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends CentralMomentAgg(child) {

  def this(child: Expression) = this(child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "kurtosis"

  override protected val momentOrder = 4

  // NOTE: this is the formula for excess kurtosis, which is default for R and SciPy
  override def getStatistic(n: Double, mean: Double, moments: Array[Double]): Any = {
    require(moments.length == momentOrder + 1,
      s"$prettyName requires ${momentOrder + 1} central moments, received: ${moments.length}")
    val m2 = moments(2)
    val m4 = moments(4)

    if (n == 0.0) {
      null
    } else if (m2 == 0.0) {
      Double.NaN
    } else {
      n * m4 / (m2 * m2) - 3.0
    }
  }
}
