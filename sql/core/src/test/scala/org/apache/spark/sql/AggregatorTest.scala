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

package org.apache.spark.sql

import org.apache.spark.sql.expressions.Aggregator

/** An `Aggregator` that adds up any numeric type returned by the given function. */
class SumOf[I, N : Numeric](f: I => N) extends Aggregator[I, N, N] {
  val numeric = implicitly[Numeric[N]]

  override def zero: N = numeric.zero

  override def reduce(b: N, a: I): N = numeric.plus(b, f(a))

  override def merge(b1: N, b2: N): N = numeric.plus(b1, b2)

  override def finish(reduction: N): N = reduction
}

object TypedAverage extends Aggregator[(String, Int), (Long, Long), Double] {
  override def zero: (Long, Long) = (0, 0)

  override def reduce(countAndSum: (Long, Long), input: (String, Int)): (Long, Long) = {
    (countAndSum._1 + 1, countAndSum._2 + input._2)
  }

  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }

  override def finish(countAndSum: (Long, Long)): Double = countAndSum._2 / countAndSum._1
}

object ComplexResultAgg extends Aggregator[(String, Int), (Long, Long), (Long, Long)] {

  override def zero: (Long, Long) = (0, 0)

  override def reduce(countAndSum: (Long, Long), input: (String, Int)): (Long, Long) = {
    (countAndSum._1 + 1, countAndSum._2 + input._2)
  }

  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }

  override def finish(reduction: (Long, Long)): (Long, Long) = reduction
}

case class AggData(a: Int, b: String)
object ClassInputAgg extends Aggregator[AggData, Int, Int] {
  /** A zero value for this aggregation. Should satisfy the property that any b + zero = b */
  override def zero: Int = 0

  /**
   * Combine two values to produce a new value.  For performance, the function may modify `b` and
   * return it instead of constructing new object for b.
   */
  override def reduce(b: Int, a: AggData): Int = b + a.a

  /**
   * Transform the output of the reduction.
   */
  override def finish(reduction: Int): Int = reduction

  /**
   * Merge two intermediate values
   */
  override def merge(b1: Int, b2: Int): Int = b1 + b2
}

object ComplexBufferAgg extends Aggregator[AggData, (Int, AggData), Int] {
  /** A zero value for this aggregation. Should satisfy the property that any b + zero = b */
  override def zero: (Int, AggData) = 0 -> AggData(0, "0")

  /**
   * Combine two values to produce a new value.  For performance, the function may modify `b` and
   * return it instead of constructing new object for b.
   */
  override def reduce(b: (Int, AggData), a: AggData): (Int, AggData) = (b._1 + 1, a)

  /**
   * Transform the output of the reduction.
   */
  override def finish(reduction: (Int, AggData)): Int = reduction._1

  /**
   * Merge two intermediate values
   */
  override def merge(b1: (Int, AggData), b2: (Int, AggData)): (Int, AggData) =
    (b1._1 + b2._1, b1._2)
}
