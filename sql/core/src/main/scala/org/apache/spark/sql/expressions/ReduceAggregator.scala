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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * :: Experimental ::
 * An aggregator that uses a single associative and commutative reduce function. This reduce
 * function can be used to go through all input values and reduces them to a single value.
 * If there is no input, a null value is returned.
 *
 * @since 2.1.0
 */
@Experimental
abstract class ReduceAggregator[T] extends Aggregator[T, (Boolean, T), T] {

  // Question 1: Should func and encoder be parameters rather than abstract methods?
  //  rxin: abstract method has better java compatibility and forces naming the concrete impl,
  //  whereas parameter has better type inference (infer encoders via context bounds).
  // Question 2: Should finish throw an exception or return null if there is no input?
  //  rxin: null might be more "SQL" like, whereas exception is more Scala like.

  /**
   * A associative and commutative reduce function.
   * @since 2.1.0
   */
  def func(a: T, b: T): T

  /**
   * Encoder for type T.
   * @since 2.1.0
   */
  def encoder: ExpressionEncoder[T]

  override def zero: (Boolean, T) = (false, null.asInstanceOf[T])

  override def bufferEncoder: Encoder[(Boolean, T)] =
    ExpressionEncoder.tuple(ExpressionEncoder[Boolean](), encoder)

  override def outputEncoder: Encoder[T] = encoder

  override def reduce(b: (Boolean, T), a: T): (Boolean, T) = {
    if (b._1) {
      (true, func(b._2, a))
    } else {
      (true, a)
    }
  }

  override def merge(b1: (Boolean, T), b2: (Boolean, T)): (Boolean, T) = {
    if (!b1._1) {
      b2
    } else if (!b2._1) {
      b1
    } else {
      (true, func(b1._2, b2._2))
    }
  }

  override def finish(reduction: (Boolean, T)): T = reduction._2
}
