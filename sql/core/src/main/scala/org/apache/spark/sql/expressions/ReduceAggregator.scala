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
 * A generic class for reduce aggregations, which accepts a reduce function that can be used to take
 * all of the elements of a group and reduce them to a single value.
 *
 * @tparam T The input and output type for the reduce function.
 * @param func The reduce aggregation function.
 * @param encoder The encoder for the input and output type of the reduce function.
 * @since 2.1.0
 */
@Experimental
private[sql] class ReduceAggregator[T](func: (T, T) => T, encoder: ExpressionEncoder[T])
    extends Aggregator[T, (Boolean, T), T] {

  /**
   * A zero value for this aggregation. It is represented as a Tuple2. The first element of the
   * tuple is a false boolean value indicating the buffer is not initialized. The second element
   * is initialized as a null value.
   * @since 2.1.0
   */
  override def zero: (Boolean, T) = (false, null.asInstanceOf[T])

  override def bufferEncoder: Encoder[(Boolean, T)] =
    ExpressionEncoder.tuple(ExpressionEncoder[Boolean](), encoder)

  override def outputEncoder: Encoder[T] = encoder

  /**
   * Combine two values to produce a new value. If the buffer `b` is not initialized, it simply
   * takes the value of `a` and set the initialization flag to `true`.
   * @since 2.1.0
   */
  override def reduce(b: (Boolean, T), a: T): (Boolean, T) = {
    if (b._1) {
      (true, func(b._2, a))
    } else {
      (true, a)
    }
  }

  /**
   * Merge two intermediate values. As it is possibly that the buffer is just the `zero` value
   * coming from empty partition, it checks if the buffers are initialized, and only performs
   * merging when they are initialized both.
   * @since 2.1.0
   */
  override def merge(b1: (Boolean, T), b2: (Boolean, T)): (Boolean, T) = {
    if (!b1._1) {
      b2
    } else if (!b2._1) {
      b1
    } else {
      (true, func(b1._2, b2._2))
    }
  }

  /**
   * Transform the output of the reduction. Simply output the value in the buffer.
   * @since 2.1.0
   */
  override def finish(reduction: (Boolean, T)): T = {
    reduction._2
  }
}
