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

import org.apache.spark.SparkException
import org.apache.spark.sql.{Encoder, Encoders}

/**
 * An aggregator that uses a single associative and commutative reduce function. This reduce
 * function can be used to go through all input values and reduces them to a single value. If
 * there is no input, a null value is returned.
 *
 * This class currently assumes there is at least one input row.
 */
@SerialVersionUID(5066084382969966160L)
private[sql] class ReduceAggregator[T: Encoder](func: (T, T) => T)
    extends Aggregator[T, (Boolean, T), T] {

  @transient private lazy val encoder = implicitly[Encoder[T]]

  private val _zero = encoder.clsTag.runtimeClass match {
    case java.lang.Boolean.TYPE => false
    case java.lang.Byte.TYPE => 0.toByte
    case java.lang.Short.TYPE => 0.toShort
    case java.lang.Integer.TYPE => 0
    case java.lang.Long.TYPE => 0L
    case java.lang.Float.TYPE => 0f
    case java.lang.Double.TYPE => 0d
    case _ => null
  }

  override def zero: (Boolean, T) = (false, _zero.asInstanceOf[T])

  override def bufferEncoder: Encoder[(Boolean, T)] =
    Encoders.tuple(Encoders.scalaBoolean, encoder)

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

  override def finish(reduction: (Boolean, T)): T = {
    if (!reduction._1) {
      throw SparkException.internalError("ReduceAggregator requires at least one input row")
    }
    reduction._2
  }
}

private[sql] object ReduceAggregator {
  def apply[T: Encoder](f: AnyRef): ReduceAggregator[T] = {
    new ReduceAggregator(f.asInstanceOf[(T, T) => T])
  }
}
