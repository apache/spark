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

package org.apache.spark.ml.recommendation

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.util.BoundedPriorityQueue


/**
 * Works on rows of the form (K1, K2, V) where K1 & K2 are IDs and V is the score value. Finds
 * the top `num` K2 items based on the given Ordering.
 */
private[recommendation] class TopByKeyAggregator[K1: TypeTag, K2: TypeTag, V: TypeTag]
  (num: Int, ord: Ordering[(K2, V)])
  extends Aggregator[(K1, K2, V), BoundedPriorityQueue[(K2, V)], Array[(K2, V)]] {

  override def zero: BoundedPriorityQueue[(K2, V)] = new BoundedPriorityQueue[(K2, V)](num)(ord)

  override def reduce(
      q: BoundedPriorityQueue[(K2, V)],
      a: (K1, K2, V)): BoundedPriorityQueue[(K2, V)] = {
    q += {(a._2, a._3)}
  }

  override def merge(
      q1: BoundedPriorityQueue[(K2, V)],
      q2: BoundedPriorityQueue[(K2, V)]): BoundedPriorityQueue[(K2, V)] = {
    q1 ++= q2
  }

  override def finish(r: BoundedPriorityQueue[(K2, V)]): Array[(K2, V)] = {
    r.toArray.sorted(ord.reverse)
  }

  override def bufferEncoder: Encoder[BoundedPriorityQueue[(K2, V)]] = {
    Encoders.kryo[BoundedPriorityQueue[(K2, V)]]
  }

  override def outputEncoder: Encoder[Array[(K2, V)]] = ExpressionEncoder[Array[(K2, V)]]()
}
