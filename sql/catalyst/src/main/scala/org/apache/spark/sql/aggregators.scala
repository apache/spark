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

package object aggregators {

  def countItems[A]: UserAggregator[A, Long, Long] = new CountItems[A]

  def sumOf[N: Numeric]: UserAggregator[N, N, N] = new SumOf[N, N](identity[N])

  def sumOf[I, N: Numeric](f: I => N) = new SumOf[I, N](f)


  trait UserAggregator[-A, B, +C] {
    /**
     * Transform the input before the reduction.
     */
    def prepare(input: A): B

    /**
     * Combine two values to produce a new value.
     */
    def reduce(l: B, r: B): B

    /**
     * Transform the output of the reduction.
     */
    def present(reduction: B): C
  }

  class CountItems[In] extends UserAggregator[In, Long, Long] with Serializable {
    override def prepare(input: In): Long = if (input == null) 0 else 1

    override def reduce(l: Long, r: Long): Long = l + r

    override def present(reduction: Long): Long = reduction
  }

  class SumOf[I, N: Numeric](f: I => N) extends UserAggregator[I, N, N] with Serializable {
    val numeric = implicitly[Numeric[N]]

    override def prepare(input: I): N = if (input == null) numeric.zero else f(input)

    override def reduce(l: N, r: N): N = numeric.plus(l, r)

    override def present(reduction: N): N = reduction
  }
}