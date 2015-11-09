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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.expressions.Aggregator

/** An `Aggregator` that adds up any numeric type returned by the given function. */
class SumOf[I, N : Numeric](f: I => N) extends Aggregator[I, N, N] with Serializable {
  val numeric = implicitly[Numeric[N]]

  override def prepare(input: I): N = if (input == null) numeric.zero else f(input)

  override def reduce(l: N, r: N): N = numeric.plus(l, r)

  override def present(reduction: N): N = reduction
}
