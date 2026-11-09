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

package org.apache.spark.sql.connector.catalog.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * A partition transform on a long key that reduces itself: `reducer` returns a merging reducer
 * (x -> x % 2) when the other side is this same function. The both-sides reduce contract
 * r1(f(x)) = r2(f(x)) is vacuous when the two sides share the function, so any merging reducer is
 * admitted. No in-tree reducible function reduces itself. This one lets a test check that a
 * same-function pair is not reduced under partial clustering (SPARK-59234).
 *
 * Like [[FlipLowBitFunction]], it lives in catalyst's test sources so `InMemoryBaseTable` can
 * compute its partition keys by calling `produceResult` here instead of repeating the arithmetic.
 */
object SelfReduceFunction extends SimpleFunction
    with ScalarFunction[Int] with ReducibleFunction[Int, Int] {
  override def inputTypes(): Array[DataType] = Array(LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "self_reduce"
  override def canonicalName(): String = name()
  override def description(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int = input.getLong(0).toInt

  override def reducer(otherFunc: ReducibleFunction[_, _]): Reducer[Int, Int] =
    if (otherFunc == SelfReduceFunction) SelfReducer else null
}

object SelfReducer extends Reducer[Int, Int] {
  override def reduce(value: Int): Int = value % 2
  override def resultType(): DataType = IntegerType
  override def displayName(): String = "self_reduce_reducer"
}
