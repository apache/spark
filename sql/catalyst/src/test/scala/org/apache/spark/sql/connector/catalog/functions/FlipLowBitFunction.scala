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
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * A partition transform that swaps each even value with the odd one above it, so it *permutes* its
 * key space, and whose result type is its argument's type.
 *
 * Those two properties together are what let a table partitioned by this report the same partition
 * key list as a table partitioned by the raw column while a key stands for different rows on each
 * side. A many-to-one transform cannot: the two key lists coincide only where it is the identity on
 * them. That is the shape `KeyedShuffleSpec.isCompatibleWith` has to refuse before a join has
 * reduced one side onto the other.
 *
 * A [[SimpleFunction]], and it lives in catalyst's test sources rather than beside the other
 * transform fixtures in `sql/core`, so that `InMemoryBaseTable` can compute its partition keys by
 * calling `produceResult` here instead of repeating the arithmetic.
 */
object FlipLowBitFunction extends SimpleFunction with ScalarFunction[Long] {
  override def inputTypes(): Array[DataType] = Array(LongType)
  override def resultType(): DataType = LongType
  override def name(): String = "flip_low_bit"
  override def canonicalName(): String = name()
  override def description(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Long = input.getLong(0) ^ 1L
}
