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
 * `xor(col, mask)`, a partition transform with a literal parameter that permutes its key space, as
 * [[FlipLowBitFunction]] does without one: `xor(id, 1)` swaps each even value with the odd one
 * above it, and `xor(id, 0)` is the identity. So tables partitioned by `identity(id)`, `xor(id, 0)`
 * or `xor(id, 1)` can report the same partition key list while a key stands for different rows.
 *
 * In catalyst's test sources, like [[FlipLowBitFunction]], so that `InMemoryBaseTable` computes its
 * partition keys by calling `produceResult` here.
 */
object XorFunction extends SimpleFunction with ScalarFunction[Long] {
  override def inputTypes(): Array[DataType] = Array(LongType, LongType)
  override def resultType(): DataType = LongType
  override def name(): String = "xor"
  override def canonicalName(): String = name()
  override def description(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Long = input.getLong(0) ^ input.getLong(1)
}
