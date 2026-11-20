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
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}

/**
 * `sign_flip(col, zero)`: `col` where `zero`'s sign is positive and `-col` where it is negative,
 * read with `Math.copySign`. So `sign_flip(id, 0.0)` and `sign_flip(id, -0.0)` report the same
 * keys for ids `{-1, 1}` while each key holds the other one's id, though `Literal` finds `0.0` and
 * `-0.0` equal.
 *
 * In catalyst's test sources, like [[FlipLowBitFunction]], so that `InMemoryBaseTable` computes its
 * partition keys by calling `produceResult` here.
 */
object SignFlipFunction extends SimpleFunction with ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, DoubleType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "sign_flip"
  override def canonicalName(): String = name()
  override def description(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int =
    if (Math.copySign(1.0, input.getDouble(1)) > 0) input.getInt(0) else -input.getInt(0)
}
