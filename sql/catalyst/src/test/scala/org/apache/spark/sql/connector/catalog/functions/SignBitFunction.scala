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
 * A partition transform that keys a `DOUBLE` on its sign bit: 1 where the bit is set, as for
 * `-1.0` and `-0.0`, and 0 otherwise. So it maps `0.0` and `-0.0`, which are equal, to different
 * keys, as a transform that reads the bits of a floating point value can.
 *
 * A [[SimpleFunction]], and it lives in catalyst's test sources, like [[FlipLowBitFunction]], so
 * that `InMemoryBaseTable` can compute its partition keys by calling `produceResult` here.
 */
object SignBitFunction extends SimpleFunction with ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(DoubleType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "sign_bit"
  override def canonicalName(): String = name()
  override def description(): String = name()
  override def toString: String = name()
  override def produceResult(input: InternalRow): Int =
    if (java.lang.Double.doubleToRawLongBits(input.getDouble(0)) < 0) 1 else 0
}
