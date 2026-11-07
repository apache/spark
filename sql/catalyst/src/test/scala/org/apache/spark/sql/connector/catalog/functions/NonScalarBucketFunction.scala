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

import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * A `bucket` that is neither a [[ScalarFunction]] nor an [[AggregateFunction]], so Spark cannot
 * evaluate it. A storage-partitioned join has to shuffle rather than evaluate it. The in-memory
 * table computes the bucket values itself, see `BucketTransform` in `InMemoryBaseTable`.
 */
object NonScalarBucketFunction extends BoundFunction {
  override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = name()
  override def toString: String = name()
}
