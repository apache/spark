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

package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.apache.spark.util.collection.CompactBuffer


class HashedRelationSuite extends SparkFunSuite {

  // Key is simply the record itself
  private val keyProjection = new Projection {
    override def apply(row: InternalRow): InternalRow = row
  }

  test("GeneralHashedRelation") {
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2), InternalRow(2))
    val hashed = HashedRelation(data.iterator, keyProjection)
    assert(hashed.isInstanceOf[GeneralHashedRelation])

    assert(hashed.get(data(0)) === CompactBuffer[InternalRow](data(0)))
    assert(hashed.get(data(1)) === CompactBuffer[InternalRow](data(1)))
    assert(hashed.get(InternalRow(10)) === null)

    val data2 = CompactBuffer[InternalRow](data(2))
    data2 += data(2)
    assert(hashed.get(data(2)) === data2)
  }

  test("UniqueKeyHashedRelation") {
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2))
    val hashed = HashedRelation(data.iterator, keyProjection)
    assert(hashed.isInstanceOf[UniqueKeyHashedRelation])

    assert(hashed.get(data(0)) === CompactBuffer[InternalRow](data(0)))
    assert(hashed.get(data(1)) === CompactBuffer[InternalRow](data(1)))
    assert(hashed.get(data(2)) === CompactBuffer[InternalRow](data(2)))
    assert(hashed.get(InternalRow(10)) === null)

    val uniqHashed = hashed.asInstanceOf[UniqueKeyHashedRelation]
    assert(uniqHashed.getValue(data(0)) === data(0))
    assert(uniqHashed.getValue(data(1)) === data(1))
    assert(uniqHashed.getValue(data(2)) === data(2))
    assert(uniqHashed.getValue(InternalRow(10)) === null)
  }

  test("UnsafeHashedRelation") {
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2), InternalRow(2))
    val buildKey = Seq(BoundReference(0, IntegerType, false))
    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val hashed = UnsafeHashedRelation(data.iterator, buildKey, schema, 1)
    assert(hashed.isInstanceOf[UnsafeHashedRelation])

    val toUnsafeKey = UnsafeProjection.create(schema)
    val unsafeData = data.map(toUnsafeKey(_).copy()).toArray
    assert(hashed.get(unsafeData(0)) === CompactBuffer[InternalRow](unsafeData(0)))
    assert(hashed.get(unsafeData(1)) === CompactBuffer[InternalRow](unsafeData(1)))
    assert(hashed.get(toUnsafeKey(InternalRow(10))) === null)

    val data2 = CompactBuffer[InternalRow](unsafeData(2).copy())
    data2 += unsafeData(2).copy()
    assert(hashed.get(unsafeData(2)) === data2)

    val hashed2 = SparkSqlSerializer.deserialize(SparkSqlSerializer.serialize(hashed))
      .asInstanceOf[UnsafeHashedRelation]
    assert(hashed2.get(unsafeData(0)) === CompactBuffer[InternalRow](unsafeData(0)))
    assert(hashed2.get(unsafeData(1)) === CompactBuffer[InternalRow](unsafeData(1)))
    assert(hashed2.get(toUnsafeKey(InternalRow(10))) === null)
    assert(hashed2.get(unsafeData(2)) === data2)
  }
}
