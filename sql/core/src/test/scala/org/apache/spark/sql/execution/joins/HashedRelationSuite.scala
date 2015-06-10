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
import org.apache.spark.sql.catalyst.expressions.{Projection, Row}
import org.apache.spark.util.collection.CompactBuffer


class HashedRelationSuite extends SparkFunSuite {

  // Key is simply the record itself
  private val keyProjection = new Projection {
    override def apply(row: Row): Row = row
  }

  test("GeneralHashedRelation") {
    val data = Array(Row(0), Row(1), Row(2), Row(2))
    val hashed = HashedRelation(data.iterator, keyProjection)
    assert(hashed.isInstanceOf[GeneralHashedRelation])

    assert(hashed.get(data(0)) == CompactBuffer[Row](data(0)))
    assert(hashed.get(data(1)) == CompactBuffer[Row](data(1)))
    assert(hashed.get(Row(10)) === null)

    val data2 = CompactBuffer[Row](data(2))
    data2 += data(2)
    assert(hashed.get(data(2)) == data2)
  }

  test("UniqueKeyHashedRelation") {
    val data = Array(Row(0), Row(1), Row(2))
    val hashed = HashedRelation(data.iterator, keyProjection)
    assert(hashed.isInstanceOf[UniqueKeyHashedRelation])

    assert(hashed.get(data(0)) == CompactBuffer[Row](data(0)))
    assert(hashed.get(data(1)) == CompactBuffer[Row](data(1)))
    assert(hashed.get(data(2)) == CompactBuffer[Row](data(2)))
    assert(hashed.get(Row(10)) === null)

    val uniqHashed = hashed.asInstanceOf[UniqueKeyHashedRelation]
    assert(uniqHashed.getValue(data(0)) == data(0))
    assert(uniqHashed.getValue(data(1)) == data(1))
    assert(uniqHashed.getValue(data(2)) == data(2))
    assert(uniqHashed.getValue(Row(10)) == null)
  }
}
