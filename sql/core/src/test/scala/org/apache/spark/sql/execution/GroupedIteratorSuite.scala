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

package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, IntegerType, StructType}

class GroupedIteratorSuite extends SparkFunSuite {

  test("basic") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema)
    val input = Seq(Row(1, "a"), Row(1, "b"), Row(2, "c"))
    val grouped = GroupedIterator(input.iterator.map(encoder.toRow),
      Seq('i.int.at(0)), schema.toAttributes)

    val result = grouped.map {
      case (key, data) =>
        assert(key.numFields == 1)
        key.getInt(0) -> data.map(encoder.fromRow).toSeq
    }.toSeq

    assert(result ==
      1 -> Seq(input(0), input(1)) ::
      2 -> Seq(input(2)) :: Nil)
  }

  test("group by 2 columns") {
    val schema = new StructType().add("i", IntegerType).add("l", LongType).add("s", StringType)
    val encoder = RowEncoder(schema)

    val input = Seq(
      Row(1, 2L, "a"),
      Row(1, 2L, "b"),
      Row(1, 3L, "c"),
      Row(2, 1L, "d"),
      Row(3, 2L, "e"))

    val grouped = GroupedIterator(input.iterator.map(encoder.toRow),
      Seq('i.int.at(0), 'l.long.at(1)), schema.toAttributes)

    val result = grouped.map {
      case (key, data) =>
        assert(key.numFields == 2)
        (key.getInt(0), key.getLong(1), data.map(encoder.fromRow).toSeq)
    }.toSeq

    assert(result ==
      (1, 2L, Seq(input(0), input(1))) ::
      (1, 3L, Seq(input(2))) ::
      (2, 1L, Seq(input(3))) ::
      (3, 2L, Seq(input(4))) :: Nil)
  }

  test("do nothing to the value iterator") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema)
    val input = Seq(Row(1, "a"), Row(1, "b"), Row(2, "c"))
    val grouped = GroupedIterator(input.iterator.map(encoder.toRow),
      Seq('i.int.at(0)), schema.toAttributes)

    assert(grouped.length == 2)
  }
}
