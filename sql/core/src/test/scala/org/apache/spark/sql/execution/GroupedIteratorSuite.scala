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
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class GroupedIteratorSuite extends SparkFunSuite {

  test("group by 1 column") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()
    val toRow = encoder.createSerializer()
    val fromRow = encoder.createDeserializer()
    val input = Seq(Row(1, "a"), Row(1, "b"), Row(2, "c"))
    val grouped = GroupedIterator(input.iterator.map(toRow),
      Seq($"i".int.at(0)), schema.toAttributes)

    val result = grouped.map {
      case (key, data) =>
        assert(key.numFields == 1)
        key.getInt(0) -> data.map(fromRow).toSeq
    }.toSeq

    assert(result ==
      1 -> Seq(input(0), input(1)) ::
      2 -> Seq(input(2)) :: Nil)
  }

  test("group by 2 columns") {
    val schema = new StructType().add("i", IntegerType).add("l", LongType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()
    val toRow = encoder.createSerializer()
    val fromRow = encoder.createDeserializer()

    val input = Seq(
      Row(1, 2L, "a"),
      Row(1, 2L, "b"),
      Row(1, 3L, "c"),
      Row(2, 1L, "d"),
      Row(3, 2L, "e"))

    val grouped = GroupedIterator(input.iterator.map(toRow),
      Seq($"i".int.at(0), $"l".long.at(1)), schema.toAttributes)

    val result = grouped.map {
      case (key, data) =>
        assert(key.numFields == 2)
        (key.getInt(0), key.getLong(1), data.map(fromRow).toSeq)
    }.toSeq

    assert(result ==
      (1, 2L, Seq(input(0), input(1))) ::
      (1, 3L, Seq(input(2))) ::
      (2, 1L, Seq(input(3))) ::
      (3, 2L, Seq(input(4))) :: Nil)
  }

  test("do nothing to the value iterator") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()
    val toRow = encoder.createSerializer()
    val input = Seq(Row(1, "a"), Row(1, "b"), Row(2, "c"))
    val grouped = GroupedIterator(input.iterator.map(toRow),
      Seq($"i".int.at(0)), schema.toAttributes)

    assert(grouped.length == 2)
  }

  test("group batch by 1 column") {
    val schema = new StructType().add("i", IntegerType).add("s", StringType)
    val encoder = RowEncoder(schema).resolveAndBind()
    val toRow = encoder.createSerializer()
    val fromRow = encoder.createDeserializer()
    // these rows will be iterated in batches of at least 3 rows, while not splitting groups
    val input = Seq(
      // first batch
      Row(1, "a"),
      Row(2, "b"),
      Row(3, "c"),
      // second batch
      Row(4, "d"), Row(5, "e"), Row(5, "f"), Row(5, "g"),
      // third batch
      Row(6, "h"), Row(6, "i"), Row(6, "j"), Row(6, "k"),
      // fourth batch
      Row(7, "l"),
      Row(8, "m")
    )
    val groupBatches = GroupedBatchIterator(input.iterator.map(toRow),
      Seq($"i".int.at(0)), schema.toAttributes, 3)

    val result = groupBatches.zipWithIndex.map { case (batch, idx) =>
      idx -> batch.map(fromRow).toList
    }.toList

    assert(result ==
      0 -> Seq(input(0), input(1), input(2)) ::
        1 -> Seq(input(3), input(4), input(5), input(6)) ::
        2 -> Seq(input(7), input(8), input(9), input(10)) ::
        3 -> Seq(input(11), input(12)) :: Nil)
  }
}
