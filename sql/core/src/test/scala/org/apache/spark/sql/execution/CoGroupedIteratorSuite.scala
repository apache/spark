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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper

class CoGroupedIteratorSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("basic") {
    val leftInput = Seq(create_row(1, "a"), create_row(1, "b"), create_row(2, "c")).iterator
    val rightInput = Seq(create_row(1, 2L), create_row(2, 3L), create_row(3, 4L)).iterator
    val leftGrouped = GroupedIterator(leftInput, Seq($"i".int.at(0)),
      Seq($"i".int, $"s".string))
    val rightGrouped = GroupedIterator(rightInput, Seq($"i".int.at(0)),
      Seq($"i".int, $"l".long))
    val cogrouped = new CoGroupedIterator(Seq(leftGrouped, rightGrouped), Seq($"i".int))

    val result = cogrouped.map {
      case (key, Seq(leftData, rightData)) =>
        assert(key.numFields == 1)
        (key.getInt(0), leftData.toSeq, rightData.toSeq)
    }.toSeq
    assert(result ==
      (1,
        Seq(create_row(1, "a"), create_row(1, "b")),
        Seq(create_row(1, 2L))) ::
      (2,
        Seq(create_row(2, "c")),
        Seq(create_row(2, 3L))) ::
      (3,
        Seq.empty,
        Seq(create_row(3, 4L))) ::
      Nil
    )
  }

  test("multi") {
    val firstInput = Seq(create_row(1, "a"), create_row(1, "b"), create_row(2, "c")).iterator
    val secondInput = Seq(create_row(1, 2L), create_row(2, 3L), create_row(3, 4L)).iterator
    val thirdInput = Seq(create_row(2, 0.0), create_row(3, 1.0), create_row(4, 2.0)).iterator
    val firstGrouped = GroupedIterator(firstInput, Seq($"i".int.at(0)),
      Seq($"i".int, $"s".string))
    val secondGrouped = GroupedIterator(secondInput, Seq($"i".int.at(0)),
      Seq($"i".int, $"l".long))
    val thirdGrouped = GroupedIterator(thirdInput, Seq($"i".int.at(0)),
      Seq($"i".int, $"l".double))
    val cogrouped = new CoGroupedIterator(
      Seq(firstGrouped, secondGrouped, thirdGrouped), Seq($"i".int))

    val result = cogrouped.map {
      case (key, Seq(firstData, secondData, thirdData)) =>
        assert(key.numFields == 1)
        (key.getInt(0), firstData.toSeq, secondData.toSeq, thirdData.toSeq)
    }.toSeq
    assert(result ==
      (1,
        Seq(create_row(1, "a"), create_row(1, "b")),
        Seq(create_row(1, 2L)),
        Seq.empty) ::
      (2,
        Seq(create_row(2, "c")),
        Seq(create_row(2, 3L)),
        Seq(create_row(2, 0.0))) ::
      (3,
        Seq.empty,
        Seq(create_row(3, 4L)),
        Seq(create_row(3, 1.0))) ::
      (4,
        Seq.empty,
        Seq.empty,
        Seq(create_row(4, 2.0))) ::
      Nil
    )
  }

  test("SPARK-11393: respect the fact that GroupedIterator.hasNext is not idempotent") {
    val leftInput = Seq(create_row(2, "a")).iterator
    val rightInput = Seq(create_row(1, 2L)).iterator
    val leftGrouped = GroupedIterator(leftInput, Seq($"i".int.at(0)),
      Seq($"i".int, $"s".string))
    val rightGrouped = GroupedIterator(rightInput, Seq($"i".int.at(0)), Seq($"i".int, $"l".long))
    val cogrouped = new CoGroupedIterator(Seq(leftGrouped, rightGrouped), Seq($"i".int))

    val result = cogrouped.map {
      case (key, Seq(leftData, rightData)) =>
        assert(key.numFields == 1)
        (key.getInt(0), leftData.toSeq, rightData.toSeq)
    }.toSeq

    assert(result ==
      (1,
        Seq.empty,
        Seq(create_row(1, 2L))) ::
      (2,
        Seq(create_row(2, "a")),
        Seq.empty) ::
      Nil
    )
  }
}
