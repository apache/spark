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

package org.apache.spark.sql.streaming

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.{CompositeOffset, LongOffset, Offset}

trait OffsetSuite extends SparkFunSuite {
  /** Creates test to check all the comparisons of offsets given a `one` that is less than `two`. */
  def compare(one: Offset, two: Offset): Unit = {
    test(s"comparison $one <=> $two") {
      assert(one == one)
      assert(two == two)
      assert(one != two)
      assert(two != one)
    }
  }
}

class LongOffsetSuite extends OffsetSuite {
  val one = LongOffset(1)
  val two = LongOffset(2)
  compare(one, two)
}

class CompositeOffsetSuite extends OffsetSuite {
  compare(
    one = CompositeOffset(Some(LongOffset(1)) :: Nil),
    two = CompositeOffset(Some(LongOffset(2)) :: Nil))

  compare(
    one = CompositeOffset(None :: Nil),
    two = CompositeOffset(Some(LongOffset(2)) :: Nil))

  compare(
    one = CompositeOffset.fill(LongOffset(0), LongOffset(1)),
    two = CompositeOffset.fill(LongOffset(1), LongOffset(2)))

  compare(
    one = CompositeOffset.fill(LongOffset(1), LongOffset(1)),
    two = CompositeOffset.fill(LongOffset(1), LongOffset(2)))

}

