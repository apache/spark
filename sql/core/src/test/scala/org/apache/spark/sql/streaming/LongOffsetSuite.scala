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
import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.{CompositeOffset, Offset, LongOffset}


trait OffsetSuite extends SparkFunSuite {
  /** Creates test to check all the comparisons of offsets given a `one` that is less than `two`. */
  def compare(one: Offset, two: Offset): Unit = {
    test(s"comparision $one <=> $two") {
      assert(one < two)
      assert(one <= two)
      assert(one <= one)
      assert(two > one)
      assert(two >= one)
      assert(one >= one)
      assert(one == one)
    }
  }
}

class LongOffsetSuite extends OffsetSuite {
  val one = new LongOffset(1)
  val two = new LongOffset(2)
  compare(one, two)
}

class CompositeOffsetSuite extends OffsetSuite {
  compare(
    one = CompositeOffset(Some(new LongOffset(1)) :: Nil),
    two = CompositeOffset(Some(new LongOffset(2)) :: Nil))

  compare(
    one = CompositeOffset(None :: Nil),
    two = CompositeOffset(Some(new LongOffset(2)) :: Nil))
}
