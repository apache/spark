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

package org.apache.spark.ml.extensions.seq

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class MultiSlidingSuite extends SparkFunSuite with Matchers {

  test("should calculate empty results for out-of-bound inputs") {
    multiSliding(Seq.empty, -1, -1) shouldBe Seq.empty
    multiSliding(Seq.empty, -1, 0)  shouldBe Seq.empty
    multiSliding(Seq.empty, 0, -1)  shouldBe Seq.empty
    multiSliding(Seq.empty, 0, 0)   shouldBe Seq.empty
    multiSliding(Seq.empty, 2, 1)   shouldBe Seq.empty
    multiSliding(Seq.empty, 2, 2)   shouldBe Seq.empty

    multiSliding(1 to 10, -1, -1) shouldBe Seq.empty
    multiSliding(1 to 10, -1, 0)  shouldBe Seq.empty
    multiSliding(1 to 10, 0, -1)  shouldBe Seq.empty
    multiSliding(1 to 10, 0, 0)   shouldBe Seq.empty
    multiSliding(1 to 10, 2, 1)   shouldBe Seq.empty
  }

  test("should calculate multiple sliding windows correctly") {
    multiSliding(1 to 5, min = 2, max = 4) shouldBe Seq(
      Seq(1, 2), Seq(1, 2, 3), Seq(1, 2, 3, 4),
      Seq(2, 3), Seq(2, 3, 4), Seq(2, 3, 4, 5),
      Seq(3, 4), Seq(3, 4, 5),
      Seq(4, 5))

    multiSliding(1 to 10, min = 2, max = 5) shouldBe Seq(
      Seq(1, 2), Seq(1, 2, 3), Seq(1, 2, 3, 4), Seq(1, 2, 3, 4, 5),
      Seq(2, 3), Seq(2, 3, 4), Seq(2, 3, 4, 5), Seq(2, 3, 4, 5, 6),
      Seq(3, 4), Seq(3, 4, 5), Seq(3, 4, 5, 6), Seq(3, 4, 5, 6, 7),
      Seq(4, 5), Seq(4, 5, 6), Seq(4, 5, 6, 7), Seq(4, 5, 6, 7, 8),
      Seq(5, 6), Seq(5, 6, 7), Seq(5, 6, 7, 8), Seq(5, 6, 7, 8, 9),
      Seq(6, 7), Seq(6, 7, 8), Seq(6, 7, 8, 9), Seq(6, 7, 8, 9, 10),
      Seq(7, 8), Seq(7, 8, 9), Seq(7, 8, 9, 10),
      Seq(8, 9), Seq(8, 9, 10),
      Seq(9, 10)
    )
  }

}
