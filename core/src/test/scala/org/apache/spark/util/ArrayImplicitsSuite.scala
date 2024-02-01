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

package org.apache.spark.util

import scala.collection.immutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ArrayImplicits._

class ArrayImplicitsSuite extends SparkFunSuite {

  test("Int Array") {
    val data = Array(1, 2, 3)
    val arraySeq = data.toImmutableArraySeq
    assert(arraySeq.getClass === classOf[immutable.ArraySeq.ofInt])
    assert(arraySeq.length === 3)
    assert(arraySeq.unsafeArray.sameElements(data))
  }

  test("TestClass Array") {
    val data = Array(TestClass(1), TestClass(2), TestClass(3))
    val arraySeq = data.toImmutableArraySeq
    assert(arraySeq.getClass === classOf[immutable.ArraySeq.ofRef[TestClass]])
    assert(arraySeq.length === 3)
    assert(arraySeq.unsafeArray.sameElements(data))
  }

  test("Null Array") {
    val data: Array[Int] = null
    val arraySeq = data.toImmutableArraySeq
    assert(arraySeq == null)
  }

  case class TestClass(i: Int)
}
