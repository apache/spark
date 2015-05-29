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

import java.util.NoSuchElementException

import scala.collection.mutable.Buffer

import org.scalatest.FunSuite
import org.scalatest.Matchers

class NextIteratorSuite extends FunSuite with Matchers {
  test("one iteration") {
    val i = new StubIterator(Buffer(1))
    i.hasNext should be (true)
    i.next should be (1)
    i.hasNext should be (false)
    intercept[NoSuchElementException] { i.next() }
  }

  test("two iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.hasNext should be (true)
    i.next should be (1)
    i.hasNext should be (true)
    i.next should be (2)
    i.hasNext should be (false)
    intercept[NoSuchElementException] { i.next() }
  }

  test("empty iteration") {
    val i = new StubIterator(Buffer())
    i.hasNext should be (false)
    intercept[NoSuchElementException] { i.next() }
  }

  test("close is called once for empty iterations") {
    val i = new StubIterator(Buffer())
    i.hasNext should be (false)
    i.hasNext should be (false)
    i.closeCalled should be (1)
  }

  test("close is called once for non-empty iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.next should be (1)
    i.next should be (2)
    // close isn't called until we check for the next element
    i.closeCalled should be (0)
    i.hasNext should be (false)
    i.closeCalled should be (1)
    i.hasNext should be (false)
    i.closeCalled should be (1)
  }

  class StubIterator(ints: Buffer[Int])  extends NextIterator[Int] {
    var closeCalled = 0

    override def getNext(): Int = {
      if (ints.size == 0) {
        finished = true
        0
      } else {
        ints.remove(0)
      }
    }

    override def close() {
      closeCalled += 1
    }
  }
}
