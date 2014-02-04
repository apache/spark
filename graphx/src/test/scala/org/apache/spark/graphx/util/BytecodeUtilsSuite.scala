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

package org.apache.spark.graphx.util

import org.scalatest.FunSuite


class BytecodeUtilsSuite extends FunSuite {

  import BytecodeUtilsSuite.TestClass

  test("closure invokes a method") {
    val c1 = {e: TestClass => println(e.foo); println(e.bar); println(e.baz); }
    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "foo"))
    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "bar"))
    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "baz"))

    val c2 = {e: TestClass => println(e.foo); println(e.bar); }
    assert(BytecodeUtils.invokedMethod(c2, classOf[TestClass], "foo"))
    assert(BytecodeUtils.invokedMethod(c2, classOf[TestClass], "bar"))
    assert(!BytecodeUtils.invokedMethod(c2, classOf[TestClass], "baz"))

    val c3 = {e: TestClass => println(e.foo); }
    assert(BytecodeUtils.invokedMethod(c3, classOf[TestClass], "foo"))
    assert(!BytecodeUtils.invokedMethod(c3, classOf[TestClass], "bar"))
    assert(!BytecodeUtils.invokedMethod(c3, classOf[TestClass], "baz"))
  }

  test("closure inside a closure invokes a method") {
    val c1 = {e: TestClass => println(e.foo); println(e.bar); println(e.baz); }
    val c2 = {e: TestClass => c1(e); println(e.foo); }
    assert(BytecodeUtils.invokedMethod(c2, classOf[TestClass], "foo"))
    assert(BytecodeUtils.invokedMethod(c2, classOf[TestClass], "bar"))
    assert(BytecodeUtils.invokedMethod(c2, classOf[TestClass], "baz"))
  }

  test("closure inside a closure inside a closure invokes a method") {
    val c1 = {e: TestClass => println(e.baz); }
    val c2 = {e: TestClass => c1(e); println(e.foo); }
    val c3 = {e: TestClass => c2(e) }
    assert(BytecodeUtils.invokedMethod(c3, classOf[TestClass], "foo"))
    assert(!BytecodeUtils.invokedMethod(c3, classOf[TestClass], "bar"))
    assert(BytecodeUtils.invokedMethod(c3, classOf[TestClass], "baz"))
  }

  test("closure calling a function that invokes a method") {
    def zoo(e: TestClass) {
      println(e.baz)
    }
    val c1 = {e: TestClass => zoo(e)}
    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "foo"))
    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "bar"))
    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "baz"))
  }

  test("closure calling a function that invokes a method which uses another closure") {
    val c2 = {e: TestClass => println(e.baz)}
    def zoo(e: TestClass) {
      c2(e)
    }
    val c1 = {e: TestClass => zoo(e)}
    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "foo"))
    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "bar"))
    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "baz"))
  }

  test("nested closure") {
    val c2 = {e: TestClass => println(e.baz)}
    def zoo(e: TestClass, c: TestClass => Unit) {
      c(e)
    }
    val c1 = {e: TestClass => zoo(e, c2)}
    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "foo"))
    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "bar"))
    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "baz"))
  }

  // The following doesn't work yet, because the byte code doesn't contain any information
  // about what exactly "c" is.
//  test("invoke interface") {
//    val c1 = {e: TestClass => c(e)}
//    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "foo"))
//    assert(!BytecodeUtils.invokedMethod(c1, classOf[TestClass], "bar"))
//    assert(BytecodeUtils.invokedMethod(c1, classOf[TestClass], "baz"))
//  }

  private val c = {e: TestClass => println(e.baz)}
}


object BytecodeUtilsSuite {
  class TestClass(val foo: Int, val bar: Long) {
    def baz: Boolean = false
  }
}
