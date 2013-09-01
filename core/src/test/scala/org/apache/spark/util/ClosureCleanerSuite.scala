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

import java.io.NotSerializableException

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.LocalSparkContext._

class ClosureCleanerSuite extends FunSuite {
  test("closures inside an object") {
    assert(TestObject.run() === 30) // 6 + 7 + 8 + 9
  }

  test("closures inside a class") {
    val obj = new TestClass
    assert(obj.run() === 30) // 6 + 7 + 8 + 9
  }

  test("closures inside a class with no default constructor") {
    val obj = new TestClassWithoutDefaultConstructor(5)
    assert(obj.run() === 30) // 6 + 7 + 8 + 9
  }

  test("closures that don't use fields of the outer class") {
    val obj = new TestClassWithoutFieldAccess
    assert(obj.run() === 30) // 6 + 7 + 8 + 9
  }

  test("nested closures inside an object") {
    assert(TestObjectWithNesting.run() === 96) // 4 * (1+2+3+4) + 4 * (1+2+3+4) + 16 * 1
  }

  test("nested closures inside a class") {
    val obj = new TestClassWithNesting(1)
    assert(obj.run() === 96) // 4 * (1+2+3+4) + 4 * (1+2+3+4) + 16 * 1
  }
}

// A non-serializable class we create in closures to make sure that we aren't
// keeping references to unneeded variables from our outer closures.
class NonSerializable {}

object TestObject {
  def run(): Int = {
    var nonSer = new NonSerializable
    var x = 5
    return withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + x).reduce(_ + _)
    }
  }
}

class TestClass extends Serializable {
  var x = 5
  
  def getX = x

  def run(): Int = {
    var nonSer = new NonSerializable
    return withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + getX).reduce(_ + _)
    }
  }
}

class TestClassWithoutDefaultConstructor(x: Int) extends Serializable {
  def getX = x

  def run(): Int = {
    var nonSer = new NonSerializable
    return withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + getX).reduce(_ + _)
    }
  }
}

// This class is not serializable, but we aren't using any of its fields in our
// closures, so they won't have a $outer pointing to it and should still work.
class TestClassWithoutFieldAccess {
  var nonSer = new NonSerializable

  def run(): Int = {
    var nonSer2 = new NonSerializable
    var x = 5
    return withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + x).reduce(_ + _)
    }
  }
}


object TestObjectWithNesting {
  def run(): Int = {
    var nonSer = new NonSerializable
    var answer = 0
    return withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      var y = 1
      for (i <- 1 to 4) {
        var nonSer2 = new NonSerializable
        var x = i
        answer += nums.map(_ + x + y).reduce(_ + _)
      }
      answer
    }
  }
}

class TestClassWithNesting(val y: Int) extends Serializable {
  def getY = y

  def run(): Int = {
    var nonSer = new NonSerializable
    var answer = 0
    return withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      for (i <- 1 to 4) {
        var nonSer2 = new NonSerializable
        var x = i
        answer += nums.map(_ + x + getY).reduce(_ + _)
      }
      answer
    }
  }
}
