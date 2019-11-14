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

import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}

import org.apache.spark.{SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.serializer.SerializerInstance

/**
 * Another test suite for the closure cleaner that is finer-grained.
 * For tests involving end-to-end Spark jobs, see {{ClosureCleanerSuite}}.
 */
class ClosureCleanerSuite2 extends SparkFunSuite with BeforeAndAfterAll with PrivateMethodTester {

  // Start a SparkContext so that the closure serializer is accessible
  // We do not actually use this explicitly otherwise
  private var sc: SparkContext = null
  private var closureSerializer: SerializerInstance = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "test")
    closureSerializer = sc.env.closureSerializer.newInstance()
  }

  override def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
      closureSerializer = null
    } finally {
      super.afterAll()
    }
  }

  // Some fields and methods to reference in inner closures later
  private val someSerializableValue = 1
  private val someNonSerializableValue = new NonSerializable
  private def someSerializableMethod() = 1
  private def someNonSerializableMethod() = new NonSerializable

  /** Assert that the given closure is serializable (or not). */
  private def assertSerializable(closure: AnyRef, serializable: Boolean): Unit = {
    if (serializable) {
      closureSerializer.serialize(closure)
    } else {
      intercept[NotSerializableException] {
        closureSerializer.serialize(closure)
      }
    }
  }

  /**
   * Helper method for testing whether closure cleaning works as expected.
   * This cleans the given closure twice, with and without transitive cleaning.
   *
   * @param closure closure to test cleaning with
   * @param serializableBefore if true, verify that the closure is serializable
   *                           before cleaning, otherwise assert that it is not
   * @param serializableAfter if true, assert that the closure is serializable
   *                          after cleaning otherwise assert that it is not
   */
  private def verifyCleaning(
      closure: AnyRef,
      serializableBefore: Boolean,
      serializableAfter: Boolean): Unit = {
    verifyCleaning(closure, serializableBefore, serializableAfter, transitive = true)
    verifyCleaning(closure, serializableBefore, serializableAfter, transitive = false)
  }

  /** Helper method for testing whether closure cleaning works as expected. */
  private def verifyCleaning(
      closure: AnyRef,
      serializableBefore: Boolean,
      serializableAfter: Boolean,
      transitive: Boolean): Unit = {
    assertSerializable(closure, serializableBefore)
    // If the resulting closure is not serializable even after
    // cleaning, we expect ClosureCleaner to throw a SparkException
    if (serializableAfter) {
      ClosureCleaner.clean(closure, checkSerializable = true, transitive)
    } else {
      intercept[SparkException] {
        ClosureCleaner.clean(closure, checkSerializable = true, transitive)
      }
    }
    assertSerializable(closure, serializableAfter)
  }

  test("clean basic serializable closures") {
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => Array[String]("a", "b", "c")
    val closure3 = (s: String, arr: Array[Long]) => s + arr.mkString(", ")
    val closure4 = () => localValue
    val closure5 = () => new NonSerializable(5) // we're just serializing the class information
    val closure1r = closure1()
    val closure2r = closure2()
    val closure3r = closure3("g", Array(1, 5, 8))
    val closure4r = closure4()
    val closure5r = closure5()

    verifyCleaning(closure1, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure2, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure3, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure4, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure5, serializableBefore = true, serializableAfter = true)

    // Verify that closures can still be invoked and the result still the same
    assert(closure1() === closure1r)
    assert(closure2() === closure2r)
    assert(closure3("g", Array(1, 5, 8)) === closure3r)
    assert(closure4() === closure4r)
    assert(closure5() === closure5r)
  }

  test("clean basic non-serializable closures") {
    val closure1 = () => this // ClosureCleanerSuite2 is not serializable
    val closure5 = () => someSerializableValue
    val closure3 = () => someSerializableMethod()
    val closure4 = () => someNonSerializableValue
    val closure2 = () => someNonSerializableMethod()

    // These are not cleanable because they ultimately reference the ClosureCleanerSuite2
    verifyCleaning(closure1, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure2, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure3, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure4, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure5, serializableBefore = false, serializableAfter = false)
  }

  test("clean basic nested serializable closures") {
    val localValue = someSerializableValue
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + localValue } // 1 level of nesting
    }
    val closure2 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).map { y => y + localValue } // 2 levels
      }
    }
    val closure3 = (k: Int, l: Int, m: Int) => {
      (1 to k).flatMap(closure2) ++ // 4 levels
      (1 to l).flatMap(closure1) ++ // 3 levels
      (1 to m).map { x => x + 1 } // 2 levels
    }
    val closure1r = closure1(1)
    val closure2r = closure2(2)
    val closure3r = closure3(3, 4, 5)

    verifyCleaning(closure1, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure2, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure3, serializableBefore = true, serializableAfter = true)

    // Verify that closures can still be invoked and the result still the same
    assert(closure1(1) === closure1r)
    assert(closure2(2) === closure2r)
    assert(closure3(3, 4, 5) === closure3r)
  }

  test("clean basic nested non-serializable closures") {
    def localSerializableMethod(): Int = someSerializableValue
    val localNonSerializableValue = someNonSerializableValue
    // These closures ultimately reference the ClosureCleanerSuite2
    // Note that even accessing `val` that is an instance variable involves a method call
    val closure1 = (i: Int) => { (1 to i).map { x => x + someSerializableValue } }
    val closure2 = (j: Int) => { (1 to j).map { x => x + someSerializableMethod() } }
    val closure4 = (k: Int) => { (1 to k).map { x => x + localSerializableMethod() } }
    // This closure references a local non-serializable value
    val closure3 = (l: Int) => { (1 to l).map { _ => localNonSerializableValue } }
    // This is non-serializable no matter how many levels we nest it
    val closure5 = (m: Int) => {
      (1 to m).foreach { x =>
        (1 to x).foreach { y =>
          (1 to y).foreach { _ =>
            someSerializableValue
          }
        }
      }
    }

    verifyCleaning(closure1, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure2, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure3, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure4, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure5, serializableBefore = false, serializableAfter = false)
  }

  test("clean complicated nested serializable closures") {
    val localValue = someSerializableValue

    // Here we assume that if the outer closure is serializable,
    // then all inner closures must also be serializable

    // Reference local fields from all levels
    val closure1 = (i: Int) => {
      val a = 1
      (1 to i).flatMap { x =>
        val b = a + 1
        (1 to x).map { y =>
          y + a + b + localValue
        }
      }
    }

    // Reference local fields and methods from all levels within the outermost closure
    val closure2 = (i: Int) => {
      val a1 = 1
      def a2 = 2
      (1 to i).flatMap { x =>
        val b1 = a1 + 1
        def b2 = a2 + 1
        (1 to x).map { y =>
          // If this references a method outside the outermost closure, then it will try to pull
          // in the ClosureCleanerSuite2. This is why `localValue` here must be a local `val`.
          y + a1 + a2 + b1 + b2 + localValue
        }
      }
    }

    val closure1r = closure1(1)
    val closure2r = closure2(2)
    verifyCleaning(closure1, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure2, serializableBefore = true, serializableAfter = true)
    assert(closure1(1) == closure1r)
    assert(closure2(2) == closure2r)
  }

  test("clean complicated nested non-serializable closures") {
    val localValue = someSerializableValue

    // Note that we are not interested in cleaning the outer closures here (they are not cleanable)
    // The only reason why they exist is to nest the inner closures

    val test1 = () => {
      val a = localValue
      val b = sc
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // This closure explicitly references a non-serializable field
      // There is no way to clean it
      verifyCleaning(inner1, serializableBefore = false, serializableAfter = false)

      // This closure is serializable to begin with since it does not need a pointer to
      // the outer closure (it only references local variables)
      verifyCleaning(inner2, serializableBefore = true, serializableAfter = true)
    }

    // Same as above, but the `val a` becomes `def a`
    // The difference here is that all inner closures now have pointers to the outer closure
    val test2 = () => {
      def a = localValue
      val b = sc
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // As before, this closure is neither serializable nor cleanable
      verifyCleaning(inner1, serializableBefore = false, serializableAfter = false)
      verifyCleaning(
        inner2, serializableBefore = true, serializableAfter = true)
    }

    // Same as above, but with more levels of nesting
    val test3 = () => { () => test1() }
    val test4 = () => { () => test2() }
    val test5 = () => { () => { () => test3() } }
    val test6 = () => { () => { () => test4() } }

    test1()
    test2()
    test3()()
    test4()()
    test5()()()
    test6()()()
  }

}
