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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.serializer.SerializerInstance

// TODO: REMOVE ME
import java.util.Properties
import org.apache.log4j.PropertyConfigurator

/**
 * Another test suite for the closure cleaner that is finer-grained.
 * For tests involving end-to-end Spark jobs, see {{ClosureCleanerSuite}}.
 */
class ClosureCleanerSuite2 extends FunSuite with BeforeAndAfterAll {

  // Start a SparkContext so that SparkEnv.get.closureSerializer is accessible
  // We do not actually use this explicitly except to stop it later
  private var sc: SparkContext = null
  private var closureSerializer: SerializerInstance = null

  override def beforeAll(): Unit = {
    sc = new SparkContext("local", "test")
    closureSerializer = sc.env.closureSerializer.newInstance()
  }

  override def afterAll(): Unit = {
    sc.stop()
    sc = null
    closureSerializer = null
  }

  // Some fields and methods that belong to this class, which is itself not serializable
  private val someSerializableValue = 1
  private val someNonSerializableValue = new NonSerializable
  private def someSerializableMethod() = 1
  private def someNonSerializableMethod() = new NonSerializable

  private def assertSerializable(closure: AnyRef, serializable: Boolean): Unit = {
    if (serializable) {
      closureSerializer.serialize(closure)
    } else {
      intercept[NotSerializableException] {
        closureSerializer.serialize(closure)
      }
    }
  }

  private def testClean(
      closure: AnyRef,
      serializableBefore: Boolean,
      serializableAfter: Boolean): Unit = {
    testClean(closure, serializableBefore, serializableAfter, transitive = true)
    testClean(closure, serializableBefore, serializableAfter, transitive = false)
  }

  private def testClean(
      closure: AnyRef,
      serializableBefore: Boolean,
      serializableAfter: Boolean,
      transitive: Boolean): Unit = {
    assertSerializable(closure, serializableBefore)
    // If the resulting closure is not serializable even after
    // cleaning, we expect ClosureCleaner to throw a SparkException
    intercept[SparkException] {
      ClosureCleaner.clean(closure, checkSerializable = true, transitive)
      // Otherwise, if we do expect the closure to be serializable after the
      // clean, throw the SparkException ourselves so scalatest is happy
      if (serializableAfter) { throw new SparkException("no-op") }
    }
    assertSerializable(closure, serializableAfter)
  }

  test("clean basic serializable closures") {
    val localSerializableVal = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => Array[String]("a", "b", "c")
    val closure3 = (s: String, arr: Array[Long]) => s + arr.mkString(", ")
    val closure4 = () => localSerializableVal
    val closure5 = () => new NonSerializable(5) // we're just serializing the class information
    val closure1r = closure1()
    val closure2r = closure2()
    val closure3r = closure3("g", Array(1, 5, 8))
    val closure4r = closure4()
    val closure5r = closure5()

    testClean(closure1, serializableBefore = true, serializableAfter = true)
    testClean(closure2, serializableBefore = true, serializableAfter = true)
    testClean(closure3, serializableBefore = true, serializableAfter = true)
    testClean(closure4, serializableBefore = true, serializableAfter = true)
    testClean(closure5, serializableBefore = true, serializableAfter = true)

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

    // These are not cleanable because they ultimately reference the `this` pointer
    testClean(closure1, serializableBefore = false, serializableAfter = false)
    testClean(closure2, serializableBefore = false, serializableAfter = false)
    testClean(closure3, serializableBefore = false, serializableAfter = false)
    testClean(closure4, serializableBefore = false, serializableAfter = false)
    testClean(closure5, serializableBefore = false, serializableAfter = false)
  }

  test("clean basic nested serializable closures") {
    val localSerializableValue = someSerializableValue
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + localSerializableValue } // 1 level of nesting
    }
    val closure2 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).map { y => y + localSerializableValue } // 2 levels
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

    testClean(closure1, serializableBefore = true, serializableAfter = true)
    testClean(closure2, serializableBefore = true, serializableAfter = true)
    testClean(closure3, serializableBefore = true, serializableAfter = true)

    assert(closure1(1) === closure1r)
    assert(closure2(2) === closure2r)
    assert(closure3(3, 4, 5) === closure3r)
  }

  test("clean basic nested non-serializable closures") {
    def localSerializableMethod() = someSerializableValue
    val localNonSerializableValue = someNonSerializableValue
    val closure1 = (i: Int) => { (1 to i).map { x => x + someSerializableValue } }
    val closure2 = (j: Int) => { (1 to j).map { x => x + someSerializableMethod() } }
    val closure4 = (k: Int) => { (1 to k).map { x => x + localSerializableMethod() } }
    val closure3 = (l: Int) => { (1 to l).map { x => localNonSerializableValue } }
    // This is non-serializable no matter how many levels we nest it
    val closure5 = (m: Int) => {
      (1 to m).foreach { x =>
        (1 to x).foreach { y =>
          (1 to y).foreach { z =>
            someSerializableValue
          }
        }
      }
    }

    testClean(closure1, serializableBefore = false, serializableAfter = false)
    testClean(closure2, serializableBefore = false, serializableAfter = false)
    testClean(closure3, serializableBefore = false, serializableAfter = false)
    testClean(closure4, serializableBefore = false, serializableAfter = false)
    testClean(closure5, serializableBefore = false, serializableAfter = false)
  }

  test("clean complicated nested serializable closures") {
    val localSerializableValue = someSerializableValue

    // Reference local fields from all levels
    val closure1 = (i: Int) => {
      val a = 1
      (1 to i).flatMap { x =>
        val b = a + 1
        (1 to x).map { y =>
          y + a + b + localSerializableValue
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
          // in the ClosureCleanerSuite2. This is why `localSerializableValue` here must be a val.
          y + a1 + a2 + b1 + b2 + localSerializableValue
        }
      }
    }

    val closure1r = closure1(1)
    val closure2r = closure2(2)
    testClean(closure1, serializableBefore = true, serializableAfter = true)
    testClean(closure2, serializableBefore = true, serializableAfter = true)
    assert(closure1(1) == closure1r)
    assert(closure2(2) == closure2r)
  }

  test("clean complicated nested non-serializable closures") {
    val localSerializableValue = someSerializableValue

    // Note that we are not interested in cleaning the outer closures here
    // The only reason why they exist is to nest the inner closures

    val test1 = () => {
      val a = localSerializableValue
      val b = sc
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // This closure explicitly references a non-serializable field
      // There is no way to clean it
      testClean(inner1, serializableBefore = false, serializableAfter = false)

      // This closure is serializable to begin with since
      // it does not have a pointer to the outer closure
      testClean(inner2, serializableBefore = true, serializableAfter = true)
    }

    // Same as above, but the `val a` becomes `def a`
    // The difference here is that all inner closures now have pointers to the outer closure
    val test2 = () => {
      def a = localSerializableValue
      val b = sc
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // As before, this closure is neither serializable nor cleanable
      testClean(inner1, serializableBefore = false, serializableAfter = false)

      // This closure is no longer serializable because it now has a pointer to the outer closure,
      // which is itself not serializable because it has a pointer to the ClosureCleanerSuite.
      // If we do not clean transitively, we will not null out this parent pointer.
      testClean(inner2, serializableBefore = false, serializableAfter = false, transitive = false)

      // If we clean transitively, we will find that method `a` does not actually reference the
      // outer closure's parent (i.e. the ClosureCleanerSuite), so we can additionally null out
      // the outer closure's parent pointer. This will make `inner2` serializable.
      testClean(inner2, serializableBefore = false, serializableAfter = true, transitive = true)
    }

    test1()
    test2()
  }





  // TODO: REMOVE ME
  configureLog4j()
  private def configureLog4j(): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", "WARN, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    pro.put("log4j.logger.org.apache.spark.util.ClosureCleaner", "DEBUG")
    PropertyConfigurator.configure(pro)
  }

}
