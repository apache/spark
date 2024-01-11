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

package org.apache.spark.util.sketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class CountMinSketchSuite extends AnyFunSuite { // scalastyle:ignore funsuite
  private val epsOfTotalCount = 0.01

  private val confidence = 0.9

  private val seed = 42

  // Serializes and deserializes a given `CountMinSketch`, then checks whether the deserialized
  // version is equivalent to the original one.
  private def checkSerDe(sketch: CountMinSketch): Unit = {
    val out = new ByteArrayOutputStream()
    sketch.writeTo(out)

    val in = new ByteArrayInputStream(out.toByteArray)
    val deserialized = CountMinSketch.readFrom(in)

    assert(sketch === deserialized)
  }

  def testAccuracy[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    test(s"accuracy - $typeName") {
      // Uses fixed seed to ensure reproducible test execution
      val r = new Random(31)

      val numAllItems = 1000000
      val allItems = Array.fill(numAllItems)(itemGenerator(r))

      val numSamples = numAllItems / 10
      val sampledItemIndices = Array.fill(numSamples)(r.nextInt(numAllItems))

      val exactFreq = {
        val sampledItems = sampledItemIndices.map(allItems)
        sampledItems.groupBy(identity).transform((_, v) => v.length.toLong)
      }

      val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
      checkSerDe(sketch)

      sampledItemIndices.foreach(i => sketch.add(allItems(i)))
      checkSerDe(sketch)

      val probCorrect = {
        val numErrors = allItems.map { item =>
          val count = exactFreq.getOrElse(item, 0L)
          val ratio = (sketch.estimateCount(item) - count).toDouble / numAllItems
          if (ratio > epsOfTotalCount) 1 else 0
        }.sum

        1.0 - (numErrors.toDouble / numAllItems)
      }

      assert(
        probCorrect > confidence,
        s"Confidence not reached: required $confidence, reached $probCorrect"
      )
    }
  }

  def testMergeInPlace[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    test(s"mergeInPlace - $typeName") {
      // Uses fixed seed to ensure reproducible test execution
      val r = new Random(31)

      val numToMerge = 5
      val numItemsPerSketch = 100000
      val perSketchItems = Array.fill(numToMerge, numItemsPerSketch) { itemGenerator(r) }

      val sketches = perSketchItems.map { items =>
        val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
        checkSerDe(sketch)

        items.foreach(sketch.add)
        checkSerDe(sketch)

        sketch
      }

      val mergedSketch = sketches.reduce(_ mergeInPlace _)
      checkSerDe(mergedSketch)

      val expectedSketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
      perSketchItems.foreach(_.foreach(expectedSketch.add))

      perSketchItems.foreach {
        _.foreach { item =>
          assert(mergedSketch.estimateCount(item) === expectedSketch.estimateCount(item))
        }
      }
    }
  }

  def testItemType[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    testAccuracy[T](typeName)(itemGenerator)
    testMergeInPlace[T](typeName)(itemGenerator)
  }

  testItemType[Byte]("Byte") { _.nextInt().toByte }

  testItemType[Short]("Short") { _.nextInt().toShort }

  testItemType[Int]("Int") { _.nextInt() }

  testItemType[Long]("Long") { _.nextLong() }

  testItemType[String]("String") { r => r.nextString(r.nextInt(20)) }

  testItemType[Array[Byte]]("Byte array") { r => r.nextString(r.nextInt(60)).getBytes }

  test("incompatible merge") {
    intercept[IncompatibleMergeException] {
      CountMinSketch.create(10, 10, 1).mergeInPlace(null)
    }

    intercept[IncompatibleMergeException] {
      val sketch1 = CountMinSketch.create(10, 20, 1)
      val sketch2 = CountMinSketch.create(10, 20, 2)
      sketch1.mergeInPlace(sketch2)
    }

    intercept[IncompatibleMergeException] {
      val sketch1 = CountMinSketch.create(10, 10, 1)
      val sketch2 = CountMinSketch.create(10, 20, 2)
      sketch1.mergeInPlace(sketch2)
    }
  }
}
