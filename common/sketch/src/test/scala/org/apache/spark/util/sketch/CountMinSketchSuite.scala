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

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class CountMinSketchSuite extends FunSuite { // scalastyle:ignore funsuite
  private val epsOfTotalCount = 0.0001

  private val confidence = 0.99

  private val seed = 42

  def testAccuracy[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    test(s"accuracy - $typeName") {
      val r = new Random()

      val numAllItems = 1000000
      val allItems = Array.fill(numAllItems)(itemGenerator(r))

      val numSamples = numAllItems / 10
      val sampledItemIndices = Array.fill(numSamples)(r.nextInt(numAllItems))

      val exactFreq = {
        val sampledItems = sampledItemIndices.map(allItems)
        sampledItems.groupBy(identity).mapValues(_.length.toLong)
      }

      val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
      sampledItemIndices.foreach(i => sketch.add(allItems(i)))

      val probCorrect = {
        val numErrors = allItems.map { item =>
          val count = exactFreq.getOrElse(item, 0L)
          val ratio = (sketch.estimateCount(item) - count).toDouble / numAllItems
          if (ratio > epsOfTotalCount) 1 else 0
        }.sum

        1D - numErrors.toDouble / numAllItems
      }

      assert(
        probCorrect > confidence,
        s"Confidence not reached: required $confidence, reached $probCorrect"
      )
    }
  }

  def testMergeInPlace[T: ClassTag](typeName: String)(itemGenerator: Random => T): Unit = {
    test(s"mergeInPlace - $typeName") {
      val r = new Random()
      val numToMerge = 5
      val numItemsPerSketch = 100000
      val perSketchItems = Array.fill(numToMerge, numItemsPerSketch) {
        itemGenerator(r)
      }

      val sketches = perSketchItems.map { items =>
        val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
        items.foreach(sketch.add)
        sketch
      }

      val mergedSketch = sketches.reduce(_ mergeInPlace _)

      val expectedSketch = {
        val sketch = CountMinSketch.create(epsOfTotalCount, confidence, seed)
        perSketchItems.foreach(_.foreach(sketch.add))
        sketch
      }

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
}
