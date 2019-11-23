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

package org.apache.spark.sql

import java.nio.ByteBuffer
import java.util.Arrays

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types._

/**
 * Tests of [[RandomDataGenerator]].
 */
class RandomDataGeneratorSuite extends SparkFunSuite {

  /**
   * Tests random data generation for the given type by using it to generate random values then
   * converting those values into their Catalyst equivalents using CatalystTypeConverters.
   */
  def testRandomDataGeneration(dataType: DataType, nullable: Boolean = true): Unit = {
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(dataType)
    val generator = RandomDataGenerator.forType(dataType, nullable, new Random(33)).getOrElse {
      fail(s"Random data generator was not defined for $dataType")
    }
    if (nullable) {
      assert(Iterator.fill(100)(generator()).contains(null))
    } else {
      assert(!Iterator.fill(100)(generator()).contains(null))
    }
    for (_ <- 1 to 10) {
      val generatedValue = generator()
      toCatalyst(generatedValue)
    }
  }

  // Basic types:
  for (
    dataType <- DataTypeTestUtils.atomicTypes;
    nullable <- Seq(true, false)
    if !dataType.isInstanceOf[DecimalType]) {
    test(s"$dataType (nullable=$nullable)") {
      testRandomDataGeneration(dataType)
    }
  }

  for (
    arrayType <- DataTypeTestUtils.atomicArrayTypes
    if RandomDataGenerator.forType(arrayType.elementType, arrayType.containsNull).isDefined
  ) {
    test(s"$arrayType") {
      testRandomDataGeneration(arrayType)
    }
  }

  val atomicTypesWithDataGenerators =
    DataTypeTestUtils.atomicTypes.filter(RandomDataGenerator.forType(_).isDefined)

  // Complex types:
  for (
    keyType <- atomicTypesWithDataGenerators;
    valueType <- atomicTypesWithDataGenerators
    // Scala's BigDecimal.hashCode can lead to OutOfMemoryError on Scala 2.10 (see SI-6173) and
    // Spark can hit NumberFormatException errors when converting certain BigDecimals (SPARK-8802).
    // For these reasons, we don't support generation of maps with decimal keys.
    if !keyType.isInstanceOf[DecimalType]
  ) {
    val mapType = MapType(keyType, valueType)
    test(s"$mapType") {
      testRandomDataGeneration(mapType)
    }
  }

  for (
    colOneType <- atomicTypesWithDataGenerators;
    colTwoType <- atomicTypesWithDataGenerators
  ) {
    val structType = StructType(StructField("a", colOneType) :: StructField("b", colTwoType) :: Nil)
    test(s"$structType") {
      testRandomDataGeneration(structType)
    }
  }

  test("check size of generated map") {
    val mapType = MapType(IntegerType, IntegerType)
    for (seed <- 1 to 1000) {
      val generator = RandomDataGenerator.forType(
        mapType, nullable = false, rand = new Random(seed)).get
      val maps = Seq.fill(100)(generator().asInstanceOf[Map[Int, Int]])
      val expectedTotalElements = 100 / 2 * RandomDataGenerator.MAX_MAP_SIZE
      val deviation = math.abs(maps.map(_.size).sum - expectedTotalElements)
      assert(deviation.toDouble / expectedTotalElements < 2e-1)
    }
  }

  test("Use Float.NaN for all NaN values") {
    val bits = -6966608
    val nan1 = java.lang.Float.intBitsToFloat(bits)
    val nan2 = RandomDataGenerator.intBitsToFloat(bits)
    assert(nan1.isNaN)
    assert(nan2.isNaN)

    val arrayExpected = ByteBuffer.allocate(4).putFloat(Float.NaN).array
    val array1 = ByteBuffer.allocate(4).putFloat(nan1).array
    val array2 = ByteBuffer.allocate(4).putFloat(nan2).array
    assert(!Arrays.equals(array1, arrayExpected))
    assert(Arrays.equals(array2, arrayExpected))
  }

  test("Use Double.NaN for all NaN values") {
    val bits = -6966608
    val nan1 = java.lang.Double.longBitsToDouble(bits)
    val nan2 = RandomDataGenerator.longBitsToDouble(bits)
    assert(nan1.isNaN)
    assert(nan2.isNaN)

    val arrayExpected = ByteBuffer.allocate(8).putDouble(Double.NaN).array
    val array1 = ByteBuffer.allocate(8).putDouble(nan1).array
    val array2 = ByteBuffer.allocate(8).putDouble(nan2).array
    assert(!Arrays.equals(array1, arrayExpected))
    assert(Arrays.equals(array2, arrayExpected))
  }
}
