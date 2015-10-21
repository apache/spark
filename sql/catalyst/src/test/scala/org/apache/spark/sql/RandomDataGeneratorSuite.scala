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
    val generator = RandomDataGenerator.forType(dataType, nullable, Some(33)).getOrElse {
      fail(s"Random data generator was not defined for $dataType")
    }
    if (nullable) {
      assert(Iterator.fill(100)(generator()).contains(null))
    } else {
      assert(Iterator.fill(100)(generator()).forall(_ != null))
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

}
