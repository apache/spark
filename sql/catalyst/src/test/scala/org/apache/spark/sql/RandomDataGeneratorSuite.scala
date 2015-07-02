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
    RandomDataGenerator.forType(dataType, nullable, Some(42L)).foreach { generator =>
      for (_ <- 1 to 10) {
        val generatedValue = generator()
        val convertedValue = toCatalyst(generatedValue)
        if (!nullable) {
          assert(convertedValue !== null)
        }
      }
    }

  }

  // Basic types:

  (DataTypeTestUtils.atomicTypes ++ DataTypeTestUtils.atomicArrayTypes).foreach { dataType =>
    test(s"$dataType") {
      testRandomDataGeneration(dataType)
    }
  }

  // Complex types:

  for (
    keyType <- DataTypeTestUtils.atomicTypes;
    valueType <- DataTypeTestUtils.atomicTypes
  ) {
    val mapType = MapType(keyType, valueType)
    test(s"$mapType") {
      testRandomDataGeneration(mapType)
    }
  }

  for (
    colOneType <- DataTypeTestUtils.atomicTypes;
    colTwoType <- DataTypeTestUtils.atomicTypes
  ) {
    val structType = StructType(StructField("a", colOneType) :: StructField("b", colTwoType) :: Nil)
    test(s"$structType") {
      testRandomDataGeneration(structType)
    }
  }

}
