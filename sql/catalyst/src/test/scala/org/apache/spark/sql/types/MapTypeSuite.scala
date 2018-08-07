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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite

class MapTypeSuite extends SparkFunSuite {
  test("SPARK-25031: MapType should produce current formatted string for complex types") {

    val keyType: DataType = StructType(Seq(
      StructField("a", DataTypes.IntegerType),
      StructField("b", DataTypes.IntegerType)))

    val valueType: DataType = StructType(Seq(
      StructField("c", DataTypes.IntegerType),
      StructField("d", DataTypes.IntegerType)))

    val builder = new StringBuilder

    MapType(keyType, valueType).buildFormattedString(prefix = "", builder = builder)

    val result = builder.toString()
    val expected =
      """-- key: struct
        |    |-- a: integer (nullable = true)
        |    |-- b: integer (nullable = true)
        |-- value: struct (valueContainsNull = true)
        |    |-- c: integer (nullable = true)
        |    |-- d: integer (nullable = true)
        |""".stripMargin

    assert(result === expected)
  }
}
