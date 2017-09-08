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

package org.apache.spark.sql.catalyst.json

import java.io.CharArrayWriter
import java.util.TimeZone

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._

class JacksonUtilsSuite extends SparkFunSuite {

  test("verifySchema") {
    def verfifyJSONGenerate(schema: StructType): Unit = {
      val convertToInternalRow = CatalystTypeConverters.createToCatalystConverter(schema)
      val maybeDataGen = RandomDataGenerator.forType(schema, nullable = false)
      val dataGen = maybeDataGen.getOrElse(
        fail(s"Failed to create data generator for type $schema"))
      val row = convertToInternalRow(dataGen.apply()).asInstanceOf[InternalRow]
      val gen = new JacksonGenerator(
        schema, new CharArrayWriter(), new JSONOptions(Map.empty, TimeZone.getDefault.getID))
      gen.write(row)
    }

    // The valid schema
    val atomicTypes = DataTypeTestUtils.atomicTypes
    val atomicArrayTypes = atomicTypes.map(ArrayType(_, containsNull = false))
    val atomicMapTypes = for (keyType <- atomicTypes;
      valueType <- atomicTypes) yield MapType(keyType, valueType, false)

    (atomicTypes ++ atomicArrayTypes ++ atomicMapTypes).foreach { dataType =>
      val schema = StructType(StructField("a", dataType, nullable = false) :: Nil)
      JacksonUtils.verifySchema(schema)
      verfifyJSONGenerate(schema)
    }

    val invalidTypes = Seq(CalendarIntervalType)

    // For MapType, its keys are treated as a string basically when generating JSON, so we only
    // care if the values are valid for JSON.
    val alsoValidMapTypes = for (keyType <- atomicTypes ++ invalidTypes;
      valueType <- atomicTypes) yield MapType(keyType, valueType, true)
    alsoValidMapTypes.foreach { dataType =>
      val schema = StructType(StructField("a", dataType, nullable = false) :: Nil)
      JacksonUtils.verifySchema(schema)
      verfifyJSONGenerate(schema)
    }

    // The invalid schema
    val invalidArrayTypes = invalidTypes.map(ArrayType(_, containsNull = false))
    val invalidMapTypes = for (keyType <- atomicTypes ++ invalidTypes;
      valueType <- invalidTypes) yield MapType(keyType, valueType, false)

    (invalidTypes ++ invalidArrayTypes ++ invalidMapTypes).foreach { dataType =>
      val schema = StructType(StructField("a", dataType, nullable = false) :: Nil)
      intercept[UnsupportedOperationException] {
        JacksonUtils.verifySchema(schema)
      }
      intercept[RuntimeException] {
        verfifyJSONGenerate(schema)
      }
    }
  }
}
