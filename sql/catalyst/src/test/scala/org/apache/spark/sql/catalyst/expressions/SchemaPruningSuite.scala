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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class SchemaPruningSuite extends SparkFunSuite {
  test("prune schema by the requested fields") {
    def testPrunedSchema(
        schema: StructType,
        requestedFields: StructField*): Unit = {
      val requestedRootFields = requestedFields.map { f =>
        // `derivedFromAtt` doesn't affect the result of pruned schema.
        SchemaPruning.RootField(field = f, derivedFromAtt = true)
      }
      val expectedSchema = SchemaPruning.pruneDataSchema(schema, requestedRootFields)
      assert(expectedSchema == StructType(requestedFields))
    }

    testPrunedSchema(StructType.fromDDL("a int, b int"), StructField("a", IntegerType))
    testPrunedSchema(StructType.fromDDL("a int, b int"), StructField("b", IntegerType))

    val structOfStruct = StructType.fromDDL("a struct<a:int, b:int>, b int")
    testPrunedSchema(structOfStruct, StructField("a", StructType.fromDDL("a int, b int")))
    testPrunedSchema(structOfStruct, StructField("b", IntegerType))
    testPrunedSchema(structOfStruct, StructField("a", StructType.fromDDL("b int")))

    val arrayOfStruct = StructField("a", ArrayType(StructType.fromDDL("a int, b int, c string")))
    val mapOfStruct = StructField("d", MapType(StructType.fromDDL("a int, b int, c string"),
      StructType.fromDDL("d int, e int, f string")))

    val complexStruct = StructType(
      arrayOfStruct :: StructField("b", structOfStruct) :: StructField("c", IntegerType) ::
        mapOfStruct :: Nil)

    testPrunedSchema(complexStruct, StructField("a", ArrayType(StructType.fromDDL("b int"))),
      StructField("b", StructType.fromDDL("a int")))
    testPrunedSchema(complexStruct,
      StructField("a", ArrayType(StructType.fromDDL("b int, c string"))),
      StructField("b", StructType.fromDDL("b int")))

    val selectFieldInMap = StructField("d", MapType(StructType.fromDDL("a int, b int"),
      StructType.fromDDL("e int, f string")))
    testPrunedSchema(complexStruct, StructField("c", IntegerType), selectFieldInMap)
  }
}
