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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf.CASE_SENSITIVE
import org.apache.spark.sql.types._

class SchemaPruningSuite extends SparkFunSuite with SQLHelper {
  private def testPrunedSchema(
      schema: StructType,
      requestedFields: Seq[StructField],
      expectedSchema: StructType): Unit = {
    val requestedRootFields = requestedFields.map { f =>
      // `derivedFromAtt` doesn't affect the result of pruned schema.
      SchemaPruning.RootField(field = f, derivedFromAtt = true)
    }
    val prunedSchema = SchemaPruning.pruneDataSchema(schema, requestedRootFields)
    assert(prunedSchema === expectedSchema)
  }

  test("prune schema by the requested fields") {
    testPrunedSchema(
      StructType.fromDDL("a int, b int"),
      Seq(StructField("a", IntegerType)),
      StructType.fromDDL("a int, b int"))

    val structOfStruct = StructType.fromDDL("a struct<a:int, b:int>, b int")
    testPrunedSchema(structOfStruct,
      Seq(StructField("a", StructType.fromDDL("a int")), StructField("b", IntegerType)),
      StructType.fromDDL("a struct<a:int>, b int"))
    testPrunedSchema(structOfStruct,
      Seq(StructField("a", StructType.fromDDL("a int"))),
      StructType.fromDDL("a struct<a:int>, b int"))

    val arrayOfStruct = StructField("a", ArrayType(StructType.fromDDL("a int, b int, c string")))
    val mapOfStruct = StructField("d", MapType(StructType.fromDDL("a int, b int, c string"),
      StructType.fromDDL("d int, e int, f string")))

    val complexStruct = StructType(
      arrayOfStruct :: StructField("b", structOfStruct) :: StructField("c", IntegerType) ::
        mapOfStruct :: Nil)

    testPrunedSchema(complexStruct,
      Seq(StructField("a", ArrayType(StructType.fromDDL("b int"))),
        StructField("b", StructType.fromDDL("a int"))),
      StructType(
        StructField("a", ArrayType(StructType.fromDDL("b int"))) ::
          StructField("b", StructType.fromDDL("a int")) ::
          StructField("c", IntegerType) ::
          mapOfStruct :: Nil))
    testPrunedSchema(complexStruct,
      Seq(StructField("a", ArrayType(StructType.fromDDL("b int, c string"))),
        StructField("b", StructType.fromDDL("b int"))),
      StructType(
        StructField("a", ArrayType(StructType.fromDDL("b int, c string"))) ::
          StructField("b", StructType.fromDDL("b int")) ::
          StructField("c", IntegerType) ::
          mapOfStruct :: Nil))

    val selectFieldInMap = StructField("d", MapType(StructType.fromDDL("a int, b int"),
      StructType.fromDDL("e int, f string")))
    testPrunedSchema(complexStruct,
      Seq(StructField("c", IntegerType), selectFieldInMap),
      StructType(
        arrayOfStruct ::
          StructField("b", structOfStruct) ::
          StructField("c", IntegerType) ::
          selectFieldInMap :: Nil))
  }

  test("SPARK-35096: test case insensitivity of pruned schema") {
    val upperCaseSchema = StructType.fromDDL("A struct<A:int, B:int>, B int")
    val lowerCaseSchema = StructType.fromDDL("a struct<a:int, b:int>, b int")
    val upperCaseRequestedFields = Seq(StructField("A", StructType.fromDDL("A int")))
    val lowerCaseRequestedFields = Seq(StructField("a", StructType.fromDDL("a int")))

    Seq(true, false).foreach { isCaseSensitive =>
      withSQLConf(CASE_SENSITIVE.key -> isCaseSensitive.toString) {
        if (isCaseSensitive) {
          testPrunedSchema(
            upperCaseSchema,
            upperCaseRequestedFields,
            StructType.fromDDL("A struct<A:int>, B int"))
          testPrunedSchema(
            upperCaseSchema,
            lowerCaseRequestedFields,
            upperCaseSchema)

          testPrunedSchema(
            lowerCaseSchema,
            upperCaseRequestedFields,
            lowerCaseSchema)
          testPrunedSchema(
            lowerCaseSchema,
            lowerCaseRequestedFields,
            StructType.fromDDL("a struct<a:int>, b int"))
        } else {
          Seq(upperCaseRequestedFields, lowerCaseRequestedFields).foreach { requestedFields =>
            testPrunedSchema(
              upperCaseSchema,
              requestedFields,
              StructType.fromDDL("A struct<A:int>, B int"))
          }

          Seq(upperCaseRequestedFields, lowerCaseRequestedFields).foreach { requestedFields =>
            testPrunedSchema(
              lowerCaseSchema,
              requestedFields,
              StructType.fromDDL("a struct<a:int>, b int"))
          }
        }
      }
    }
  }
}
