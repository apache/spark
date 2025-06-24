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

package org.apache.spark.sql.connect.planner

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.types._

class InvalidInputErrorsSuite extends PlanTest with SparkConnectPlanTest {

  lazy val testLocalRelation =
    createLocalRelationProto(
      Seq(AttributeReference("id", IntegerType)(), AttributeReference("name", StringType)()),
      Seq.empty)

  val testCases = Seq(
    TestCase(
      name = "Invalid schema data type non struct for Parse",
      expectedErrorCondition = "INVALID_SCHEMA_TYPE_NON_STRUCT",
      expectedParameters = Map("dataType" -> "\"ARRAY<INT>\""),
      invalidInput = {
        val parse = proto.Parse
          .newBuilder()
          .setSchema(DataTypeProtoConverter.toConnectProtoType(ArrayType(IntegerType)))
          .setFormat(proto.Parse.ParseFormat.PARSE_FORMAT_CSV)
          .build()

        proto.Relation.newBuilder().setParse(parse).build()
      }),
    TestCase(
      name = "Invalid schema type non struct for TransformWithState",
      expectedErrorCondition = "INVALID_SCHEMA_TYPE_NON_STRUCT",
      expectedParameters = Map("dataType" -> "\"ARRAY<INT>\""),
      invalidInput = {
        val pythonUdf = proto.CommonInlineUserDefinedFunction
          .newBuilder()
          .setPythonUdf(
            proto.PythonUDF
              .newBuilder()
              .setEvalType(211)
              .setOutputType(DataTypeProtoConverter.toConnectProtoType(ArrayType(IntegerType)))
              .build())
          .build()

        val groupMap = proto.GroupMap
          .newBuilder()
          .setInput(testLocalRelation)
          .setFunc(pythonUdf)
          .setTransformWithStateInfo(
            proto.TransformWithStateInfo
              .newBuilder()
              .setOutputSchema(DataTypeProtoConverter.toConnectProtoType(ArrayType(IntegerType)))
              .build())
          .build()

        proto.Relation.newBuilder().setGroupMap(groupMap).build()
      }),
    TestCase(
      name = "Invalid schema string non struct type",
      expectedErrorCondition = "INVALID_SCHEMA.NON_STRUCT_TYPE",
      expectedParameters = Map(
        "inputSchema" -> """"{"type":"array","elementType":"integer","containsNull":false}"""",
        "dataType" -> "\"ARRAY<INT>\""),
      invalidInput = {
        val invalidSchema = """{"type":"array","elementType":"integer","containsNull":false}"""

        val dataSource = proto.Read.DataSource
          .newBuilder()
          .setFormat("csv")
          .setSchema(invalidSchema)
          .build()

        val read = proto.Read
          .newBuilder()
          .setDataSource(dataSource)
          .build()

        proto.Relation.newBuilder().setRead(read).build()
      }),
    TestCase(
      name = "Deduplicate needs input",
      expectedErrorCondition = "INTERNAL_ERROR",
      expectedParameters = Map("message" -> "Deduplicate needs a plan input"),
      invalidInput = {
        val deduplicate = proto.Deduplicate
          .newBuilder()
          .setAllColumnsAsKeys(true)
          .build()

        proto.Relation.newBuilder().setDeduplicate(deduplicate).build()
      }),
    TestCase(
      name = "Catalog not set",
      expectedErrorCondition = "INTERNAL_ERROR",
      expectedParameters =
        Map("message" -> "This oneOf field in spark.connect.Catalog is not set: CATTYPE_NOT_SET"),
      invalidInput = {
        val catalog = proto.Catalog
          .newBuilder()
          .build()

        proto.Relation
          .newBuilder()
          .setCatalog(catalog)
          .build()
      }))

  // Run all test cases
  testCases.foreach { testCase =>
    test(s"${testCase.name}") {
      checkError(
        exception = intercept[InvalidPlanInput] {
          transform(testCase.invalidInput)
        },
        condition = testCase.expectedErrorCondition,
        parameters = testCase.expectedParameters)
    }
  }

  // Helper case class to define test cases
  case class TestCase(
      name: String,
      expectedErrorCondition: String,
      expectedParameters: Map[String, String],
      invalidInput: proto.Relation)
}
