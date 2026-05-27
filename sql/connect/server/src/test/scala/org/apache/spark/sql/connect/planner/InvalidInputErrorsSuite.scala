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
import org.apache.spark.sql.connect.planner.{InvalidInputErrors, SparkConnectPlanTest}
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
      expectedErrorCondition = "CONNECT_INVALID_PLAN.INVALID_SCHEMA_NON_STRUCT_TYPE",
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
      expectedErrorCondition = "CONNECT_INVALID_PLAN.DEDUPLICATE_NEEDS_INPUT",
      expectedParameters = Map.empty,
      invalidInput = {
        val deduplicate = proto.Deduplicate
          .newBuilder()
          .setAllColumnsAsKeys(true)
          .build()

        proto.Relation.newBuilder().setDeduplicate(deduplicate).build()
      }),
    TestCase(
      name = "Catalog not set",
      expectedErrorCondition = "CONNECT_INVALID_PLAN.INVALID_ONE_OF_FIELD_NOT_SET",
      expectedParameters =
        Map("fullName" -> "spark.connect.Catalog", "name" -> "CATTYPE_NOT_SET"),
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
      val exception = intercept[InvalidPlanInput] {
        transform(testCase.invalidInput)
      }
      checkError(
        exception = exception,
        condition = testCase.expectedErrorCondition,
        parameters = testCase.expectedParameters)
      if (testCase.expectedErrorCondition.startsWith("CONNECT_INVALID_PLAN")) {
        assert(exception.getSqlState == "56K00")
      }
    }
  }

  test("noHandlerFoundForExtension") {
    val ex = InvalidInputErrors.noHandlerFoundForExtension("foo.bar.Ext")
    assert(ex.getCondition.contains("NO_HANDLER_FOR_EXTENSION"))
    assert(ex.getMessage.contains("foo.bar.Ext"))
    assert(ex.getSqlState == "56K00")
  }

  test("notFoundCachedLocalRelation") {
    val ex = InvalidInputErrors.notFoundCachedLocalRelation("abc123", "sess-uuid")
    assert(ex.getCondition.contains("NOT_FOUND_CACHED_LOCAL_RELATION"))
    assert(ex.getMessage.contains("abc123"))
    assert(ex.getMessage.contains("sess-uuid"))
    assert(ex.getSqlState == "56K00")
  }

  test("localRelationSizeLimitExceeded") {
    val ex = InvalidInputErrors.localRelationSizeLimitExceeded(1000L, 500L)
    assert(ex.getCondition.contains("LOCAL_RELATION_SIZE_LIMIT_EXCEEDED"))
    assert(ex.getMessage.contains("1000"))
    assert(ex.getMessage.contains("500"))
    assert(ex.getSqlState == "56K00")
  }

  test("functionEvalTypeNotSupported") {
    val ex = InvalidInputErrors.functionEvalTypeNotSupported(42)
    assert(ex.getCondition.contains("FUNCTION_EVAL_TYPE_NOT_SUPPORTED"))
    assert(ex.getMessage.contains("42"))
    assert(ex.getSqlState == "56K00")
  }

  test("streamingQueryRunIdMismatch") {
    val ex = InvalidInputErrors.streamingQueryRunIdMismatch("q1", "run1", "run2")
    assert(ex.getCondition.contains("STREAMING_QUERY_RUN_ID_MISMATCH"))
    assert(ex.getMessage.contains("q1"))
    assert(ex.getMessage.contains("run1"))
    assert(ex.getMessage.contains("run2"))
    assert(ex.getSqlState == "56K00")
  }

  // Helper case class to define test cases
  case class TestCase(
      name: String,
      expectedErrorCondition: String,
      expectedParameters: Map[String, String],
      invalidInput: proto.Relation)
}
