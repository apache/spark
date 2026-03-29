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
package org.apache.spark.sql.connect.messages

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.IntegerType

/**
 * Test suite for ExternalUDF integration into CommonInlineUserDefinedFunction.
 * SPARK-55278: Language-agnostic external UDF protocol.
 */
class ExternalUdfProtoSuite extends SparkFunSuite {

  test("ExternalUDF in CommonInlineUserDefinedFunction oneof") {
    val externalUdf = proto.ExternalUDF
      .newBuilder()
      .setLanguage("go")
      .setMode(proto.UdfMode.VECTORIZED)
      .setSerializedPayload(ByteString.copyFromUtf8("payload"))
      .setPayloadFormat("source")
      .setReturnType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .build()

    val commonInlineUdf = proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("test_func")
      .setExternalUdf(externalUdf)
      .build()

    assert(commonInlineUdf.hasExternalUdf)
    assert(!commonInlineUdf.hasPythonUdf)
    assert(!commonInlineUdf.hasScalarScalaUdf)
    assert(!commonInlineUdf.hasJavaUdf)
  }

  test("ExternalUDF with all fields") {
    val returnType = DataTypeProtoConverter.toConnectProtoType(IntegerType)
    val serializedPayload = ByteString.copyFromUtf8("function code here")
    val compiledArtifact = ByteString.copyFromUtf8("compiled binary data")

    val externalUdf = proto.ExternalUDF
      .newBuilder()
      .setLanguage("go")
      .setMode(proto.UdfMode.VECTORIZED)
      .setSerializedPayload(serializedPayload)
      .setPayloadFormat("source")
      .setReturnType(returnType)
      .putConfig("timeout_ms", "5000")
      .putConfig("max_memory", "1GB")
      .setEntryPoint("my_module:my_func")
      .setCompiledArtifact(compiledArtifact)
      .build()

    // Serialize and parse
    val serialized = externalUdf.toByteArray
    val parsed = proto.ExternalUDF.parseFrom(serialized)

    // Verify all fields
    assert(parsed.getLanguage == "go")
    assert(parsed.getMode == proto.UdfMode.VECTORIZED)
    assert(parsed.getSerializedPayload == serializedPayload)
    assert(parsed.getPayloadFormat == "source")
    assert(parsed.getReturnType == returnType)
    assert(parsed.getConfigCount == 2)
    assert(parsed.getConfigOrDefault("timeout_ms", "") == "5000")
    assert(parsed.getConfigOrDefault("max_memory", "") == "1GB")
    assert(parsed.getEntryPoint == "my_module:my_func")
    assert(parsed.getCompiledArtifact == compiledArtifact)
  }

  test("ExternalUDF serialization round-trip") {
    val argument = proto.Expression
      .newBuilder()
      .setUnresolvedAttribute(
        proto.Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("col1"))
      .build()

    val externalUdf = proto.ExternalUDF
      .newBuilder()
      .setLanguage("rust")
      .setMode(proto.UdfMode.SCALAR)
      .setSerializedPayload(ByteString.copyFromUtf8("fn add_one(x: i32) -> i32 { x + 1 }"))
      .setPayloadFormat("source")
      .setReturnType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .setEntryPoint("add_one")
      .build()

    val commonInlineUdfExpr = proto.Expression
      .newBuilder()
      .setCommonInlineUserDefinedFunction(
        proto.CommonInlineUserDefinedFunction
          .newBuilder()
          .setFunctionName("add_one_udf")
          .setDeterministic(true)
          .addArguments(argument)
          .setExternalUdf(externalUdf))
      .build()

    // Serialize and parse
    val serialized = commonInlineUdfExpr.toByteArray
    val parsed = proto.Expression.parseFrom(serialized)

    // Verify structure
    assert(parsed.hasCommonInlineUserDefinedFunction)
    val parsedUdf = parsed.getCommonInlineUserDefinedFunction
    assert(parsedUdf.getFunctionName == "add_one_udf")
    assert(parsedUdf.getDeterministic)
    assert(parsedUdf.getArgumentsCount == 1)
    assert(parsedUdf.hasExternalUdf)

    val parsedExternalUdf = parsedUdf.getExternalUdf
    assert(parsedExternalUdf.getLanguage == "rust")
    assert(parsedExternalUdf.getMode == proto.UdfMode.SCALAR)
    assert(parsedExternalUdf.getPayloadFormat == "source")
    assert(parsedExternalUdf.getEntryPoint == "add_one")
  }

  test("ExternalUDF mode values") {
    val modes = Seq(
      proto.UdfMode.SCALAR,
      proto.UdfMode.VECTORIZED,
      proto.UdfMode.GROUPED_MAP,
      proto.UdfMode.TABLE
    )

    modes.foreach { mode =>
      val externalUdf = proto.ExternalUDF
        .newBuilder()
        .setLanguage("python")
        .setMode(mode)
        .setSerializedPayload(ByteString.copyFromUtf8("def func(): pass"))
        .setPayloadFormat("source")
        .setReturnType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
        .build()

      // Round-trip
      val serialized = externalUdf.toByteArray
      val parsed = proto.ExternalUDF.parseFrom(serialized)

      assert(parsed.getMode == mode, s"Mode mismatch for $mode")
    }
  }

  test("ExternalUDF with Python language") {
    val serializedPayload = ByteString.copyFromUtf8("cloudpickled function bytes")

    val externalUdf = proto.ExternalUDF
      .newBuilder()
      .setLanguage("python")
      .setMode(proto.UdfMode.VECTORIZED)
      .setSerializedPayload(serializedPayload)
      .setPayloadFormat("cloudpickle")
      .setReturnType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .build()

    assert(externalUdf.getLanguage == "python")
    assert(externalUdf.getPayloadFormat == "cloudpickle")
    assert(externalUdf.getSerializedPayload == serializedPayload)
  }

  test("ExternalUDF vs PythonUDF mutual exclusion") {
    // Build with PythonUDF
    val pythonUdf = proto.PythonUDF
      .newBuilder()
      .setEvalType(100)
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .setCommand(ByteString.copyFrom("python command".getBytes()))
      .setPythonVer("3.10")
      .build()

    val withPythonUdf = proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("python_func")
      .setPythonUdf(pythonUdf)
      .build()

    assert(withPythonUdf.hasPythonUdf)
    assert(!withPythonUdf.hasExternalUdf)

    // Build with ExternalUDF
    val externalUdf = proto.ExternalUDF
      .newBuilder()
      .setLanguage("go")
      .setMode(proto.UdfMode.SCALAR)
      .setSerializedPayload(ByteString.copyFromUtf8("go code"))
      .setPayloadFormat("source")
      .setReturnType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .build()

    val withExternalUdf = proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("go_func")
      .setExternalUdf(externalUdf)
      .build()

    assert(withExternalUdf.hasExternalUdf)
    assert(!withExternalUdf.hasPythonUdf)

    // Verify that setting one clears the other
    val builder = proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("test")
      .setPythonUdf(pythonUdf)

    assert(builder.hasPythonUdf)
    assert(!builder.hasExternalUdf)

    builder.setExternalUdf(externalUdf)
    assert(builder.hasExternalUdf)
    assert(!builder.hasPythonUdf)
  }

  test("ExternalUDF with empty optional fields") {
    // Build with only required fields
    val externalUdf = proto.ExternalUDF
      .newBuilder()
      .setLanguage("swift")
      .setMode(proto.UdfMode.SCALAR)
      .setSerializedPayload(ByteString.copyFromUtf8("swift code"))
      .setPayloadFormat("source")
      .setReturnType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .build()

    // Verify optional fields have default values
    assert(externalUdf.getConfigCount == 0)
    assert(externalUdf.getEntryPoint == "")
    assert(externalUdf.getCompiledArtifact == ByteString.EMPTY)

    // Round-trip to ensure defaults persist
    val serialized = externalUdf.toByteArray
    val parsed = proto.ExternalUDF.parseFrom(serialized)

    assert(parsed.getConfigCount == 0)
    assert(parsed.getEntryPoint == "")
    assert(parsed.getCompiledArtifact == ByteString.EMPTY)
  }
}
