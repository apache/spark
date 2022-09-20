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
package org.apache.spark.sql.proto

import com.google.protobuf.{ByteString, Descriptors, DynamicMessage}
import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.proto.utils.ProtoUtils
import org.apache.spark.sql.test.SharedSparkSession

class ProtoFunctionsSuite extends QueryTest with SharedSparkSession with Serializable {

  import testImplicits._

  val testFileDesc = testFile("protobuf/proto_functions_suite.desc").replace("file:/", "/")

  test("roundtrip in to_proto and from_proto - struct") {
    val df = spark.range(10).select(
      struct(
        $"id",
        $"id".cast("string").as("string_value"),
        $"id".cast("int").as("int32_value"),
        $"id".cast("int").as("uint32_value"),
        $"id".cast("int").as("sint32_value"),
        $"id".cast("int").as("fixed32_value"),
        $"id".cast("int").as("sfixed32_value"),
        $"id".cast("long").as("int64_value"),
        $"id".cast("long").as("uint64_value"),
        $"id".cast("long").as("sint64_value"),
        $"id".cast("long").as("fixed64_value"),
        $"id".cast("long").as("sfixed64_value"),
        $"id".cast("double").as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")
      ).as("SimpleMessage")
    )
    val protoStructDF = df.select(functions.to_proto($"SimpleMessage", testFileDesc,
      "SimpleMessage").as("proto"))
    val actualDf = protoStructDF.select(functions.from_proto($"proto", testFileDesc,
      "SimpleMessage").as("proto.*"))
    checkAnswer(actualDf, df)
  }

  test("roundtrip in to_proto(without descriptor params) and from_proto - struct") {
    val df = spark.range(10).select(
      struct(
        $"id",
        $"id".cast("string").as("string_value"),
        $"id".cast("int").as("int32_value"),
        $"id".cast("long").as("int64_value"),
        $"id".cast("double").as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")
      ).as("SimpleMessageJavaTypes")
    )
    val protoStructDF = df.select(functions.to_proto($"SimpleMessageJavaTypes").as("proto"))
    val actualDf = protoStructDF.select(functions.from_proto($"proto", testFileDesc,
      "SimpleMessageJavaTypes").as("proto.*"))
    checkAnswer(actualDf, df)
  }

  test("roundtrip in from_proto and to_proto - Repeated") {
    val descriptor = ProtoUtils.buildDescriptor(testFileDesc, "SimpleMessageRepeated")

    val dynamicMessage = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("key"), "key")
      .setField(descriptor.findFieldByName("value"), "value")
      .addRepeatedField(descriptor.findFieldByName("rbool_value"), false)
      .addRepeatedField(descriptor.findFieldByName("rbool_value"), true)
      .addRepeatedField(descriptor.findFieldByName("rdouble_value"), 1092092.654D)
      .addRepeatedField(descriptor.findFieldByName("rdouble_value"), 1092093.654D)
      .addRepeatedField(descriptor.findFieldByName("rfloat_value"), 10903.0f)
      .addRepeatedField(descriptor.findFieldByName("rfloat_value"), 10902.0f)
      .addRepeatedField(descriptor.findFieldByName("rnested_enum"),
        descriptor.findEnumTypeByName("NestedEnum").findValueByName("ESTED_NOTHING"))
      .addRepeatedField(descriptor.findFieldByName("rnested_enum"),
        descriptor.findEnumTypeByName("NestedEnum").findValueByName("NESTED_FIRST"))
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "SimpleMessageRepeated").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", testFileDesc,
      "SimpleMessageRepeated").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "SimpleMessageRepeated").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Repeated Message Once") {
    val repeatedMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage = DynamicMessage.newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0D)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer"))
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", testFileDesc,
      "RepeatedMessage").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto(without descriptor params) - Repeated Message Once") {
    val repeatedMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage = DynamicMessage.newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0D)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer"))
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Repeated Message Twice") {
    val repeatedMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage1 = DynamicMessage.newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value1")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0D)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer1"))
      .build()
    val basicMessage2 = DynamicMessage.newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1112L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value2")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12346)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0D)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10903.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), false)
      .setField(basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer2"))
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage1)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage2)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", testFileDesc,
      "RepeatedMessage").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto(without descriptor params) - Repeated Message Twice") {
    val repeatedMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtoUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage1 = DynamicMessage.newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value1")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0D)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer1"))
      .build()
    val basicMessage2 = DynamicMessage.newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1112L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value2")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12346)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0D)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10903.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), false)
      .setField(basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer2"))
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage1)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage2)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Map") {
    val messageMapDesc = ProtoUtils.buildDescriptor(testFileDesc, "SimpleMessageMap")

    val mapStr1 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "key value1")
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value value2")
      .build()
    val mapStr2 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "key value2")
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value value2")
      .build()
    val mapInt64 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("Int64MapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("key"),
        0x90000000000L)
      .setField(messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("value"),
        0x90000000001L)
      .build()
    val mapInt32 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("Int32MapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("key"),
        12345)
      .setField(messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("value"),
        54321)
      .build()
    val mapFloat = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("FloatMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("key"),
        "float key")
      .setField(messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("value"),
        109202.234F)
      .build()
    val mapDouble = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("DoubleMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("key")
        , "double key")
      .setField(messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("value")
        , 109202.12D)
      .build()
    val mapBool = DynamicMessage.newBuilder(messageMapDesc.findNestedTypeByName("BoolMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("key"),
        true)
      .setField(messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("value"),
        false)
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(messageMapDesc)
      .setField(messageMapDesc.findFieldByName("key"), "key")
      .setField(messageMapDesc.findFieldByName("value"), "value")
      .addRepeatedField(messageMapDesc.findFieldByName("string_mapdata"), mapStr1)
      .addRepeatedField(messageMapDesc.findFieldByName("string_mapdata"), mapStr2)
      .addRepeatedField(messageMapDesc.findFieldByName("int64_mapdata"), mapInt64)
      .addRepeatedField(messageMapDesc.findFieldByName("int32_mapdata"), mapInt32)
      .addRepeatedField(messageMapDesc.findFieldByName("float_mapdata"), mapFloat)
      .addRepeatedField(messageMapDesc.findFieldByName("double_mapdata"), mapDouble)
      .addRepeatedField(messageMapDesc.findFieldByName("bool_mapdata"), mapBool)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "SimpleMessageMap").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", testFileDesc,
      "SimpleMessageMap").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "SimpleMessageMap").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto(without descriptor params) - Map") {
    val messageMapDesc = ProtoUtils.buildDescriptor(testFileDesc, "SimpleMessageMap")

    val mapStr1 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "key value1")
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value value2")
      .build()
    val mapStr2 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "key value2")
      .setField(messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value value2")
      .build()
    val mapInt64 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("Int64MapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("key"),
        0x90000000000L)
      .setField(messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("value"),
        0x90000000001L)
      .build()
    val mapInt32 = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("Int32MapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("key"),
        12345)
      .setField(messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("value"),
        54321)
      .build()
    val mapFloat = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("FloatMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("key"),
        "float key")
      .setField(messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("value"),
        109202.234F)
      .build()
    val mapDouble = DynamicMessage.newBuilder(
      messageMapDesc.findNestedTypeByName("DoubleMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("key")
        , "double key")
      .setField(messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("value")
        , 109202.12D)
      .build()
    val mapBool = DynamicMessage.newBuilder(messageMapDesc.findNestedTypeByName("BoolMapdataEntry"))
      .setField(messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("key"),
        true)
      .setField(messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("value"),
        false)
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(messageMapDesc)
      .setField(messageMapDesc.findFieldByName("key"), "key")
      .setField(messageMapDesc.findFieldByName("value"), "value")
      .addRepeatedField(messageMapDesc.findFieldByName("string_mapdata"), mapStr1)
      .addRepeatedField(messageMapDesc.findFieldByName("string_mapdata"), mapStr2)
      .addRepeatedField(messageMapDesc.findFieldByName("int64_mapdata"), mapInt64)
      .addRepeatedField(messageMapDesc.findFieldByName("int32_mapdata"), mapInt32)
      .addRepeatedField(messageMapDesc.findFieldByName("float_mapdata"), mapFloat)
      .addRepeatedField(messageMapDesc.findFieldByName("double_mapdata"), mapDouble)
      .addRepeatedField(messageMapDesc.findFieldByName("bool_mapdata"), mapBool)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "SimpleMessageMap").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "SimpleMessageMap").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Enum") {
    val messageEnumDesc = ProtoUtils.buildDescriptor(testFileDesc, "SimpleMessageEnum")
    val basicEnumDesc = ProtoUtils.buildDescriptor(testFileDesc, "BasicEnumMessage")

    val dynamicMessage = DynamicMessage.newBuilder(messageEnumDesc)
      .setField(messageEnumDesc.findFieldByName("key"), "key")
      .setField(messageEnumDesc.findFieldByName("value"), "value")
      .setField(messageEnumDesc.findFieldByName("nested_enum"),
        messageEnumDesc.findEnumTypeByName("NestedEnum").findValueByName("ESTED_NOTHING"))
      .setField(messageEnumDesc.findFieldByName("nested_enum"),
        messageEnumDesc.findEnumTypeByName("NestedEnum").findValueByName("NESTED_FIRST"))
      .setField(messageEnumDesc.findFieldByName("basic_enum"),
        basicEnumDesc.findEnumTypeByName("BasicEnum").findValueByName("FIRST"))
      .setField(messageEnumDesc.findFieldByName("basic_enum"),
        basicEnumDesc.findEnumTypeByName("BasicEnum").findValueByName("NOTHING"))
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "SimpleMessageEnum").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", testFileDesc,
      "SimpleMessageEnum").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "SimpleMessageEnum").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Multiple Message") {
    val messageMultiDesc = ProtoUtils.buildDescriptor(testFileDesc, "MultipleExample")
    val messageIncludeDesc = ProtoUtils.buildDescriptor(testFileDesc, "IncludedExample")
    val messageOtherDesc = ProtoUtils.buildDescriptor(testFileDesc, "OtherExample")

    val otherMessage = DynamicMessage.newBuilder(messageOtherDesc)
      .setField(messageOtherDesc.findFieldByName("other"), "other value")
      .build()

    val includeMessage = DynamicMessage.newBuilder(messageIncludeDesc)
      .setField(messageIncludeDesc.findFieldByName("included"), "included value")
      .setField(messageIncludeDesc.findFieldByName("other"), otherMessage)
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(messageMultiDesc)
      .setField(messageMultiDesc.findFieldByName("included_example"), includeMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "MultipleExample").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", testFileDesc,
      "MultipleExample").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "MultipleExample").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto(without descriptor params) - Multiple Message") {
    val messageMultiDesc = ProtoUtils.buildDescriptor(testFileDesc, "MultipleExample")
    val messageIncludeDesc = ProtoUtils.buildDescriptor(testFileDesc, "IncludedExample")
    val messageOtherDesc = ProtoUtils.buildDescriptor(testFileDesc, "OtherExample")

    val otherMessage = DynamicMessage.newBuilder(messageOtherDesc)
      .setField(messageOtherDesc.findFieldByName("other"), "other value")
      .build()

    val includeMessage = DynamicMessage.newBuilder(messageIncludeDesc)
      .setField(messageIncludeDesc.findFieldByName("included"), "included value")
      .setField(messageIncludeDesc.findFieldByName("other"), otherMessage)
      .build()

    val dynamicMessage = DynamicMessage.newBuilder(messageMultiDesc)
      .setField(messageMultiDesc.findFieldByName("included_example"), includeMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", testFileDesc,
      "MultipleExample").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", testFileDesc,
      "MultipleExample").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in to_proto and from_proto - with null") {
    val df = spark.range(10).select(
      struct(
        $"id",
        lit(null).cast("string").as("string_value"),
        $"id".cast("int").as("int32_value"),
        $"id".cast("int").as("uint32_value"),
        $"id".cast("int").as("sint32_value"),
        $"id".cast("int").as("fixed32_value"),
        $"id".cast("int").as("sfixed32_value"),
        $"id".cast("long").as("int64_value"),
        $"id".cast("long").as("uint64_value"),
        $"id".cast("long").as("sint64_value"),
        $"id".cast("long").as("fixed64_value"),
        $"id".cast("long").as("sfixed64_value"),
        $"id".cast("double").as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")
      ).as("SimpleMessage")
    )

    intercept[SparkException] {
      df.select(functions.to_proto($"SimpleMessage", testFileDesc,
        "SimpleMessage").as("proto")).collect()
    }
  }

  def buildDescriptor(desc: String): Descriptors.Descriptor = {
    val descriptor = ProtoUtils.parseFileDescriptor(testFileDesc).getMessageTypes().get(0)
    descriptor
  }
}
