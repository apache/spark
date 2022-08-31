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

import com.google.protobuf.ByteString
import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.proto.SimpleMessageRepeatedProtos.SimpleMessageRepeated
import org.apache.spark.sql.proto.RepeatedMessageProtos.{BasicMessage, RepeatedMessage}
import org.apache.spark.sql.proto.SimpleMessageEnumProtos.{BasicEnum, SimpleMessageEnum}
import org.apache.spark.sql.proto.SimpleMessageMapProtos.SimpleMessageMap
import org.apache.spark.sql.proto.MessageMultipleMessage.{IncludedExample, MultipleExample, OtherExample}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.proto.SimpleMessageEnumProtos.SimpleMessageEnum.NestedEnum

import java.util

class ProtoFunctionsSuite extends QueryTest with SharedSparkSession with Serializable {
  import testImplicits._

  test("roundtrip in to_proto and from_proto - int and string") {
    val SIMPLE_MESSAGE = "protobuf/message_with_repeated.desc"
    val simpleMessagePath = testFile(SIMPLE_MESSAGE).replace("file:/", "/")

    val simpleMessage = SimpleMessageRepeated.newBuilder()
      .setKey("key")
      .setValue("value")
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092)
      .addRdoubleValue(1092093)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .addRnestedEnum(SimpleMessageRepeated.NestedEnum.ESTED_NOTHING)
      .addRnestedEnum(SimpleMessageRepeated.NestedEnum.NESTED_FIRST)
      .build()

    val df = Seq(simpleMessage.toByteArray).toDF("value")

    val dfRes = df.select(functions.from_proto($"value", simpleMessagePath, "SimpleMessageRepeated").as("value"))
    dfRes.select($"value.*").show()
    dfRes.printSchema()

    val dfRes2 = dfRes.select(functions.to_proto($"value", simpleMessagePath, "SimpleMessageRepeated").as("value2"))
    dfRes2.show()

    val dfRes3 = dfRes2.select(functions.from_proto($"value2", simpleMessagePath, "SimpleMessageRepeated").as("value3"))
    dfRes3.select($"value3.*").show()
    dfRes3.printSchema()
  }

  test("roundtrip in to_proto and from_proto - struct") {
    val messagePath = testFile("protobuf/simple_message.desc").replace("file:/", "/")
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
    val protoStructDF = df.select(functions.to_proto($"SimpleMessage", messagePath, "SimpleMessage").as("proto"))
    val actualDf = protoStructDF.select(functions.from_proto($"proto", messagePath, "SimpleMessage").as("proto.*"))
    checkAnswer(actualDf, df)
  }

  test("roundtrip in from_proto and to_proto - Repeated") {
    val messagePath = testFile("protobuf/message_with_repeated.desc").replace("file:/", "/")
    val repeatedMessage = SimpleMessageRepeated.newBuilder()
      .setKey("key")
      .setValue("value")
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092)
      .addRdoubleValue(1092093)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .addRnestedEnum(SimpleMessageRepeated.NestedEnum.ESTED_NOTHING)
      .addRnestedEnum(SimpleMessageRepeated.NestedEnum.NESTED_FIRST)
      .build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "SimpleMessageRepeated").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "SimpleMessageRepeated").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "SimpleMessageRepeated").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Repeated Message Once") {
    val messagePath = testFile("protobuf/repeated_message.desc").replace("file:/", "/")
    val basicMessage = BasicMessage.newBuilder()
      .setId(1111L)
      .setStringValue("value")
      .setInt32Value(12345)
      .setInt64Value(0x90000000000L)
      .setDoubleValue(10000000000.0D)
      .setBoolValue(true)
      .setBytesValue(ByteString.copyFromUtf8("ProtobufDeserializer"))
      .build()
    val repeatedMessage = RepeatedMessage.newBuilder()
      .addBasicMessage(basicMessage)
      .build()

    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "RepeatedMessage").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Repeated Message Twice") {
    val messagePath = testFile("protobuf/repeated_message.desc").replace("file:/", "/")
    val baseArray = new util.ArrayList[BasicMessage]();
    val basicMessage1 = BasicMessage.newBuilder()
      .setId(1111L)
      .setStringValue("value1")
      .setInt32Value(12345)
      .setInt64Value(0x20000000000L)
      .setDoubleValue(10000000000.0D)
      .setBoolValue(true)
      .setBytesValue(ByteString.copyFromUtf8("ProtobufDeserializer1"))
      .build()
    val basicMessage2 = BasicMessage.newBuilder()
      .setId(2222L)
      .setStringValue("value2")
      .setInt32Value(54321)
      .setInt64Value(0x20000000000L)
      .setDoubleValue(20000000000.0D)
      .setBoolValue(false)
      .setBytesValue(ByteString.copyFromUtf8("ProtobufDeserializer2"))
      .build()
    baseArray.add(basicMessage1)
    baseArray.add(basicMessage2)
    val repeatedMessage = RepeatedMessage.newBuilder()
      .addAllBasicMessage(baseArray)
      .build()

    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "RepeatedMessage").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Map") {
    val messagePath = testFile("protobuf/message_with_map.desc").replace("file:/", "/")
    val repeatedMessage = SimpleMessageMap.newBuilder()
      .setKey("key")
      .setValue("value")
      .putBoolMapdata(true, true)
      .putDoubleMapdata("double", 20930930)
      .putInt64Mapdata(109209092, 920920920)
      .putStringMapdata("key", "value")
      .putStringMapdata("key1", "value1")
      .putInt32Mapdata(1092, 902)
      .putFloatMapdata("float", 10920.0f)
      .build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "SimpleMessageMap").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "SimpleMessageMap").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "SimpleMessageMap").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Enum") {
    val messagePath = testFile("protobuf/message_with_enum.desc").replace("file:/", "/")
    val repeatedMessage = SimpleMessageEnum.newBuilder()
      .setKey("key")
      .setValue("value")
      .setBasicEnum(BasicEnum.FIRST)
      .setBasicEnum(BasicEnum.NOTHING)
      .setNestedEnum(NestedEnum.NESTED_FIRST)
      .setNestedEnum(NestedEnum.NESTED_SECOND)
      .build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "SimpleMessageEnum").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "SimpleMessageEnum").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "SimpleMessageEnum").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Multiple Message") {
    val messagePath = testFile("protobuf/message_multiple_message.desc").replace("file:/", "/")
    val repeatedMessage = MultipleExample.newBuilder()
      .setIncludedExample(
        IncludedExample.newBuilder().setIncluded("included_value").setOther(
          OtherExample.newBuilder().setOther("other_value"))
      ).build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "MultipleExample").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "MultipleExample").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "MultipleExample").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("test Proto Schema") {
    val MULTIPLE_EXAMPLE = "protobuf/protobuf_multiple_message.desc"
    val desc = ProtoUtils.buildDescriptor(testFile(MULTIPLE_EXAMPLE).replace("file:/", "/"), "MultipleExample")
  }

  test("handle invalid input in from_proto") {

  }

  test("roundtrip in to_proto and from_proto - with null") {
    val messagePath = testFile("protobuf/simple_message.desc").replace("file:/", "/")
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
      df.select(functions.to_proto($"SimpleMessage", messagePath, "SimpleMessage").as("proto")).collect()
    }
  }

  test("roundtrip in to_proto and from_proto - struct with nullable Proto schema") {

  }
  test("to_proto with unsupported nullable Proto schema") {

  }
}