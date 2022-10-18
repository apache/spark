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
package org.apache.spark.sql.protobuf

import java.sql.Timestamp
import java.time.Duration

import scala.collection.JavaConverters._

import com.google.protobuf.{ByteString, DynamicMessage}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.utils.SchemaConverters.IncompatibleSchemaException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, StringType, StructField, StructType, TimestampType}

class ProtobufFunctionsSuite extends QueryTest with SharedSparkSession with Serializable {

  import testImplicits._

  val testFileDesc = testFile("protobuf/functions_suite.desc").replace("file:/", "/")

  test("roundtrip in to_protobuf and from_protobuf - struct") {
    val df = spark
      .range(1, 10)
      .select(struct(
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
        lit("0".getBytes).as("bytes_value")).as("SimpleMessage"))
    val protoStructDF = df.select(
      functions.to_protobuf($"SimpleMessage", testFileDesc, "SimpleMessage").as("proto"))
    val actualDf = protoStructDF.select(
      functions.from_protobuf($"proto", testFileDesc, "SimpleMessage").as("proto.*"))
    checkAnswer(actualDf, df)
  }

  test("roundtrip in from_protobuf and to_protobuf - Repeated") {
    val descriptor = ProtobufUtils.buildDescriptor(testFileDesc, "SimpleMessageRepeated")

    val dynamicMessage = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("key"), "key")
      .setField(descriptor.findFieldByName("value"), "value")
      .addRepeatedField(descriptor.findFieldByName("rbool_value"), false)
      .addRepeatedField(descriptor.findFieldByName("rbool_value"), true)
      .addRepeatedField(descriptor.findFieldByName("rdouble_value"), 1092092.654d)
      .addRepeatedField(descriptor.findFieldByName("rdouble_value"), 1092093.654d)
      .addRepeatedField(descriptor.findFieldByName("rfloat_value"), 10903.0f)
      .addRepeatedField(descriptor.findFieldByName("rfloat_value"), 10902.0f)
      .addRepeatedField(
        descriptor.findFieldByName("rnested_enum"),
        descriptor.findEnumTypeByName("NestedEnum").findValueByName("ESTED_NOTHING"))
      .addRepeatedField(
        descriptor.findFieldByName("rnested_enum"),
        descriptor.findEnumTypeByName("NestedEnum").findValueByName("NESTED_FIRST"))
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(
      functions.from_protobuf($"value", testFileDesc, "SimpleMessageRepeated").as("value_from"))
    val toProtoDF = fromProtoDF.select(
      functions.to_protobuf($"value_from", testFileDesc, "SimpleMessageRepeated").as("value_to"))
    val toFromProtoDF = toProtoDF.select(
      functions
        .from_protobuf($"value_to", testFileDesc, "SimpleMessageRepeated")
        .as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_protobuf and to_protobuf - Repeated Message Once") {
    val repeatedMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer"))
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(
      functions.from_protobuf($"value", testFileDesc, "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(
      functions.to_protobuf($"value_from", testFileDesc, "RepeatedMessage").as("value_to"))
    val toFromProtoDF = toProtoDF.select(
      functions.from_protobuf($"value_to", testFileDesc, "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_protobuf and to_protobuf - Repeated Message Twice") {
    val repeatedMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage1 = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value1")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer1"))
      .build()
    val basicMessage2 = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1112L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value2")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12346)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10903.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), false)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer2"))
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage1)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage2)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(
      functions.from_protobuf($"value", testFileDesc, "RepeatedMessage").as("value_from"))
    val toProtoDF = fromProtoDF.select(
      functions.to_protobuf($"value_from", testFileDesc, "RepeatedMessage").as("value_to"))
    val toFromProtoDF = toProtoDF.select(
      functions.from_protobuf($"value_to", testFileDesc, "RepeatedMessage").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_protobuf and to_protobuf - Map") {
    val messageMapDesc = ProtobufUtils.buildDescriptor(testFileDesc, "SimpleMessageMap")

    val mapStr1 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "string_key")
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value1")
      .build()
    val mapStr2 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "string_key")
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value2")
      .build()
    val mapInt64 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("Int64MapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("key"),
        0x90000000000L)
      .setField(
        messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("value"),
        0x90000000001L)
      .build()
    val mapInt32 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("Int32MapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("key"),
        12345)
      .setField(
        messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("value"),
        54321)
      .build()
    val mapFloat = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("FloatMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("key"),
        "float_key")
      .setField(
        messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("value"),
        109202.234f)
      .build()
    val mapDouble = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("DoubleMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("key"),
        "double_key")
      .setField(
        messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("value"),
        109202.12d)
      .build()
    val mapBool = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("BoolMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("key"),
        true)
      .setField(
        messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("value"),
        false)
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(messageMapDesc)
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
    val fromProtoDF = df.select(
      functions.from_protobuf($"value", testFileDesc, "SimpleMessageMap").as("value_from"))
    val toProtoDF = fromProtoDF.select(
      functions.to_protobuf($"value_from", testFileDesc, "SimpleMessageMap").as("value_to"))
    val toFromProtoDF = toProtoDF.select(
      functions.from_protobuf($"value_to", testFileDesc, "SimpleMessageMap").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_protobuf and to_protobuf - Enum") {
    val messageEnumDesc = ProtobufUtils.buildDescriptor(testFileDesc, "SimpleMessageEnum")
    val basicEnumDesc = ProtobufUtils.buildDescriptor(testFileDesc, "BasicEnumMessage")

    val dynamicMessage = DynamicMessage
      .newBuilder(messageEnumDesc)
      .setField(messageEnumDesc.findFieldByName("key"), "key")
      .setField(messageEnumDesc.findFieldByName("value"), "value")
      .setField(
        messageEnumDesc.findFieldByName("nested_enum"),
        messageEnumDesc.findEnumTypeByName("NestedEnum").findValueByName("ESTED_NOTHING"))
      .setField(
        messageEnumDesc.findFieldByName("nested_enum"),
        messageEnumDesc.findEnumTypeByName("NestedEnum").findValueByName("NESTED_FIRST"))
      .setField(
        messageEnumDesc.findFieldByName("basic_enum"),
        basicEnumDesc.findEnumTypeByName("BasicEnum").findValueByName("FIRST"))
      .setField(
        messageEnumDesc.findFieldByName("basic_enum"),
        basicEnumDesc.findEnumTypeByName("BasicEnum").findValueByName("NOTHING"))
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(
      functions.from_protobuf($"value", testFileDesc, "SimpleMessageEnum").as("value_from"))
    val toProtoDF = fromProtoDF.select(
      functions.to_protobuf($"value_from", testFileDesc, "SimpleMessageEnum").as("value_to"))
    val toFromProtoDF = toProtoDF.select(
      functions.from_protobuf($"value_to", testFileDesc, "SimpleMessageEnum").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_protobuf and to_protobuf - Multiple Message") {
    val messageMultiDesc = ProtobufUtils.buildDescriptor(testFileDesc, "MultipleExample")
    val messageIncludeDesc = ProtobufUtils.buildDescriptor(testFileDesc, "IncludedExample")
    val messageOtherDesc = ProtobufUtils.buildDescriptor(testFileDesc, "OtherExample")

    val otherMessage = DynamicMessage
      .newBuilder(messageOtherDesc)
      .setField(messageOtherDesc.findFieldByName("other"), "other value")
      .build()

    val includeMessage = DynamicMessage
      .newBuilder(messageIncludeDesc)
      .setField(messageIncludeDesc.findFieldByName("included"), "included value")
      .setField(messageIncludeDesc.findFieldByName("other"), otherMessage)
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(messageMultiDesc)
      .setField(messageMultiDesc.findFieldByName("included_example"), includeMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(
      functions.from_protobuf($"value", testFileDesc, "MultipleExample").as("value_from"))
    val toProtoDF = fromProtoDF.select(
      functions.to_protobuf($"value_from", testFileDesc, "MultipleExample").as("value_to"))
    val toFromProtoDF = toProtoDF.select(
      functions.from_protobuf($"value_to", testFileDesc, "MultipleExample").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("Handle recursive fields in Protobuf schema, A->B->A") {
    val schemaA = ProtobufUtils.buildDescriptor(testFileDesc, "recursiveA")
    val schemaB = ProtobufUtils.buildDescriptor(testFileDesc, "recursiveB")

    val messageBForA = DynamicMessage
      .newBuilder(schemaB)
      .setField(schemaB.findFieldByName("keyB"), "key")
      .build()

    val messageA = DynamicMessage
      .newBuilder(schemaA)
      .setField(schemaA.findFieldByName("keyA"), "key")
      .setField(schemaA.findFieldByName("messageB"), messageBForA)
      .build()

    val messageB = DynamicMessage
      .newBuilder(schemaB)
      .setField(schemaB.findFieldByName("keyB"), "key")
      .setField(schemaB.findFieldByName("messageA"), messageA)
      .build()

    val df = Seq(messageB.toByteArray).toDF("messageB")

    val e = intercept[IncompatibleSchemaException] {
      df.select(
        functions.from_protobuf($"messageB", testFileDesc, "recursiveB").as("messageFromProto"))
        .show()
    }
    val expectedMessage = s"""
         |Found recursive reference in Protobuf schema, which can not be processed by Spark:
         |org.apache.spark.sql.protobuf.recursiveB.messageA""".stripMargin
    assert(e.getMessage == expectedMessage)
  }

  test("Handle recursive fields in Protobuf schema, C->D->Array(C)") {
    val schemaC = ProtobufUtils.buildDescriptor(testFileDesc, "recursiveC")
    val schemaD = ProtobufUtils.buildDescriptor(testFileDesc, "recursiveD")

    val messageDForC = DynamicMessage
      .newBuilder(schemaD)
      .setField(schemaD.findFieldByName("keyD"), "key")
      .build()

    val messageC = DynamicMessage
      .newBuilder(schemaC)
      .setField(schemaC.findFieldByName("keyC"), "key")
      .setField(schemaC.findFieldByName("messageD"), messageDForC)
      .build()

    val messageD = DynamicMessage
      .newBuilder(schemaD)
      .setField(schemaD.findFieldByName("keyD"), "key")
      .addRepeatedField(schemaD.findFieldByName("messageC"), messageC)
      .build()

    val df = Seq(messageD.toByteArray).toDF("messageD")

    val e = intercept[IncompatibleSchemaException] {
      df.select(
        functions.from_protobuf($"messageD", testFileDesc, "recursiveD").as("messageFromProto"))
        .show()
    }
    val expectedMessage =
      s"""
         |Found recursive reference in Protobuf schema, which can not be processed by Spark:
         |org.apache.spark.sql.protobuf.recursiveD.messageC""".stripMargin
    assert(e.getMessage == expectedMessage)
  }

  test("Handle extra fields : oldProducer -> newConsumer") {
    val testFileDesc = testFile("protobuf/catalyst_types.desc").replace("file:/", "/")
    val oldProducer = ProtobufUtils.buildDescriptor(testFileDesc, "oldProducer")
    val newConsumer = ProtobufUtils.buildDescriptor(testFileDesc, "newConsumer")

    val oldProducerMessage = DynamicMessage
      .newBuilder(oldProducer)
      .setField(oldProducer.findFieldByName("key"), "key")
      .build()

    val df = Seq(oldProducerMessage.toByteArray).toDF("oldProducerData")
    val fromProtoDf = df.select(
      functions
        .from_protobuf($"oldProducerData", testFileDesc, "newConsumer")
        .as("fromProto"))

    val toProtoDf = fromProtoDf.select(
      functions
        .to_protobuf($"fromProto", testFileDesc, "newConsumer")
        .as("toProto"))

    val toProtoDfToFromProtoDf = toProtoDf.select(
      functions
        .from_protobuf($"toProto", testFileDesc, "newConsumer")
        .as("toProtoToFromProto"))

    val actualFieldNames =
      toProtoDfToFromProtoDf.select("toProtoToFromProto.*").schema.fields.toSeq.map(f => f.name)
    newConsumer.getFields.asScala.map { f =>
      {
        assert(actualFieldNames.contains(f.getName))

      }
    }
    assert(
      toProtoDfToFromProtoDf.select("toProtoToFromProto.value").take(1).toSeq(0).get(0) == null)
    assert(
      toProtoDfToFromProtoDf.select("toProtoToFromProto.actual.*").take(1).toSeq(0).get(0) == null)
  }

  test("Handle extra fields : newProducer -> oldConsumer") {
    val testFileDesc = testFile("protobuf/catalyst_types.desc").replace("file:/", "/")
    val newProducer = ProtobufUtils.buildDescriptor(testFileDesc, "newProducer")
    val oldConsumer = ProtobufUtils.buildDescriptor(testFileDesc, "oldConsumer")

    val newProducerMessage = DynamicMessage
      .newBuilder(newProducer)
      .setField(newProducer.findFieldByName("key"), "key")
      .setField(newProducer.findFieldByName("value"), 1)
      .build()

    val df = Seq(newProducerMessage.toByteArray).toDF("newProducerData")
    val fromProtoDf = df.select(
      functions
        .from_protobuf($"newProducerData", testFileDesc, "oldConsumer")
        .as("oldConsumerProto"))

    val expectedFieldNames = oldConsumer.getFields.asScala.map(f => f.getName)
    fromProtoDf.select("oldConsumerProto.*").schema.fields.toSeq.map { f =>
      {
        assert(expectedFieldNames.contains(f.name))
      }
    }
  }

  test("roundtrip in to_protobuf and from_protobuf - with nulls") {
    val schema = StructType(
      StructField("requiredMsg",
        StructType(
          StructField("key", StringType, nullable = false) ::
            StructField("col_1", IntegerType, nullable = true) ::
            StructField("col_2", StringType, nullable = false) ::
            StructField("col_3", IntegerType, nullable = true) :: Nil
        ),
        nullable = true
      ) :: Nil
    )
    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Row("key1", null, "value2", null))
      )),
      schema
    )
    val toProtobuf = inputDf.select(
      functions.to_protobuf($"requiredMsg", testFileDesc, "requiredMsg")
        .as("to_proto"))

    val binary = toProtobuf.take(1).toSeq(0).get(0).asInstanceOf[Array[Byte]]

    val messageDescriptor = ProtobufUtils.buildDescriptor(testFileDesc, "requiredMsg")
    val actualMessage = DynamicMessage.parseFrom(messageDescriptor, binary)

    assert(actualMessage.getField(messageDescriptor.findFieldByName("key"))
      == inputDf.select("requiredMsg.key").take(1).toSeq(0).get(0))
    assert(actualMessage.getField(messageDescriptor.findFieldByName("col_2"))
      == inputDf.select("requiredMsg.col_2").take(1).toSeq(0).get(0))
    assert(actualMessage.getField(messageDescriptor.findFieldByName("col_1")) == 0)
    assert(actualMessage.getField(messageDescriptor.findFieldByName("col_3")) == 0)

    val fromProtoDf = toProtobuf.select(
      functions.from_protobuf($"to_proto", testFileDesc, "requiredMsg") as 'from_proto)

    assert(fromProtoDf.select("from_proto.key").take(1).toSeq(0).get(0)
      == inputDf.select("requiredMsg.key").take(1).toSeq(0).get(0))
    assert(fromProtoDf.select("from_proto.col_2").take(1).toSeq(0).get(0)
      == inputDf.select("requiredMsg.col_2").take(1).toSeq(0).get(0))
    assert(fromProtoDf.select("from_proto.col_1").take(1).toSeq(0).get(0) == null)
    assert(fromProtoDf.select("from_proto.col_3").take(1).toSeq(0).get(0) == null)
  }

  test("from_protobuf filter to_protobuf") {
    val basicMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "slam")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer"))
      .build()

    val df = Seq(basicMessage.toByteArray).toDF("value")
    val resultFrom = df
      .select(functions.from_protobuf($"value", testFileDesc, "BasicMessage") as 'sample)
      .where("sample.string_value == \"slam\"")

    val resultToFrom = resultFrom
      .select(functions.to_protobuf($"sample", testFileDesc, "BasicMessage") as 'value)
      .select(functions.from_protobuf($"value", testFileDesc, "BasicMessage") as 'sample)
      .where("sample.string_value == \"slam\"")

    assert(resultFrom.except(resultToFrom).isEmpty)
  }

  test("Handle TimestampType between to_protobuf and from_protobuf") {
    val schema = StructType(
      StructField("timeStampMsg",
        StructType(
          StructField("key", StringType, nullable = true) ::
            StructField("stmp", TimestampType, nullable = true) :: Nil
        ),
        nullable = true
      ) :: Nil
    )

    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Row("key1", Timestamp.valueOf("2016-05-09 10:12:43.999")))
      )),
      schema
    )

    val toProtoDf = inputDf
      .select(functions.to_protobuf($"timeStampMsg", testFileDesc, "timeStampMsg") as 'to_proto)

    val fromProtoDf = toProtoDf
      .select(functions.from_protobuf($"to_proto", testFileDesc, "timeStampMsg") as 'timeStampMsg)
    fromProtoDf.show(truncate = false)

    val actualFields = fromProtoDf.schema.fields.toList
    val expectedFields = inputDf.schema.fields.toList

    assert(actualFields.size === expectedFields.size)
    assert(actualFields === expectedFields)
    assert(fromProtoDf.select("timeStampMsg.key").take(1).toSeq(0).get(0)
      === inputDf.select("timeStampMsg.key").take(1).toSeq(0).get(0))
    assert(fromProtoDf.select("timeStampMsg.stmp").take(1).toSeq(0).get(0)
      === inputDf.select("timeStampMsg.stmp").take(1).toSeq(0).get(0))
  }

  test("Handle DayTimeIntervalType between to_protobuf and from_protobuf") {
    val schema = StructType(
      StructField("durationMsg",
        StructType(
          StructField("key", StringType, nullable = true) ::
            StructField("duration",
              DayTimeIntervalType.defaultConcreteType, nullable = true) :: Nil
        ),
        nullable = true
      ) :: Nil
    )

    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(Row("key1",
          Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)
        ))
      )),
      schema
    )

    val toProtoDf = inputDf
      .select(functions.to_protobuf($"durationMsg", testFileDesc, "durationMsg") as 'to_proto)

    val fromProtoDf = toProtoDf
      .select(functions.from_protobuf($"to_proto", testFileDesc, "durationMsg") as 'durationMsg)

    val actualFields = fromProtoDf.schema.fields.toList
    val expectedFields = inputDf.schema.fields.toList

    assert(actualFields.size === expectedFields.size)
    assert(actualFields === expectedFields)
    assert(fromProtoDf.select("durationMsg.key").take(1).toSeq(0).get(0)
      === inputDf.select("durationMsg.key").take(1).toSeq(0).get(0))
    assert(fromProtoDf.select("durationMsg.duration").take(1).toSeq(0).get(0)
      === inputDf.select("durationMsg.duration").take(1).toSeq(0).get(0))

  }
}
