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

import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.protobuf.protos.SimpleMessageProtos.SimpleMessageRepeated
import org.apache.spark.sql.protobuf.protos.SimpleMessageProtos.SimpleMessageRepeated.NestedEnum
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, StringType, StructField, StructType, TimestampType}

class ProtobufFunctionsSuite extends QueryTest with SharedSparkSession with ProtobufTestBase
  with Serializable {

  import testImplicits._

  val testFileDesc = testFile("functions_suite.desc", "protobuf/functions_suite.desc")
  private val javaClassNamePrefix = "org.apache.spark.sql.protobuf.protos.SimpleMessageProtos$"

  /**
   * Runs the given closure twice. Once with descriptor file and second time with Java class name.
   */
  private def checkWithFileAndClassName(messageName: String)(
    fn: (String, Option[String]) => Unit): Unit = {
      withClue("(With descriptor file)") {
        fn(messageName, Some(testFileDesc))
      }
      withClue("(With Java class name)") {
        fn(s"$javaClassNamePrefix$messageName", None)
      }
  }

  // A wrapper to invoke the right variable of from_protobuf() depending on arguments.
  private def from_protobuf_wrapper(
    col: Column, messageName: String, descFilePathOpt: Option[String]): Column = {
    descFilePathOpt match {
      case Some(descFilePath) => functions.from_protobuf(col, messageName, descFilePath)
      case None => functions.from_protobuf(col, messageName)
    }
  }

  // A wrapper to invoke the right variable of to_protobuf() depending on arguments.
  private def to_protobuf_wrapper(
    col: Column, messageName: String, descFilePathOpt: Option[String]): Column = {
    descFilePathOpt match {
      case Some(descFilePath) => functions.to_protobuf(col, messageName, descFilePath)
      case None => functions.to_protobuf(col, messageName)
    }
  }


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

    checkWithFileAndClassName("SimpleMessage") {
      case (name, descFilePathOpt) =>
        val protoStructDF = df.select(
          to_protobuf_wrapper($"SimpleMessage", name, descFilePathOpt).as("proto"))
        val actualDf = protoStructDF.select(
          from_protobuf_wrapper($"proto", name, descFilePathOpt).as("proto.*"))
        checkAnswer(actualDf, df)
    }
  }

  test("roundtrip in from_protobuf and to_protobuf - Repeated") {

    val protoMessage = SimpleMessageRepeated
      .newBuilder()
      .setKey("key")
      .setValue("value")
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092.654d)
      .addRdoubleValue(1092093.654d)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .addRnestedEnum(NestedEnum.NESTED_NOTHING)
      .addRnestedEnum(NestedEnum.NESTED_FIRST)
      .build()

    val df = Seq(protoMessage.toByteArray).toDF("value")

    checkWithFileAndClassName("SimpleMessageRepeated") {
      case (name, descFilePathOpt) =>
        val fromProtoDF = df.select(
          from_protobuf_wrapper($"value", name, descFilePathOpt).as("value_from"))
        val toProtoDF = fromProtoDF.select(
          to_protobuf_wrapper($"value_from", name, descFilePathOpt).as("value_to"))
        val toFromProtoDF = toProtoDF.select(
          from_protobuf_wrapper($"value_to", name, descFilePathOpt).as("value_to_from"))
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
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

    checkWithFileAndClassName("RepeatedMessage") {
      case (name, descFilePathOpt) =>
        val fromProtoDF = df.select(
          from_protobuf_wrapper($"value", name, descFilePathOpt).as("value_from"))
        val toProtoDF = fromProtoDF.select(
          to_protobuf_wrapper($"value_from", name, descFilePathOpt).as("value_to"))
        val toFromProtoDF = toProtoDF.select(
          from_protobuf_wrapper($"value_to", name, descFilePathOpt).as("value_to_from"))
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
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

    checkWithFileAndClassName("RepeatedMessage") {
      case (name, descFilePathOpt) =>
        val fromProtoDF = df.select(
          from_protobuf_wrapper($"value", name, descFilePathOpt).as("value_from"))
        val toProtoDF = fromProtoDF.select(
          to_protobuf_wrapper($"value_from", name, descFilePathOpt).as("value_to"))
        val toFromProtoDF = toProtoDF.select(
          from_protobuf_wrapper($"value_to", name, descFilePathOpt).as("value_to_from"))
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
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

    checkWithFileAndClassName("SimpleMessageMap") {
      case (name, descFilePathOpt) =>
        val fromProtoDF = df.select(
          from_protobuf_wrapper($"value", name, descFilePathOpt).as("value_from"))
        val toProtoDF = fromProtoDF.select(
          to_protobuf_wrapper($"value_from", name, descFilePathOpt).as("value_to"))
        val toFromProtoDF = toProtoDF.select(
          from_protobuf_wrapper($"value_to", name, descFilePathOpt).as("value_to_from"))
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
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
        messageEnumDesc.findEnumTypeByName("NestedEnum").findValueByName("NESTED_NOTHING"))
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

    checkWithFileAndClassName("SimpleMessageEnum") {
      case (name, descFilePathOpt) =>
        val fromProtoDF = df.select(
          from_protobuf_wrapper($"value", name, descFilePathOpt).as("value_from"))
        val toProtoDF = fromProtoDF.select(
          to_protobuf_wrapper($"value_from", name, descFilePathOpt).as("value_to"))
        val toFromProtoDF = toProtoDF.select(
          from_protobuf_wrapper($"value_to", name, descFilePathOpt).as("value_to_from"))
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
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

    checkWithFileAndClassName("MultipleExample") {
      case (name, descFilePathOpt) =>
        val fromProtoDF = df.select(
          from_protobuf_wrapper($"value", name, descFilePathOpt).as("value_from"))
        val toProtoDF = fromProtoDF.select(
          to_protobuf_wrapper($"value_from", name, descFilePathOpt).as("value_to"))
        val toFromProtoDF = toProtoDF.select(
          from_protobuf_wrapper($"value_to", name, descFilePathOpt).as("value_to_from"))
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
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

    checkWithFileAndClassName("recursiveB") {
      case (name, descFilePathOpt) =>
        val e = intercept[AnalysisException] {
          df.select(
            from_protobuf_wrapper($"messageB", name, descFilePathOpt).as("messageFromProto"))
            .show()
        }
        assert(e.getMessage.contains(
          "Found recursive reference in Protobuf schema, which can not be processed by Spark:"
        ))
    }
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

    checkWithFileAndClassName("recursiveD") {
      case (name, descFilePathOpt) =>
        val e = intercept[AnalysisException] {
          df.select(
            from_protobuf_wrapper($"messageD", name, descFilePathOpt).as("messageFromProto"))
            .show()
        }
        assert(e.getMessage.contains(
          "Found recursive reference in Protobuf schema, which can not be processed by Spark:"
        ))
    }
  }

  test("Handle extra fields : oldProducer -> newConsumer") {
    val testFileDesc = testFile("catalyst_types.desc", "protobuf/catalyst_types.desc")
    val oldProducer = ProtobufUtils.buildDescriptor(testFileDesc, "oldProducer")
    val newConsumer = ProtobufUtils.buildDescriptor(testFileDesc, "newConsumer")

    val oldProducerMessage = DynamicMessage
      .newBuilder(oldProducer)
      .setField(oldProducer.findFieldByName("key"), "key")
      .build()

    val df = Seq(oldProducerMessage.toByteArray).toDF("oldProducerData")
    val fromProtoDf = df.select(
      functions
        .from_protobuf($"oldProducerData", "newConsumer", testFileDesc)
        .as("fromProto"))

    val toProtoDf = fromProtoDf.select(
      functions
        .to_protobuf($"fromProto", "newConsumer", testFileDesc)
        .as("toProto"))

    val toProtoDfToFromProtoDf = toProtoDf.select(
      functions
        .from_protobuf($"toProto", "newConsumer", testFileDesc)
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
    val testFileDesc = testFile("catalyst_types.desc", "protobuf/catalyst_types.desc")
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
        .from_protobuf($"newProducerData", "oldConsumer", testFileDesc)
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
      functions.to_protobuf($"requiredMsg", "requiredMsg", testFileDesc)
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
      functions.from_protobuf($"to_proto", "requiredMsg", testFileDesc) as 'from_proto)

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
      .select(from_protobuf_wrapper($"value", "BasicMessage", Some(testFileDesc)) as 'sample)
      .where("sample.string_value == \"slam\"")

    val resultToFrom = resultFrom
      .select(to_protobuf_wrapper($"sample", "BasicMessage", Some(testFileDesc)) as 'value)
      .select(from_protobuf_wrapper($"value", "BasicMessage", Some(testFileDesc)) as 'sample)
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

    checkWithFileAndClassName("timeStampMsg") {
      case (name, descFilePathOpt) =>
        val toProtoDf = inputDf
          .select(to_protobuf_wrapper($"timeStampMsg", name, descFilePathOpt) as 'to_proto)

        val fromProtoDf = toProtoDf
          .select(from_protobuf_wrapper($"to_proto", name, descFilePathOpt) as 'timeStampMsg)

        val actualFields = fromProtoDf.schema.fields.toList
        val expectedFields = inputDf.schema.fields.toList

        assert(actualFields.size === expectedFields.size)
        assert(actualFields === expectedFields)
        assert(fromProtoDf.select("timeStampMsg.key").take(1).toSeq(0).get(0)
          === inputDf.select("timeStampMsg.key").take(1).toSeq(0).get(0))
        assert(fromProtoDf.select("timeStampMsg.stmp").take(1).toSeq(0).get(0)
          === inputDf.select("timeStampMsg.stmp").take(1).toSeq(0).get(0))
    }
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

    checkWithFileAndClassName("durationMsg") {
      case (name, descFilePathOpt) =>
        val toProtoDf = inputDf
          .select(to_protobuf_wrapper($"durationMsg", name, descFilePathOpt) as 'to_proto)

        val fromProtoDf = toProtoDf
          .select(from_protobuf_wrapper($"to_proto", name, descFilePathOpt) as 'durationMsg)

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

  test("raise cannot construct protobuf descriptor error") {
    val df = Seq(ByteString.empty().toByteArray).toDF("value")
    val testFileDescriptor =
      testFile("basicmessage_noimports.desc", "protobuf/basicmessage_noimports.desc")

    val e = intercept[AnalysisException] {
      df.select(functions.from_protobuf($"value", "BasicMessage", testFileDescriptor) as 'sample)
        .where("sample.string_value == \"slam\"").show()
    }
    checkError(
      exception = e,
      errorClass = "CANNOT_CONSTRUCT_PROTOBUF_DESCRIPTOR",
      parameters = Map("descFilePath" -> testFileDescriptor))
  }
}
