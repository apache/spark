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

import com.google.protobuf.DynamicMessage
import com.google.protobuf.DescriptorProtos.FileDescriptorSet

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{ProtobufUtils => CommonProtobufUtils}

class ProtobufExtensionsSuite
    extends QueryTest
    with SharedSparkSession
    with ProtobufTestBase
    with Serializable {

  import testImplicits._

  val proto2FileDescFile = protobufDescriptorFile("proto2_messages.desc")
  val proto2FileDesc = CommonProtobufUtils.readDescriptorFileContent(proto2FileDescFile)
  private val proto2JavaClassNamePrefix = "org.apache.spark.sql.protobuf.protos.Proto2Messages$"

  test("SPARK-55062: roundtrip - proto2 extension basic types") {
    withExtensionSupport {
      val descWithExt = ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions(descriptor.getFullName)

      val extStringField = extensionFields.find(_.getName == "extension_string").get
      val extIntField = extensionFields.find(_.getName == "extension_int").get

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "test_name")
        .setField(descriptor.findFieldByName("id"), 42)
        .setField(extStringField, "ext_value")
        .setField(extIntField, 123)
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions.from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc).as("value_from"))

      // Verify extension field values are correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("name") == "test_name")
      assert(row.getAs[Int]("id") == 42)
      assert(row.getAs[String]("extension_string") == "ext_value")
      assert(row.getAs[Int]("extension_int") == 123)

      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: roundtrip - proto2 extension enum") {
    withExtensionSupport {
      val descWithExt = ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions(descriptor.getFullName)

      val extEnumField = extensionFields.find(_.getName == "extension_enum").get

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "enum_test")
        .setField(descriptor.findFieldByName("id"), 99)
        .setField(extEnumField, extEnumField.getEnumType.findValueByName("FIRST"))
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions.from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc).as("value_from"))

      // Verify extension field value is correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("name") == "enum_test")
      assert(row.getAs[Int]("id") == 99)
      assert(row.getAs[String]("extension_enum") == "FIRST")

      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: roundtrip - proto2 extension nested message") {
    withExtensionSupport {
      val descWithExt = ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions
        .getOrElse(descriptor.getFullName, Seq.empty)

      val extMessageField = extensionFields.find(_.getName == "extension_message").get

      val nestedMessage = DynamicMessage
        .newBuilder(extMessageField.getMessageType)
        .setField(extMessageField.getMessageType.findFieldByName("nested_value"), "nested_test")
        .setField(extMessageField.getMessageType.findFieldByName("nested_id"), 99)
        .build()

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "main")
        .setField(extMessageField, nestedMessage)
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions.from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc).as("value_from"))

      // Verify extension field value is correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("name") == "main")
      val nestedRow = row.getAs[Row]("extension_message")
      assert(nestedRow.getAs[String]("nested_value") == "nested_test")
      assert(nestedRow.getAs[Int]("nested_id") == 99)

      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: roundtrip - proto2 extension repeated") {
    withExtensionSupport {
      val descWithExt = ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions
        .getOrElse(descriptor.getFullName, Seq.empty)

      val extRepeatedField = extensionFields.find(_.getName == "extension_repeated_int").get

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "repeated_test")
        .addRepeatedField(extRepeatedField, 1)
        .addRepeatedField(extRepeatedField, 2)
        .addRepeatedField(extRepeatedField, 3)
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions.from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc).as("value_from"))

      // Verify extension field value is correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("name") == "repeated_test")
      val repeatedValues = row.getSeq[Int](row.fieldIndex("extension_repeated_int"))
      assert(repeatedValues == Seq(1, 2, 3))

      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: roundtrip - proto2 extension defined in another message") {
    withExtensionSupport {
      val descWithExt = ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions
        .getOrElse(descriptor.getFullName, Seq.empty)

      val nestedExtBoolField = extensionFields.find(_.getName == "nested_extension_bool")
      assert(
        nestedExtBoolField.isDefined,
        "Should find extension defined in message ContainerWithNestedExtension")

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "nested_ext_test")
        .setField(nestedExtBoolField.get, true)
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions.from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc).as("value_from"))

      // Verify extension field value is correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("name") == "nested_ext_test")
      assert(row.getAs[Boolean]("nested_extension_bool"))

      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "Proto2ExtensionTest", proto2FileDesc)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: roundtrip - proto2 extension on nested message") {
    withExtensionSupport {
      val descWithExt =
        ProtobufUtils.buildDescriptor("MessageWithExtendableNested", Some(proto2FileDesc))
      val topLevelDescriptor = descWithExt.descriptor

      val nestedFieldDesc = topLevelDescriptor.findFieldByName("nested")
      val nestedMsgDescriptor = nestedFieldDesc.getMessageType
      val nestedExtensions = descWithExt.fullNamesToExtensions
        .getOrElse(nestedMsgDescriptor.getFullName, Seq.empty)

      assert(
        nestedExtensions.nonEmpty,
        "Should find extensions for nested message type ExtendableNestedMessage")
      val nestedExtField = nestedExtensions.find(_.getName == "nested_ext_field").get
      val nestedExtInt = nestedExtensions.find(_.getName == "nested_ext_int").get

      val nestedMessage = DynamicMessage
        .newBuilder(nestedMsgDescriptor)
        .setField(nestedMsgDescriptor.findFieldByName("nested_name"), "nested_name_value")
        .setField(nestedMsgDescriptor.findFieldByName("nested_id"), 42)
        .setField(nestedExtField, "ext_field_value")
        .setField(nestedExtInt, 123)
        .build()

      val topLevelMessage = DynamicMessage
        .newBuilder(topLevelDescriptor)
        .setField(topLevelDescriptor.findFieldByName("top_level_name"), "top_name")
        .setField(nestedFieldDesc, nestedMessage)
        .build()

      val df = Seq(topLevelMessage.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions
          .from_protobuf($"value", "MessageWithExtendableNested", proto2FileDesc)
          .as("value_from"))

      // Verify nested extension field values are correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("top_level_name") == "top_name")
      val nestedRow = row.getAs[Row]("nested")
      assert(nestedRow.getAs[String]("nested_name") == "nested_name_value")
      assert(nestedRow.getAs[Int]("nested_id") == 42)
      assert(nestedRow.getAs[String]("nested_ext_field") == "ext_field_value")
      assert(nestedRow.getAs[Int]("nested_ext_int") == 123)

      // Verify roundtrip preserves values
      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "MessageWithExtendableNested", proto2FileDesc)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "MessageWithExtendableNested", proto2FileDesc)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: roundtrip - proto2 extension in separate file") {
    withExtensionSupport {
      // NB: We manually construct a merged descriptor file because Maven's Protobuf plugin
      // generates a single descriptor file for each .proto file.
      val extDescriptorPath = protobufDescriptorFile("proto2_messages_ext.desc")
      val fdsExt = FileDescriptorSet.parseFrom(
        CommonProtobufUtils.readDescriptorFileContent(extDescriptorPath))
      val fds = FileDescriptorSet.parseFrom(proto2FileDesc)
      val combinedFds = FileDescriptorSet.newBuilder
        .addAllFile(fdsExt.getFileList)
        .addAllFile(fds.getFileList)
        .build()
      val combinedFdsBytes = combinedFds.toByteArray

      val descWithExt =
        ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(combinedFds.toByteArray))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions
        .getOrElse(descriptor.getFullName, Seq.empty)

      val extMessageField = extensionFields.find(_.getName == "cross_file_extension").get

      val nestedMessage = DynamicMessage
        .newBuilder(extMessageField.getMessageType)
        .setField(extMessageField.getMessageType.findFieldByName("foo"), 1)
        .build()

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "main")
        .setField(extMessageField, nestedMessage)
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      val fromProtoDF = df.select(
        functions
          .from_protobuf($"value", "Proto2ExtensionTest", combinedFdsBytes)
          .as("value_from"))

      // Verify extension field value is correct
      val row = fromProtoDF.select($"value_from.*").collect().head
      assert(row.getAs[String]("name") == "main")
      val crossFileExtRow = row.getAs[Row]("cross_file_extension")
      assert(crossFileExtRow.getAs[Int]("foo") == 1)

      val toProtoDF = fromProtoDF.select(
        functions
          .to_protobuf($"value_from", "Proto2ExtensionTest", combinedFdsBytes)
          .as("value_to"))
      val toFromProtoDF = toProtoDF.select(
        functions
          .from_protobuf($"value_to", "Proto2ExtensionTest", combinedFdsBytes)
          .as("value_to_from"))

      checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("SPARK-55062: proto2 extension field name collision with regular field") {
    withExtensionSupport {
      val descriptor =
        ProtobufUtils
          .buildDescriptor("Proto2ExtensionCollisionTest", Some(proto2FileDesc))
          .descriptor
      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "test")
        .build()

      val df = Seq(message.toByteArray).toDF("value")

      // Some unwrapping required to get to the root error, as it is thrown during execution.
      val e = intercept[SparkException] {
        df.select(
          functions.from_protobuf($"value", "Proto2ExtensionCollisionTest", proto2FileDesc))
          .collect()
      }
      checkError(
        exception = e.getCause
          .asInstanceOf[AnalysisException]
          .getCause
          .asInstanceOf[AnalysisException],
        condition = "PROTOBUF_FIELD_MISSING",
        parameters = Map(
          "field" -> "name",
          "protobufSchema" -> "top-level record",
          "matchSize" -> "2",
          "matches" -> "[name, NAME]"))
    }
  }

  test("SPARK-55062: schema evolution - data without extensions read with extension schema") {
    withExtensionSupport {
      val descriptorWithoutExt =
        ProtobufUtils.buildDescriptor("Proto2ExtensionTestBase", Some(proto2FileDesc)).descriptor
      val messageWithoutExtensions = DynamicMessage
        .newBuilder(descriptorWithoutExt)
        .setField(descriptorWithoutExt.findFieldByName("name"), "base_name")
        .setField(descriptorWithoutExt.findFieldByName("id"), 42)
        .build()

      val df = Seq(messageWithoutExtensions.toByteArray).toDF("value")

      // Read using schema WITH extensions
      val fromProtoDF = df.select(
        functions.from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc).as("parsed"))
      val result = fromProtoDF.select($"parsed.*").collect()
      assert(result.length == 1)
      val row = result(0)

      // Regular fields have expected values, while extension fields are null/empty
      assert(row.getAs[String]("name") == "base_name")
      assert(row.getAs[Int]("id") == 42)
      assert(row.getAs[String]("extension_string") == null)
      assert(row.isNullAt(row.fieldIndex("extension_int")))
      assert(row.isNullAt(row.fieldIndex("extension_enum")))
      assert(row.isNullAt(row.fieldIndex("extension_message")))
      assert(row.isNullAt(row.fieldIndex("nested_extension_bool")))
      assert(row.getSeq[Int](row.fieldIndex("extension_repeated_int")).isEmpty)
    }
  }

  test("SPARK-55062: Java class fallback drops extensions") {
    withExtensionSupport {
      val descWithExt = ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
      val descriptor = descWithExt.descriptor
      val extensionFields = descWithExt.fullNamesToExtensions(descriptor.getFullName)

      val extStringField = extensionFields.find(_.getName == "extension_string").get

      val message = DynamicMessage
        .newBuilder(descriptor)
        .setField(descriptor.findFieldByName("name"), "test_name")
        .setField(descriptor.findFieldByName("id"), 42)
        .setField(extStringField, "ext_value")
        .build()
      val df = Seq(message.toByteArray).toDF("value")

      // Read using Java class, which does not support extensions
      val fromProtoDF = df.select(
        functions
          .from_protobuf($"value", proto2JavaClassNamePrefix + "Proto2ExtensionTest")
          .as("parsed"))

      // Schema should only have regular fields, not extension fields
      val row = fromProtoDF.select($"parsed.*").collect().head
      val schema = row.schema
      assert(row.getAs[String]("name") == "test_name")
      assert(row.getAs[Int]("id") == 42)
      assert(!schema.fieldNames.contains("extension_string"))
      assert(!schema.fieldNames.contains("extension_int"))
    }
  }

  test("SPARK-55062: from_protobuf drops extension fields by default") {
    // We need extensions on when calling buildDescriptor so that we can access the extension
    // fields for test setup and assertions.
    val descWithExt = withExtensionSupport {
      ProtobufUtils.buildDescriptor("Proto2ExtensionTest", Some(proto2FileDesc))
    }
    val descriptor = descWithExt.descriptor
    val extensionFields = descWithExt.fullNamesToExtensions(descriptor.getFullName)

    val extStringField = extensionFields.find(_.getName == "extension_string").get
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "test_name")
      .setField(descriptor.findFieldByName("id"), 42)
      .setField(extStringField, "ext_value")
      .build()

    val df = Seq(message.toByteArray).toDF("value")

    val fromProtoDF = df.select(
      functions
        .from_protobuf($"value", "Proto2ExtensionTest", proto2FileDesc)
        .as("value_from"))
    val row = fromProtoDF.select($"value_from.*").collect().head
    val schema = row.schema
    assert(row.getAs[String]("name") == "test_name")
    assert(row.getAs[Int]("id") == 42)
    assert(!schema.fieldNames.contains("extension_string"))
    assert(!schema.fieldNames.contains("extension_int"))
  }

  test("SPARK-55062: to_protobuf does not recognize extension fields by default") {
    val schema = StructType(
      StructField(
        "Proto2ExtensionTest",
        StructType(
          StructField("name", StringType) ::
            StructField("id", IntegerType) ::
            StructField("extension_string", StringType) ::
            StructField("extension_int", IntegerType) :: Nil)) :: Nil)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(Row("test_name", 42, "extension_value", 123)))),
      schema)

    // We expect an error because from to_protobuf's perspective, the DataFrame contains extra
    // fields that are not in the message descriptor.
    val e = intercept[AnalysisException] {
      val toProtoDF = df
        .select(
          functions
            .to_protobuf($"Proto2ExtensionTest", "Proto2ExtensionTest", proto2FileDesc)
            .as("to_proto"))
        .collect()
    }
    val toType = "\"STRUCT<name: STRING, id: INT, extension_string: STRING, extension_int: INT>\""
    checkError(
      exception = e,
      condition = "UNABLE_TO_CONVERT_TO_PROTOBUF_MESSAGE_TYPE",
      parameters = Map("protobufType" -> "Proto2ExtensionTest", "toType" -> toType))
  }

  // Extension support is disabled by default.
  private def withExtensionSupport[T](f: => T): T = {
    val old = conf.getConf(PROTOBUF_EXTENSIONS_SUPPORT_ENABLED)
    conf.setConf(PROTOBUF_EXTENSIONS_SUPPORT_ENABLED, true)
    try {
      f
    } finally {
      conf.setConf(PROTOBUF_EXTENSIONS_SUPPORT_ENABLED, old)
    }
  }
}
