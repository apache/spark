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

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage

import org.apache.spark.sql.catalyst.NoopFilters
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.utils.SchemaConverters.IncompatibleSchemaException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Tests for [[ProtobufSerializer]] and [[ProtobufDeserializer]] with a more specific focus on
 * those classes.
 */
class ProtobufSerdeSuite extends SharedSparkSession {

  import ProtoSerdeSuite._
  import ProtoSerdeSuite.MatchType._

  val testFileDesc = testFile("protobuf/serde_suite.desc").replace("file:/", "/")
  private val javaClassNamePrefix = "org.apache.spark.sql.protobuf.protos.SerdeSuiteProtos$"

  test("Test basic conversion") {
    withFieldMatchType { fieldMatch =>
      val (top, nest) = fieldMatch match {
        case BY_NAME => ("foo", "bar")
      }
      val protoFile = ProtobufUtils.buildDescriptor(testFileDesc, "BasicMessage")

      val dynamicMessageFoo = DynamicMessage
        .newBuilder(protoFile.getFile.findMessageTypeByName("Foo"))
        .setField(protoFile.getFile.findMessageTypeByName("Foo").findFieldByName("bar"), 10902)
        .build()

      val dynamicMessage = DynamicMessage
        .newBuilder(protoFile)
        .setField(protoFile.findFieldByName("foo"), dynamicMessageFoo)
        .build()

      val serializer = Serializer.create(CATALYST_STRUCT, protoFile, fieldMatch)
      val deserializer = Deserializer.create(CATALYST_STRUCT, protoFile, fieldMatch)

      assert(
        serializer.serialize(deserializer.deserialize(dynamicMessage).get) === dynamicMessage)
    }
  }

  test("Fail to convert with field type mismatch") {
    val protoFile = ProtobufUtils.buildDescriptor(testFileDesc, "MissMatchTypeInRoot")

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(
        protoFile,
        Deserializer,
        fieldMatch,
        "Cannot convert Protobuf field 'foo' to SQL field 'foo' because schema is incompatible " +
          s"(protoType = org.apache.spark.sql.protobuf.MissMatchTypeInRoot.foo " +
          s"LABEL_OPTIONAL LONG INT64, sqlType = ${CATALYST_STRUCT.head.dataType.sql})".stripMargin)

      assertFailedConversionMessage(
        protoFile,
        Serializer,
        fieldMatch,
        s"Cannot convert SQL field 'foo' to Protobuf field 'foo' because schema is incompatible " +
          s"""(sqlType = ${CATALYST_STRUCT.head.dataType.sql}, protoType = LONG)""")
    }
  }

  test("Fail to convert with missing nested Protobuf fields for serializer") {
    val protoFile = ProtobufUtils.buildDescriptor(testFileDesc, "FieldMissingInProto")

    val nonnullCatalyst = new StructType()
      .add("foo", new StructType().add("bar", IntegerType, nullable = false))

    // serialize fails whether or not 'bar' is nullable
    val byNameMsg = "Cannot find field 'foo.bar' in Protobuf schema"
    assertFailedConversionMessage(protoFile, Serializer, BY_NAME, byNameMsg)
    assertFailedConversionMessage(protoFile, Serializer, BY_NAME, byNameMsg, nonnullCatalyst)
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val protoFile = ProtobufUtils.buildDescriptorFromJavaClass(
      s"${javaClassNamePrefix}MissMatchTypeInDeepNested"
    )
    val catalyst = new StructType().add("top", CATALYST_STRUCT)

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(
        protoFile,
        Deserializer,
        fieldMatch,
        s"Cannot convert Protobuf field 'top.foo.bar' to SQL field 'top.foo.bar' because schema " +
          s"is incompatible (protoType = org.apache.spark.sql.protobuf.protos.TypeMiss.bar " +
          s"LABEL_OPTIONAL LONG INT64, sqlType = INT)",
        catalyst)

      assertFailedConversionMessage(
        protoFile,
        Serializer,
        fieldMatch,
        "Cannot convert SQL field 'top.foo.bar' to Protobuf field 'top.foo.bar' because schema " +
          """is incompatible (sqlType = INT, protoType = LONG)""",
        catalyst)
    }
  }

  test("Fail to convert with missing Catalyst fields") {
    val protoFile = ProtobufUtils.buildDescriptor(testFileDesc, "FieldMissingInSQLRoot")

    // serializing with extra fails if extra field is missing in SQL Schema
    assertFailedConversionMessage(
      protoFile,
      Serializer,
      BY_NAME,
      "Found field 'boo' in Protobuf schema but there is no match in the SQL schema")

    /* deserializing should work regardless of whether the extra field is missing
     in SQL Schema or not */
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoFile, _))
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoFile, _))

    val protoNestedFile = ProtobufUtils.buildDescriptor(testFileDesc, "FieldMissingInSQLNested")

    // serializing with extra fails if extra field is missing in SQL Schema
    assertFailedConversionMessage(
      protoNestedFile,
      Serializer,
      BY_NAME,
      "Found field 'foo.baz' in Protobuf schema but there is no match in the SQL schema")

    /* deserializing should work regardless of whether the extra field is missing
      in SQL Schema or not */
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoNestedFile, _))
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoNestedFile, _))
  }

  /**
   * Attempt to convert `catalystSchema` to `protoSchema` (or vice-versa if `deserialize` is
   * true), assert that it fails, and assert that the _cause_ of the thrown exception has a
   * message matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(
      protoSchema: Descriptor,
      serdeFactory: SerdeFactory[_],
      fieldMatchType: MatchType,
      expectedCauseMessage: String,
      catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      serdeFactory.create(catalystSchema, protoSchema, fieldMatchType)
    }
    val expectMsg = serdeFactory match {
      case Deserializer =>
        s"Cannot convert Protobuf type ${protoSchema.getName} to SQL type ${catalystSchema.sql}."
      case Serializer =>
        s"Cannot convert SQL type ${catalystSchema.sql} to Protobuf type ${protoSchema.getName}."
    }

    assert(e.getMessage === expectMsg)
    assert(e.getCause.getMessage === expectedCauseMessage)
  }

  def withFieldMatchType(f: MatchType => Unit): Unit = {
    MatchType.values.foreach { fieldMatchType =>
      withClue(s"fieldMatchType == $fieldMatchType") {
        f(fieldMatchType)
      }
    }
  }
}

object ProtoSerdeSuite {

  val CATALYST_STRUCT =
    new StructType().add("foo", new StructType().add("bar", IntegerType))

  /**
   * Specifier for type of field matching to be used for easy creation of tests that do by-name
   * field matching.
   */
  object MatchType extends Enumeration {
    type MatchType = Value
    val BY_NAME = Value
  }

  import MatchType._

  /**
   * Specifier for type of serde to be used for easy creation of tests that do both serialization
   * and deserialization.
   */
  sealed trait SerdeFactory[T] {
    def create(sqlSchema: StructType, descriptor: Descriptor, fieldMatchType: MatchType): T
  }

  object Serializer extends SerdeFactory[ProtobufSerializer] {
    override def create(
        sql: StructType,
        descriptor: Descriptor,
        matchType: MatchType): ProtobufSerializer = new ProtobufSerializer(sql, descriptor, false)
  }

  object Deserializer extends SerdeFactory[ProtobufDeserializer] {
    override def create(
        sql: StructType,
        descriptor: Descriptor,
        matchType: MatchType): ProtobufDeserializer =
      new ProtobufDeserializer(descriptor, sql, new NoopFilters)
  }
}
