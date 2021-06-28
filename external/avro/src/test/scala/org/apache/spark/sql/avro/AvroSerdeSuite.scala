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
package org.apache.spark.sql.avro

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericRecordBuilder

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.NoopFilters
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Tests for [[AvroSerializer]] and [[AvroDeserializer]], complementing those in [[AvroSuite]]
 * with a more specific focus on those classes.
 */
class AvroSerdeSuite extends SparkFunSuite {
  import AvroSerdeSuite._
  import AvroSerdeSuite.SerdeType._
  import AvroSerdeSuite.FieldMatchType._

  test("Test basic conversion") {
    withFieldMatchType { fieldMatch =>
      val (top, nest) = fieldMatch match {
        case BY_NAME => ("foo", "bar")
        case BY_POSITION => ("NOTfoo", "NOTbar")
      }
      val avro = createNestedAvroSchemaWithFields(top, _.optionalInt(nest))
      val record = new GenericRecordBuilder(avro)
          .set(top, new GenericRecordBuilder(avro.getField(top).schema()).set(nest, 42).build())
          .build()
      val serializer = createSerde(CATALYST_STRUCT, avro, SERIALIZE, fieldMatch).left.get
      val deserializer = createSerde(CATALYST_STRUCT, avro, DESERIALIZE, fieldMatch).right.get
      assert(serializer.serialize(deserializer.deserialize(record).get) === record)
    }
  }

  test("Fail to convert with field type mismatch") {
    val avro = createAvroSchemaWithTopLevelFields(_.requiredInt("foo"))
    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(avro, DESERIALIZE, fieldMatch,
        "Cannot convert Avro field 'foo' to SQL field 'foo' because schema is incompatible " +
          s"""(avroType = "int", sqlType = ${CATALYST_STRUCT.head.dataType.sql})""")

      assertFailedConversionMessage(avro, SERIALIZE, fieldMatch,
        s"Cannot convert SQL field 'foo' to Avro field 'foo' because schema is incompatible " +
          s"""(sqlType = ${CATALYST_STRUCT.head.dataType.sql}, avroType = "int")""")
    }
  }

  test("Fail to convert with nested field type mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar"))

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(avro, DESERIALIZE, fieldMatch,
        "Cannot convert Avro field 'foo.bar' to SQL field 'foo.bar' because schema is " +
          """incompatible (avroType = "float", sqlType = INT)""")

      assertFailedConversionMessage(avro, SERIALIZE, fieldMatch,
        "Cannot convert SQL field 'foo.bar' to Avro field 'foo.bar' because " +
          """schema is incompatible (sqlType = INT, avroType = "float")""")
    }
  }

  test("Fail to convert with missing nested Avro fields") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalInt("NOTbar"))
    val nonnullCatalyst = new StructType()
        .add("foo", new StructType().add("bar", IntegerType, nullable = false))
    // Positional matching will work fine with the name change, so add a new field
    val extraNonnullCatalyst = new StructType().add("foo",
      new StructType().add("bar", IntegerType).add("baz", IntegerType, nullable = false))

    // deserialize should have no issues when 'bar' is nullable but fail when it is nonnull
    createSerde(CATALYST_STRUCT, avro, DESERIALIZE, BY_NAME)
    assertFailedConversionMessage(avro, DESERIALIZE, BY_NAME,
      "Cannot find non-nullable field 'foo.bar' (at position 0) in Avro schema.",
      nonnullCatalyst)
    assertFailedConversionMessage(avro, DESERIALIZE, BY_POSITION,
      "Cannot find non-nullable field at position 1 (field 'foo.baz') in Avro schema.",
      extraNonnullCatalyst)

    // serialize fails whether or not 'bar' is nullable
    val expectMsg = "Cannot find field 'foo.bar' (at position 0) in Avro schema at field 'foo'"
    assertFailedConversionMessage(avro, SERIALIZE, BY_NAME, expectMsg)
    assertFailedConversionMessage(avro, SERIALIZE, BY_NAME, expectMsg, nonnullCatalyst)
    assertFailedConversionMessage(avro, SERIALIZE, BY_POSITION,
      "Avro field 'foo' schema length (1) doesn't match SQL field 'foo' schema length (2)",
      extraNonnullCatalyst)
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val avro = SchemaBuilder.builder().record("toptest").fields()
        .name("top").`type`(createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar")))
        .noDefault().endRecord()
    val catalyst = new StructType().add("top", CATALYST_STRUCT)

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(avro, DESERIALIZE, fieldMatch,
        "Cannot convert Avro field 'top.foo.bar' to SQL field 'top.foo.bar' because schema " +
          """is incompatible (avroType = "float", sqlType = INT)""",
        catalyst)

      assertFailedConversionMessage(avro, SERIALIZE, fieldMatch,
        "Cannot convert SQL field 'top.foo.bar' to Avro field 'top.foo.bar' because schema is " +
          """incompatible (sqlType = INT, avroType = "float")""",
        catalyst)
    }
  }

  test("Fail to convert for serialization with field count mismatch") {
    // Note that this is allowed for deserialization, but not serialization
    withFieldMatchType { fieldMatch =>
      val tooManyFields =
        createAvroSchemaWithTopLevelFields(_.optionalInt("foo").optionalLong("bar"))
      assertFailedConversionMessage(tooManyFields, SERIALIZE, fieldMatch,
        "Avro top-level record schema length (2) " +
          "doesn't match SQL top-level record schema length (1)")

      val tooFewFields = createAvroSchemaWithTopLevelFields(f => f)
      assertFailedConversionMessage(tooFewFields, SERIALIZE, fieldMatch,
        "Avro top-level record schema length (0) " +
          "doesn't match SQL top-level record schema length (1)")
    }
  }

  /**
   * Attempt to convert `catalystSchema` to `avroSchema` (or vice-versa if `deserialize` is true),
   * assert that it fails, and assert that the _cause_ of the thrown exception has a message
   * matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(avroSchema: Schema,
      serdeType: SerdeType,
      fieldMatchType: FieldMatchType,
      expectedCauseMessage: String,
      catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      createSerde(catalystSchema, avroSchema, serdeType, fieldMatchType)
    }
    val expectMsg = serdeType match {
      case DESERIALIZE => s"Cannot convert Avro type $avroSchema to SQL type ${catalystSchema.sql}."
      case SERIALIZE => s"Cannot convert SQL type ${catalystSchema.sql} to Avro type $avroSchema."
    }
    assert(e.getMessage === expectMsg)
    assert(e.getCause.getMessage === expectedCauseMessage)
  }

  def withFieldMatchType(f: FieldMatchType => Unit): Unit = {
    FieldMatchType.values.foreach { fieldMatchType =>
      withClue(s"fieldMatchType == $fieldMatchType") {
        f(fieldMatchType)
      }
    }
  }
}


object AvroSerdeSuite {

  private val CATALYST_STRUCT =
    new StructType().add("foo", new StructType().add("bar", IntegerType))

  /**
   * Specifier for type of serde to be used for easy creation of tests that do both
   * serialization and deserialization.
   */
  object SerdeType extends Enumeration {
    type SerdeType = Value
    val SERIALIZE, DESERIALIZE = Value
  }
  import SerdeType._

  /**
   * Specifier for type of field matching to be used for easy creation of tests that do both
   * positional and by-name field matching.
   */
  object FieldMatchType extends Enumeration {
    type FieldMatchType = Value
    val BY_NAME, BY_POSITION = Value
  }
  import FieldMatchType._

  private def createSerde(
      catalystSchema: StructType,
      avroSchema: Schema,
      serdeType: SerdeType,
      fieldMatchType: FieldMatchType): Either[AvroSerializer, AvroDeserializer] = {
    val positional = fieldMatchType match {
      case BY_NAME => false
      case BY_POSITION => true
    }
    serdeType match {
      case SERIALIZE => Left(new AvroSerializer(catalystSchema, avroSchema, false, positional,
        LegacyBehaviorPolicy.CORRECTED))
      case DESERIALIZE => Right(new AvroDeserializer(avroSchema, catalystSchema, positional,
        LegacyBehaviorPolicy.CORRECTED, new NoopFilters))
    }
  }

  /**
   * Convenience method to create a top-level Avro schema with a single nested record
   * (at field name `nestedRecordFieldName`) which has fields as defined by those set
   * on the field assembler using `f`.
   */
  private def createNestedAvroSchemaWithFields(
      nestedRecordFieldName: String,
      f: SchemaBuilder.FieldAssembler[Schema] => SchemaBuilder.FieldAssembler[Schema]): Schema = {
    createAvroSchemaWithTopLevelFields(_.name(nestedRecordFieldName)
        .`type`(f(SchemaBuilder.builder().record("test").fields()).endRecord())
        .noDefault())
  }

  /**
   * Convenience method to create a top-level Avro schema with fields as defined by those set
   * on the field assembler using `f`.
   */
  private def createAvroSchemaWithTopLevelFields(
      f: SchemaBuilder.FieldAssembler[Schema] => SchemaBuilder.FieldAssembler[Schema]): Schema = {
    f(SchemaBuilder.builder().record("top").fields()).endRecord()
  }
}
