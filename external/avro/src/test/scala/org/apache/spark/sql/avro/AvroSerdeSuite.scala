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
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Tests for [[AvroSerializer]] and [[AvroDeserializer]], complementing those in [[AvroSuite]]
 * with a more specific focus on those classes.
 */
class AvroSerdeSuite extends SparkFunSuite {
  import AvroSerdeSuite._

  private val defaultRebaseMode = LegacyBehaviorPolicy.CORRECTED.toString

  test("Test basic conversion") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalInt("bar"))
    val record = new GenericRecordBuilder(avro)
        .set("foo", new GenericRecordBuilder(avro.getField("foo").schema()).set("bar", 42).build())
        .build()
    val serializer = new AvroSerializer(CATALYST_STRUCT, avro, false)
    val deserializer = new AvroDeserializer(avro, CATALYST_STRUCT, defaultRebaseMode)
    assert(serializer.serialize(deserializer.deserialize(record).get) === record)
  }

  test("Fail to convert with field type mismatch") {
    val avro = createAvroSchemaWithTopLevelFields(_.requiredInt("foo"))

    assertFailedConversionMessage(avro, deserialize = true,
      "Cannot convert Avro field 'foo' to SQL field 'foo' because schema is incompatible " +
          s"""(avroType = "int", sqlType = ${CATALYST_STRUCT.head.dataType.sql})""")

    assertFailedConversionMessage(avro, deserialize = false,
      s"Cannot convert SQL field 'foo' to Avro field 'foo' because schema is incompatible " +
          s"""(sqlType = ${CATALYST_STRUCT.head.dataType.sql}, avroType = "int")""")
  }

  test("Fail to convert with nested field type mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar"))

    assertFailedConversionMessage(avro, deserialize = true,
      "Cannot convert Avro field 'foo.bar' to SQL field 'foo.bar' because schema is " +
          """incompatible (avroType = "float", sqlType = INT)""")

    assertFailedConversionMessage(avro, deserialize = false,
      "Cannot convert SQL field 'foo.bar' to Avro field 'foo.bar' because " +
        """schema is incompatible (sqlType = INT, avroType = "float")""")
  }

  test("Fail to convert with nested field name mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalInt("NOTbar"))
    val nonnullCatalyst = new StructType()
        .add("foo", new StructType().add("bar", IntegerType, nullable = false))

    // deserialize should have no issues when 'bar' is nullable but fail when it is nonnull
    new AvroDeserializer(avro, CATALYST_STRUCT, defaultRebaseMode)
    assertFailedConversionMessage(avro, deserialize = true,
      "Cannot find non-nullable field 'foo.bar' in Avro schema.",
      nonnullCatalyst)

    // serialize fails whether or not 'bar' is nullable
    val expectMsg = "Cannot find field 'foo.bar' in Avro schema at field 'foo'"
    assertFailedConversionMessage(avro, deserialize = false, expectMsg)
    assertFailedConversionMessage(avro, deserialize = false, expectMsg, nonnullCatalyst)
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val avro = SchemaBuilder.builder().record("toptest").fields()
        .name("top").`type`(createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar")))
        .noDefault().endRecord()
    val catalyst = new StructType().add("top", CATALYST_STRUCT)

    assertFailedConversionMessage(avro, deserialize = true,
      "Cannot convert Avro field 'top.foo.bar' to SQL field 'top.foo.bar' because schema " +
        """is incompatible (avroType = "float", sqlType = INT)""",
      catalyst)

    assertFailedConversionMessage(avro, deserialize = false,
      "Cannot convert SQL field 'top.foo.bar' to Avro field 'top.foo.bar' because schema is " +
          """incompatible (sqlType = INT, avroType = "float")""",
      catalyst)
  }

  test("Fail to convert for serialization with field count mismatch") {
    val tooManyFields = createAvroSchemaWithTopLevelFields(_.optionalInt("foo").optionalLong("bar"))
    assertFailedConversionMessage(tooManyFields, deserialize = false,
      "Avro top-level record schema length (2) " +
        "doesn't match SQL top-level record schema length (1)")

    val tooFewFields = createAvroSchemaWithTopLevelFields(f => f)
    assertFailedConversionMessage(tooFewFields, deserialize = false,
      "Avro top-level record schema length (0) " +
        "doesn't match SQL top-level record schema length (1)")
  }

  /**
   * Attempt to convert `catalystSchema` to `avroSchema` (or vice-versa if `deserialize` is true),
   * assert that it fails, and assert that the _cause_ of the thrown exception has a message
   * matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(avroSchema: Schema,
      deserialize: Boolean,
      expectedCauseMessage: String,
      catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      if (deserialize) {
        new AvroDeserializer(avroSchema, catalystSchema, defaultRebaseMode)
      } else {
        new AvroSerializer(catalystSchema, avroSchema, false)
      }
    }
    val expectMsg = if (deserialize) {
      s"Cannot convert Avro type $avroSchema to SQL type ${catalystSchema.sql}."
    } else {
      s"Cannot convert SQL type ${catalystSchema.sql} to Avro type $avroSchema."
    }
    assert(e.getMessage === expectMsg)
    assert(e.getCause.getMessage === expectedCauseMessage)
  }
}


object AvroSerdeSuite {

  private val CATALYST_STRUCT =
    new StructType().add("foo", new StructType().add("bar", IntegerType))

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
