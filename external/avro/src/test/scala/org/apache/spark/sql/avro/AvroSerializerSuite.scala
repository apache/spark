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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{IntegerType, StructType}

/** Tests for [[AvroSerializer]], complementing those in [[AvroSuite]]. */
class AvroSerializerSuite extends SparkFunSuite {
  import AvroSerializerSuite._

  test("Test basic conversion") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalInt("bar"))
    new AvroSerializer(CATALYST_STRUCT, avro, false)
  }

  test("Fail to convert with field type mismatch") {
    val avro = createAvroSchemaWithTopLevelFields(_.requiredInt("foo"))
    assertFailedConversionMessage(avro,
      s"While attempting to convert Catalyst field 'foo' to Avro field 'foo': " +
        s"""Cannot convert Catalyst type ${CATALYST_STRUCT.head.dataType} to Avro type "int"""")
  }

  test("Fail to convert with nested field type mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar"))
    assertFailedConversionMessage(avro, "While attempting to convert Catalyst field 'foo.bar' to " +
      "Avro field 'foo.bar': Cannot convert Catalyst type IntegerType to Avro type \"float\"")
  }

  test("Fail to convert with nested field name mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalInt("NOTbar"))
    assertFailedConversionMessage(avro,
      "Searching for field 'foo.bar' in Avro schema gave 0 matches")
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val avro = SchemaBuilder.builder().record("toptest").fields()
      .name("top").`type`(createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar")))
      .noDefault().endRecord()

    assertFailedConversionMessage(avro, "While attempting to convert Catalyst field 'top.foo.bar'" +
      " to Avro field 'top.foo.bar': Cannot convert Catalyst type IntegerType to Avro type",
      new StructType().add("top", CATALYST_STRUCT))
  }

  test("Fail to convert with field count mismatch") {
    val tooManyFields = createAvroSchemaWithTopLevelFields(_.optionalInt("foo").optionalLong("bar"))
    assertFailedConversionMessage(tooManyFields, "Avro top-level record schema length (2) " +
      "doesn't match Catalyst top-level record schema length (1)")

    val tooFewFields = createAvroSchemaWithTopLevelFields(f => f)
    assertFailedConversionMessage(tooFewFields, "Avro top-level record schema length (0) " +
      "doesn't match Catalyst top-level record schema length (1)")
  }

  /**
   * Attempt to convert `catalystSchema` to `avroSchema`, assert that it fails, and assert
   * that the _cause_ of the thrown exception has a message matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(avroSchema: Schema,
      expectedCauseMessage: String,
      catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      new AvroSerializer(catalystSchema, avroSchema, false)
    }
    assert(e.getMessage ==
      s"Cannot convert Catalyst type $catalystSchema to Avro type $avroSchema.")
    assert(e.getCause.getMessage.contains(expectedCauseMessage))
  }
}

object AvroSerializerSuite {

  private val CATALYST_STRUCT = new StructType()
    .add("foo", new StructType().add("bar", IntegerType))

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