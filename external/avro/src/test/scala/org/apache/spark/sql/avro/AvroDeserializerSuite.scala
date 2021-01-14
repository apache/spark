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

/** Tests for [[AvroDeserializer]], complementing those in [[AvroSuite]]. */
class AvroDeserializerSuite extends SparkFunSuite {
  import AvroDeserializerSuite._

  test("Fail to convert with field type mismatch") {
    val avro = createAvroSchemaWithTopLevelFields(_.requiredInt("foo"))
    assertFailedConversionMessage(avro,
      "Cannot convert Avro to Catalyst because schema of field 'foo' is not compatible " +
          s"""(avroType = "int", sqlType = ${CATALYST_STRUCT.head.dataType})""")
  }

  test("Fail to convert with nested field type mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar"))
    assertFailedConversionMessage(avro,
      "Cannot convert Avro to Catalyst because schema of field 'foo.bar' is not compatible " +
          """(avroType = "float", sqlType = IntegerType)""")
  }

  test("Fail to convert with missing nested field only if non-nullable") {
    val avro = createNestedAvroSchemaWithFields("foo", _.requiredInt("NOTbar"))
    assertFailedConversionMessage(avro,
      "Cannot find non-nullable field 'foo.bar' in Avro schema.",
      new StructType().add("foo", new StructType().add("bar", IntegerType, nullable = false)))
    new AvroDeserializer(avro, CATALYST_STRUCT) // should be no issue when 'bar' is nullable
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val avro = SchemaBuilder.builder().record("toptest").fields()
      .name("top").`type`(createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar")))
      .noDefault().endRecord()

    assertFailedConversionMessage(avro, "Cannot convert Avro to Catalyst because schema of field " +
        """'top.foo.bar' is not compatible (avroType = "float", sqlType = IntegerType)""",
      new StructType().add("top", CATALYST_STRUCT))
  }

  /**
   * Attempt to convert `avroSchema` to `catalystSchema`, assert that it fails, and assert
   * that the _cause_ of the thrown exception has a message matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(avroSchema: Schema,
      expectedCauseMessage: String,
      catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      new AvroDeserializer(avroSchema, catalystSchema)
    }
    assert(e.getMessage ==
      s"Cannot convert Avro type $avroSchema to Catalyst type $catalystSchema.")
    assert(e.getCause.getMessage.contains(expectedCauseMessage))
  }
}

object AvroDeserializerSuite {

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