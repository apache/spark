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

import org.apache.avro.SchemaBuilder

import org.apache.spark.sql.avro.AvroUtils.AvroMatchedField
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class AvroSchemaHelperSuite extends SQLTestUtils with SharedSparkSession {

  test("ensure schema is a record") {
    val avroSchema = SchemaBuilder.builder().intType()

    val msg = intercept[IncompatibleSchemaException] {
      new AvroUtils.AvroSchemaHelper(avroSchema, StructType(Seq()), Seq(""), Seq(""), false)
    }.getMessage
    assert(msg.contains("Attempting to treat int as a RECORD"))
  }

  test("handle mixed case field names") {
    val catalystSchema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", IntegerType) ::
      StructField("A", IntegerType) ::
      Nil
    )

    val avroSchema = SchemaConverters.toAvroType(catalystSchema)
    val helper =
      new AvroUtils.AvroSchemaHelper(avroSchema, StructType(Seq()), Seq(""), Seq(""), false)
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      assert(helper.getFieldByName("A").get.name() == "A")
      assert(helper.getFieldByName("a").get.name() == "a")
      assert(helper.getFieldByName("b").get.name() == "b")
      assert(helper.getFieldByName("B").isEmpty)
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      Seq("a", "A").foreach { fieldName =>
        withClue(s"looking for field name: $fieldName") {
          val msg = intercept[IncompatibleSchemaException] {
            helper.getFieldByName(fieldName)
          }.getMessage
          assert(msg.contains(s"Searching for '$fieldName' in Avro schema"))
        }
      }

      assert(helper.getFieldByName("b").get.name() == "b")
      assert(helper.getFieldByName("B").get.name() == "b")
    }
  }

  test("change field match strategy based on positionalFieldMatch value") {
    val catalystSchema = new StructType().add("foo", IntegerType).add("bar", StringType)
    val avroSchema = SchemaConverters.toAvroType(catalystSchema)

    val posHelper =
      new AvroUtils.AvroSchemaHelper(avroSchema, catalystSchema, Seq(""), Seq(""), true)
    val nameHelper =
      new AvroUtils.AvroSchemaHelper(avroSchema, catalystSchema, Seq(""), Seq(""), false)

    for (name <- Seq("foo", "bar"); fieldPos <- Seq(0, 1)) {
      assert(posHelper.getAvroField(name, fieldPos) === Some(avroSchema.getFields.get(fieldPos)))
      assert(nameHelper.getAvroField(name, fieldPos) === Some(avroSchema.getField(name)))
    }
    assert(posHelper.getAvroField("foo", 5).isEmpty)
    assert(nameHelper.getAvroField("foo", 5).isDefined)

    assert(posHelper.getAvroField("nonexist", 1).isDefined)
    assert(nameHelper.getAvroField("nonexist", 1).isEmpty)
  }

  test("properly match fields between Avro and Catalyst schemas") {
    val catalystSchema = StructType(
      Seq("catalyst1", "catalyst2", "shared1", "shared2").map(StructField(_, IntegerType))
    )
    val avroSchema = SchemaBuilder.record("toplevel").fields()
      .requiredInt("shared1")
      .requiredInt("shared2")
      .requiredInt("avro1")
      .requiredInt("avro2")
      .endRecord()

    val helper = new AvroUtils.AvroSchemaHelper(avroSchema, catalystSchema, Seq(""), Seq(""), false)
    assert(helper.matchedFields === Seq(
      AvroMatchedField(catalystSchema("shared1"), 2, avroSchema.getField("shared1")),
      AvroMatchedField(catalystSchema("shared2"), 3, avroSchema.getField("shared2"))
    ))
    assertThrows[IncompatibleSchemaException] {
      helper.validateNoExtraAvroFields()
    }
    helper.validateNoExtraCatalystFields(ignoreNullable = true)
    assertThrows[IncompatibleSchemaException] {
      helper.validateNoExtraCatalystFields(ignoreNullable = false)
    }
  }

  test("respect nullability settings for validateNoExtraSqlFields") {
    val avroSchema = SchemaBuilder.record("record").fields().requiredInt("bar").endRecord()

    val catalystNonnull = new StructType().add("foo", IntegerType, nullable = false)
    val helperNonnull =
      new AvroUtils.AvroSchemaHelper(avroSchema, catalystNonnull, Seq(""), Seq(""), false)
    assertThrows[IncompatibleSchemaException] {
      helperNonnull.validateNoExtraCatalystFields(ignoreNullable = true)
    }
    assertThrows[IncompatibleSchemaException] {
      helperNonnull.validateNoExtraCatalystFields(ignoreNullable = false)
    }

    val catalystNullable = new StructType().add("foo", IntegerType)
    val helperNullable =
      new AvroUtils.AvroSchemaHelper(avroSchema, catalystNullable, Seq(""), Seq(""), false)
    helperNullable.validateNoExtraCatalystFields(ignoreNullable = true)
    assertThrows[IncompatibleSchemaException] {
      helperNullable.validateNoExtraCatalystFields(ignoreNullable = false)
    }
  }
}
