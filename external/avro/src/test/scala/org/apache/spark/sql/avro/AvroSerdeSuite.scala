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

import java.nio.ByteBuffer
import java.time.{Instant, ZoneId}

import scala.collection.JavaConverters._

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericFixed, GenericRecord, GenericRecordBuilder}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.CORRECTED
import org.apache.spark.sql.types._

/**
 * Tests for [[AvroSerializer]] and [[AvroDeserializer]], complementing those in [[AvroSuite]]
 * with a more specific focus on those classes.
 */
class AvroSerdeSuite extends SparkFunSuite {
  import AvroSerdeSuite._
  import AvroSerdeSuite.MatchType._

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
      val serializer = Serializer.create(CATALYST_STRUCT, avro, fieldMatch)
      val deserializer = Deserializer.create(CATALYST_STRUCT, avro, fieldMatch)
      assert(serializer.serialize(deserializer.deserialize(record).get) === record)
    }
  }

  test("Serialize DecimalType to Avro FIXED with logical type decimal") {
    withFieldMatchType { fieldMatch =>
      val structType = StructType(
        Seq(
          StructField("javaBigDecimal", DecimalType(6, 2), nullable = false),
          StructField("sparkDecimal", DecimalType(6, 2), nullable = false)))

      val fixedSchema = Schema.createFixed("fixed_name", "doc", "namespace", 32)
      val logicalType = LogicalTypes.decimal(6, 2)
      val fieldSchema = logicalType.addToSchema(fixedSchema)

      val avroSchema = Schema.createRecord(
        "name",
        "doc",
        "space",
        true,
        Seq(
          new Schema.Field("javaBigDecimal", fieldSchema, "", null.asInstanceOf[AnyVal]),
          new Schema.Field("sparkDecimal", fieldSchema, "", null.asInstanceOf[AnyVal])).asJava)

      val serializer = Serializer.create(structType, avroSchema, fieldMatch)

      val input = InternalRow(new java.math.BigDecimal("1000.12"), Decimal("1000.12"))

      val grec = serializer.serialize(input).asInstanceOf[GenericRecord]
      val javaDecimal = grec.get("javaBigDecimal").asInstanceOf[GenericFixed]
      val sparkDecimal = grec.get("sparkDecimal").asInstanceOf[GenericFixed]

      assert(javaDecimal === sparkDecimal)
      assert(
        new DecimalConversion().fromFixed(sparkDecimal, fixedSchema, logicalType) ===
          new java.math.BigDecimal("1000.12"))
    }
  }

  test("Serialize DecimalType to Avro BYTES with logical type decimal") {
    withFieldMatchType { fieldMatch =>
      val structType = StructType(
        Seq(
          StructField("javaBigDecimal", DecimalType(6, 2), nullable = false),
          StructField("sparkDecimal", DecimalType(6, 2), nullable = false)))

      val bytesSchema = Schema.create(BYTES)
      val logicalType = LogicalTypes.decimal(6, 2)
      val fieldSchema = logicalType.addToSchema(bytesSchema)

      val avroSchema = Schema.createRecord(
        "name",
        "doc",
        "space",
        true,
        Seq(
          new Schema.Field("javaBigDecimal", fieldSchema, "", null.asInstanceOf[AnyVal]),
          new Schema.Field("sparkDecimal", fieldSchema, "", null.asInstanceOf[AnyVal])).asJava)

      val serializer = Serializer.create(structType, avroSchema, fieldMatch)

      val input = InternalRow(new java.math.BigDecimal("1000.12"), Decimal("1000.12"))

      val grec = serializer.serialize(input).asInstanceOf[GenericRecord]
      val javaDecimal = grec.get("javaBigDecimal").asInstanceOf[ByteBuffer]
      val sparkDecimal = grec.get("sparkDecimal").asInstanceOf[ByteBuffer]

      assert(javaDecimal === sparkDecimal)
      assert(
        new DecimalConversion().fromBytes(sparkDecimal, bytesSchema, logicalType) ===
          new java.math.BigDecimal("1000.12"))
    }
  }

  test("Serialize DateType to Avro INT") {
    withFieldMatchType { fieldMatch =>
      val structType = StructType(
        Seq(
          StructField("javaSqlDate", DateType, nullable = false),
          StructField("java8TimeDate", DateType, nullable = false)))

      val dateSchema = Schema.create(INT)

      val avroSchema = Schema.createRecord(
        "name",
        "doc",
        "space",
        true,
        Seq(
          new Schema.Field("javaSqlDate", dateSchema, "", null.asInstanceOf[AnyVal]),
          new Schema.Field("java8TimeDate", dateSchema, "", null.asInstanceOf[AnyVal])).asJava)

      val serializer = Serializer.create(structType, avroSchema, fieldMatch)

      val input = InternalRow(
        new java.sql.Date(1643121231000L),
        Instant.ofEpochMilli(1643121231000L).atZone(ZoneId.of("UTC")).toLocalDate())

      val grec = serializer.serialize(input).asInstanceOf[GenericRecord]
      val javaSqlDate = grec.get("javaSqlDate").asInstanceOf[Int]
      val java8TimeDate = grec.get("java8TimeDate").asInstanceOf[Int]

      assert(javaSqlDate === java8TimeDate)
      assert(javaSqlDate === 19017) // 19017 is 25 January 2022
    }
  }

  test(s"""
        |Serialize TimestampType to Avro LONG with logical type
        | timestamp-micros and timestamp-millis
        """.stripMargin) {
    withFieldMatchType { fieldMatch =>
      val structType = StructType(
        Seq(
          StructField("javaSqlTimeMicro", TimestampType, nullable = false),
          StructField("java8TimeInstantMicro", TimestampType, nullable = false),
          StructField("javaSqlTimeMillis", TimestampType, nullable = false),
          StructField("java8TimeInstantMillis", TimestampType, nullable = false)))

      val microSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(LONG))

      val millisSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(LONG))

      val avroSchema = Schema.createRecord(
        "name",
        "doc",
        "space",
        true,
        Seq(
          new Schema.Field("javaSqlTimeMicro", microSchema, "", null.asInstanceOf[AnyVal]),
          new Schema.Field("java8TimeInstantMicro", microSchema, "", null.asInstanceOf[AnyVal]),
          new Schema.Field("javaSqlTimeMillis", millisSchema, "", null.asInstanceOf[AnyVal]),
          new Schema.Field(
            "java8TimeInstantMillis",
            millisSchema,
            "",
            null.asInstanceOf[AnyVal])).asJava)

      val serializer = Serializer.create(structType, avroSchema, fieldMatch)

      val epoch = 1643121231000L
      val epochMicro = 1000 * 1643121231000L

      val input = InternalRow(
        new java.sql.Timestamp(epoch),
        Instant.ofEpochMilli(epoch),
        new java.sql.Timestamp(epoch),
        Instant.ofEpochMilli(epoch))

      val grec = serializer.serialize(input).asInstanceOf[GenericRecord]

      assert(grec.get("javaSqlTimeMicro").asInstanceOf[Long] === epochMicro)
      assert(grec.get("java8TimeInstantMicro").asInstanceOf[Long] === epochMicro)
      assert(grec.get("javaSqlTimeMillis").asInstanceOf[Long] === epoch)
      assert(grec.get("java8TimeInstantMillis").asInstanceOf[Long] === epoch)
    }
  }

  test("Fail to convert with field type mismatch") {
    val avro = createAvroSchemaWithTopLevelFields(_.requiredInt("foo"))
    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(
        avro,
        Deserializer,
        fieldMatch,
        "Cannot convert Avro field 'foo' to SQL field 'foo' because schema is incompatible " +
          s"""(avroType = "int", sqlType = ${CATALYST_STRUCT.head.dataType.sql})""")

      assertFailedConversionMessage(
        avro,
        Serializer,
        fieldMatch,
        s"Cannot convert SQL field 'foo' to Avro field 'foo' because schema is incompatible " +
          s"""(sqlType = ${CATALYST_STRUCT.head.dataType.sql}, avroType = "int")""")
    }
  }

  test("Fail to convert with nested field type mismatch") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar"))

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(
        avro,
        Deserializer,
        fieldMatch,
        "Cannot convert Avro field 'foo.bar' to SQL field 'foo.bar' because schema is " +
          """incompatible (avroType = "float", sqlType = INT)""")

      assertFailedConversionMessage(
        avro,
        Serializer,
        fieldMatch,
        "Cannot convert SQL field 'foo.bar' to Avro field 'foo.bar' because " +
          """schema is incompatible (sqlType = INT, avroType = "float")""")
    }
  }

  test("Fail to convert with missing nested Avro fields") {
    val avro = createNestedAvroSchemaWithFields("foo", _.optionalInt("NOTbar"))
    val nonnullCatalyst = new StructType()
      .add("foo", new StructType().add("bar", IntegerType, nullable = false))
    // Positional matching will work fine with the name change, so add a new field
    val extraNonnullCatalyst = new StructType().add(
      "foo",
      new StructType().add("bar", IntegerType).add("baz", IntegerType, nullable = false))

    // deserialize should have no issues when 'bar' is nullable but fail when it is nonnull
    Deserializer.create(CATALYST_STRUCT, avro, BY_NAME)
    assertFailedConversionMessage(
      avro,
      Deserializer,
      BY_NAME,
      "Cannot find field 'foo.bar' in Avro schema",
      nonnullCatalyst)
    assertFailedConversionMessage(
      avro,
      Deserializer,
      BY_POSITION,
      "Cannot find field at position 1 of field 'foo' from Avro schema (using positional matching)",
      extraNonnullCatalyst)

    // serialize fails whether or not 'bar' is nullable
    val byNameMsg = "Cannot find field 'foo.bar' in Avro schema"
    assertFailedConversionMessage(avro, Serializer, BY_NAME, byNameMsg)
    assertFailedConversionMessage(avro, Serializer, BY_NAME, byNameMsg, nonnullCatalyst)
    assertFailedConversionMessage(
      avro,
      Serializer,
      BY_POSITION,
      "Cannot find field at position 1 of field 'foo' from Avro schema (using positional matching)",
      extraNonnullCatalyst)
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val avro = SchemaBuilder
      .builder()
      .record("toptest")
      .fields()
      .name("top")
      .`type`(createNestedAvroSchemaWithFields("foo", _.optionalFloat("bar")))
      .noDefault()
      .endRecord()
    val catalyst = new StructType().add("top", CATALYST_STRUCT)

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(
        avro,
        Deserializer,
        fieldMatch,
        "Cannot convert Avro field 'top.foo.bar' to SQL field 'top.foo.bar' because schema " +
          """is incompatible (avroType = "float", sqlType = INT)""",
        catalyst)

      assertFailedConversionMessage(
        avro,
        Serializer,
        fieldMatch,
        "Cannot convert SQL field 'top.foo.bar' to Avro field 'top.foo.bar' because schema is " +
          """incompatible (sqlType = INT, avroType = "float")""",
        catalyst)
    }
  }

  test("Fail to convert for serialization with field count mismatch") {
    // Note that this is allowed for deserialization, but not serialization
    val tooManyFields =
      createAvroSchemaWithTopLevelFields(_.optionalInt("foo").optionalLong("bar"))
    assertFailedConversionMessage(
      tooManyFields,
      Serializer,
      BY_NAME,
      "Found field 'bar' in Avro schema but there is no match in the SQL schema")
    assertFailedConversionMessage(
      tooManyFields,
      Serializer,
      BY_POSITION,
      "Found field 'bar' at position 1 of top-level record from Avro schema but there is no " +
        "match in the SQL schema at top-level record (using positional matching)")

    val tooManyFieldsNested =
      createNestedAvroSchemaWithFields("foo", _.optionalInt("bar").optionalInt("baz"))
    assertFailedConversionMessage(
      tooManyFieldsNested,
      Serializer,
      BY_NAME,
      "Found field 'foo.baz' in Avro schema but there is no match in the SQL schema")
    assertFailedConversionMessage(
      tooManyFieldsNested,
      Serializer,
      BY_POSITION,
      s"Found field 'baz' at position 1 of field 'foo' from Avro schema but there is no match " +
        s"in the SQL schema at field 'foo' (using positional matching)")

    val tooFewFields = createAvroSchemaWithTopLevelFields(f => f)
    assertFailedConversionMessage(
      tooFewFields,
      Serializer,
      BY_NAME,
      "Cannot find field 'foo' in Avro schema")
    assertFailedConversionMessage(
      tooFewFields,
      Serializer,
      BY_POSITION,
      "Cannot find field at position 0 of top-level record from Avro schema " +
        "(using positional matching)")
  }

  /**
   * Attempt to convert `catalystSchema` to `avroSchema` (or vice-versa if `deserialize` is true),
   * assert that it fails, and assert that the _cause_ of the thrown exception has a message
   * matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(
      avroSchema: Schema,
      serdeFactory: SerdeFactory[_],
      fieldMatchType: MatchType,
      expectedCauseMessage: String,
      catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      serdeFactory.create(catalystSchema, avroSchema, fieldMatchType)
    }
    val expectMsg = serdeFactory match {
      case Deserializer =>
        s"Cannot convert Avro type $avroSchema to SQL type ${catalystSchema.sql}."
      case Serializer =>
        s"Cannot convert SQL type ${catalystSchema.sql} to Avro type $avroSchema."
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

object AvroSerdeSuite {

  private val CATALYST_STRUCT =
    new StructType().add("foo", new StructType().add("bar", IntegerType))

  /**
   * Specifier for type of field matching to be used for easy creation of tests that do both
   * positional and by-name field matching.
   */
  private object MatchType extends Enumeration {
    type MatchType = Value
    val BY_NAME, BY_POSITION = Value

    def isPositional(fieldMatchType: MatchType): Boolean = fieldMatchType == BY_POSITION
  }

  import MatchType._

  /**
   * Specifier for type of serde to be used for easy creation of tests that do both
   * serialization and deserialization.
   */
  private sealed trait SerdeFactory[T] {
    def create(sqlSchema: StructType, avroSchema: Schema, fieldMatchType: MatchType): T
  }

  private object Serializer extends SerdeFactory[AvroSerializer] {

    override def create(sql: StructType, avro: Schema, matchType: MatchType): AvroSerializer =
      new AvroSerializer(sql, avro, false, isPositional(matchType), CORRECTED)

  }

  private object Deserializer extends SerdeFactory[AvroDeserializer] {

    override def create(sql: StructType, avro: Schema, matchType: MatchType): AvroDeserializer =
      new AvroDeserializer(
        avro,
        sql,
        isPositional(matchType),
        RebaseSpec(CORRECTED),
        new NoopFilters)

  }

  /**
   * Convenience method to create a top-level Avro schema with a single nested record
   * (at field name `nestedRecordFieldName`) which has fields as defined by those set
   * on the field assembler using `f`.
   */
  private def createNestedAvroSchemaWithFields(
      nestedRecordFieldName: String,
      f: SchemaBuilder.FieldAssembler[Schema] => SchemaBuilder.FieldAssembler[Schema]): Schema = {
    createAvroSchemaWithTopLevelFields(
      _.name(nestedRecordFieldName)
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
