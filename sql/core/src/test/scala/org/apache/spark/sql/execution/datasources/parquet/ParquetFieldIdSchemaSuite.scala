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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters._

import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ParquetFieldIdSchemaSuite extends ParquetSchemaTest {

  private val FAKE_COLUMN_NAME = "_fake_name_"
  private val UUID_REGEX =
    "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}".r

  private def withId(id: Int) =
    new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()

  private def testSchemaClipping(
      testName: String,
      parquetSchema: String,
      catalystSchema: StructType,
      expectedSchema: String,
      caseSensitive: Boolean = true,
      useFieldId: Boolean = true): Unit = {
    test(s"Clipping with field id - $testName") {
      val fileSchema = MessageTypeParser.parseMessageType(parquetSchema)
      val actual = ParquetReadSupport.clipParquetSchema(
        fileSchema,
        catalystSchema,
        caseSensitive = caseSensitive,
        useFieldId = useFieldId)

      // each fake name should be uniquely generated
      val fakeColumnNames = actual.getPaths.asScala.flatten.filter(_.startsWith(FAKE_COLUMN_NAME))
      assert(
        fakeColumnNames.distinct == fakeColumnNames, "Should generate unique fake column names")

      // replace the random part of all fake names with a fixed id generator
      val ids1 = (1 to 100).iterator
      val actualNormalized = MessageTypeParser.parseMessageType(
        UUID_REGEX.replaceAllIn(actual.toString, _ => ids1.next().toString)
      )
      val ids2 = (1 to 100).iterator
      val expectedNormalized = MessageTypeParser.parseMessageType(
        FAKE_COLUMN_NAME.r.replaceAllIn(expectedSchema, _ => s"$FAKE_COLUMN_NAME${ids2.next()}")
      )

      try {
        expectedNormalized.checkContains(actualNormalized)
        actualNormalized.checkContains(expectedNormalized)
      } catch { case cause: Throwable =>
        fail(
          s"""Expected clipped schema:
             |$expectedSchema
             |Actual clipped schema:
             |$actual
           """.stripMargin,
          cause)
      }
      checkEqual(actualNormalized, expectedNormalized)
      // might be redundant but just to have some free tests for the utils
      assert(ParquetReadSupport.containsFieldIds(fileSchema))
      assert(ParquetUtils.hasFieldIds(catalystSchema))
    }
  }

  private def testSqlToParquet(
    testName: String,
    sqlSchema: StructType,
    parquetSchema: String): Unit = {
    val converter = new SparkToParquetSchemaConverter(
      writeLegacyParquetFormat = false,
      outputTimestampType = SQLConf.ParquetOutputTimestampType.INT96,
      useFieldId = true)

    test(s"sql => parquet: $testName") {
      val actual = converter.convert(sqlSchema)
      val expected = MessageTypeParser.parseMessageType(parquetSchema)
      checkEqual(actual, expected)
    }
  }

  private def checkEqual(actual: MessageType, expected: MessageType): Unit = {
    actual.checkContains(expected)
    expected.checkContains(actual)
    assert(actual.toString == expected.toString,
      s"""
         |Schema mismatch.
         |Expected schema:
         |${expected.toString}
         |Actual schema:
         |${actual.toString}
         """.stripMargin
    )
  }

  test("check hasFieldIds for schema") {
    val simpleSchemaMissingId = new StructType()
      .add("f010", DoubleType, nullable = true, withId(7))
      .add("f012", LongType, nullable = true)

    assert(ParquetUtils.hasFieldIds(simpleSchemaMissingId))

    val f01ElementType = new StructType()
      .add("f010", DoubleType, nullable = true, withId(7))
      .add("f012", LongType, nullable = true, withId(8))

    assert(ParquetUtils.hasFieldIds(f01ElementType))

    val f0Type = new StructType()
      .add("f00", ArrayType(StringType, containsNull = false), nullable = true, withId(2))
      .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

    assert(ParquetUtils.hasFieldIds(f0Type))

    assert(ParquetUtils.hasFieldIds(
      new StructType().add("f0", f0Type, nullable = false, withId(1))))

    assert(!ParquetUtils.hasFieldIds(new StructType().add("f0", IntegerType, nullable = true)))
    assert(!ParquetUtils.hasFieldIds(new StructType()));
  }

  test("check getFieldId for schema") {
    val schema = new StructType()
      .add("overflowId", DoubleType, nullable = true,
        new MetadataBuilder()
          .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 12345678987654321L).build())
      .add("stringId", StringType, nullable = true,
        new MetadataBuilder()
          .putString(ParquetUtils.FIELD_ID_METADATA_KEY, "lol").build())
      .add("negativeId", LongType, nullable = true, withId(-20))
      .add("noId", LongType, nullable = true)

    assert(intercept[IllegalArgumentException] {
      ParquetUtils.getFieldId(schema.findNestedField(Seq("noId")).get._2)
    }.getMessage.contains("doesn't exist"))

    assert(intercept[IllegalArgumentException] {
      ParquetUtils.getFieldId(schema.findNestedField(Seq("overflowId")).get._2)
    }.getMessage.contains("must be a 32-bit integer"))

    assert(intercept[IllegalArgumentException] {
      ParquetUtils.getFieldId(schema.findNestedField(Seq("stringId")).get._2)
    }.getMessage.contains("must be a 32-bit integer"))

    // negative id allowed
    assert(ParquetUtils.getFieldId(schema.findNestedField(Seq("negativeId")).get._2) == -20)
  }

  test("check containsFieldIds for parquet schema") {

    // empty Parquet schema fails too
    assert(
      !ParquetReadSupport.containsFieldIds(
        MessageTypeParser.parseMessageType(
          """message root {
             |}
          """.stripMargin)))

    assert(
      !ParquetReadSupport.containsFieldIds(
        MessageTypeParser.parseMessageType(
          """message root {
            |  required group f0 {
            |    optional int32 f00;
            |  }
            |}
          """.stripMargin)))

    assert(
      ParquetReadSupport.containsFieldIds(
        MessageTypeParser.parseMessageType(
          """message root {
            |  required group f0 = 1 {
            |    optional int32 f00;
            |    optional binary f01;
            |  }
            |}
          """.stripMargin)))

    assert(
      ParquetReadSupport.containsFieldIds(
        MessageTypeParser.parseMessageType(
          """message root {
            |  required group f0 {
            |    optional int32 f00 = 1;
            |    optional binary f01;
            |  }
            |}
          """.stripMargin)))

    assert(
      !ParquetReadSupport.containsFieldIds(
        MessageTypeParser.parseMessageType(
          """message spark_schema {
              |  required group f0 {
              |    optional group f00 (LIST) {
              |      repeated group list {
              |        required binary element (UTF8);
              |      }
              |    }
              |  }
              |}
            """.stripMargin)))

    assert(
      ParquetReadSupport.containsFieldIds(
        MessageTypeParser.parseMessageType(
          """message spark_schema {
            |  required group f0 {
            |    optional group f00 (LIST) {
            |      repeated group list = 1 {
            |        required binary element (UTF8);
            |      }
            |    }
            |  }
            |}
            """.stripMargin)))
  }

  test("ID in Parquet Types is read as null when not set") {
    val parquetSchemaString =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |  }
        |}
      """.stripMargin

    val parquetSchema = MessageTypeParser.parseMessageType(parquetSchemaString)
    val f0 = parquetSchema.getFields().get(0)
    assert(f0.getId() == null)
    assert(f0.asGroupType().getFields.get(0).getId == null)
  }

  testSqlToParquet(
    "standard array",
    sqlSchema = {
      val f01ElementType = new StructType()
        .add("f010", DoubleType, nullable = true, withId(7))
        .add("f012", LongType, nullable = true, withId(9))

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true, withId(2))
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true, withId(5))

      new StructType().add("f0", f0Type, nullable = false, withId(1))
    },
    parquetSchema =
      """message spark_schema {
        |  required group f0 = 1 {
        |    optional group f00 (LIST) = 2 {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) = 5 {
        |      repeated group list {
        |        required group element {
        |          optional double f010 = 7;
        |          optional int64 f012 = 9;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "simple nested struct",

    parquetSchema =
      """message root {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |    optional int32 f01 = 3;
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType().add(
        "g00", IntegerType, nullable = true, withId(2))
      new StructType()
        .add("g0", f0Type, nullable = false, withId(1))
        .add("g1", IntegerType, nullable = true, withId(4))
    },

    expectedSchema =
      s"""message spark_schema {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |  optional int32 $FAKE_COLUMN_NAME = 4;
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard array",

    parquetSchema =
      """message root {
        |  required group f0 = 1 {
        |    optional group f00 (LIST) = 2 {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) = 5 {
        |      repeated group list {
        |        required group element {
        |          optional int32 f010 = 7;
        |          optional double f011 = 8;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("g011", DoubleType, nullable = true, withId(8))
        .add("g012", LongType, nullable = true, withId(9))

      val f0Type = new StructType()
        .add("g00", ArrayType(StringType, containsNull = false), nullable = true, withId(2))
        .add("g01", ArrayType(f01ElementType, containsNull = false), nullable = true, withId(5))

      new StructType().add("g0", f0Type, nullable = false, withId(1))
    },

    expectedSchema =
      s"""message spark_schema {
        |  required group f0 = 1 {
        |    optional group f00 (LIST) = 2 {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) = 5 {
        |      repeated group list {
        |        required group element {
        |          optional double f011 = 8;
        |          optional int64 $FAKE_COLUMN_NAME = 9;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard map with complex key",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) = 3 {
        |    repeated group key_value = 1 {
        |      required group key = 2 {
        |        required int32 value_f0 = 4;
        |        required int64 value_f1 = 6;
        |      }
        |      required int32 value = 5;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val keyType =
        new StructType()
          .add("value_g1", LongType, nullable = false, withId(6))
          .add("value_g2", DoubleType, nullable = false, withId(7))

      val f0Type = MapType(keyType, IntegerType, valueContainsNull = false)

      new StructType().add("g0", f0Type, nullable = false, withId(3))
    },

    expectedSchema =
      s"""message spark_schema {
        |  required group f0 (MAP) = 3 {
        |    repeated group key_value = 1 {
        |      required group key = 2 {
        |        required int64 value_f1 = 6;
        |        required double $FAKE_COLUMN_NAME = 7;
        |      }
        |      required int32 value = 5;
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "won't match field id if structure is different",

    parquetSchema =
      """message root {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |  optional int32 f1 = 3;
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType()
        .add("g00", IntegerType, nullable = true, withId(2))
        // parquet has id 3, but won't use because structure is different
        .add("g01", IntegerType, nullable = true, withId(3))
      new StructType()
        .add("g0", f0Type, nullable = false, withId(1))
    },

    // note that f1 is not picked up, even though it's Id is 3
    expectedSchema =
      s"""message spark_schema {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |    optional int32 $FAKE_COLUMN_NAME = 3;
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "Complex type with multiple mismatches should work",

    parquetSchema =
      """message root {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |  optional int32 f1 = 3;
        |  optional int32 f2 = 4;
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType()
        .add("g00", IntegerType, nullable = true, withId(2))

      new StructType()
        .add("g0", f0Type, nullable = false, withId(999))
        .add("g1", IntegerType, nullable = true, withId(3))
        .add("g2", IntegerType, nullable = true, withId(888))
    },

    expectedSchema =
      s"""message spark_schema {
        |  required group $FAKE_COLUMN_NAME = 999 {
        |    optional int32 g00 = 2;
        |  }
        |  optional int32 f1 = 3;
        |  optional int32 $FAKE_COLUMN_NAME = 888;
        |}
      """.stripMargin)

  testSchemaClipping(
    "Should allow fall-back to name matching if id not found",

    parquetSchema =
      """message root {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |  optional int32 f1 = 3;
        |  optional int32 f2 = 4;
        |  required group f4 = 5 {
        |    optional int32 f40 = 6;
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType()
        // nested f00 without id should also work
        .add("f00", IntegerType, nullable = true)

      val f4Type = new StructType()
        .add("g40", IntegerType, nullable = true, withId(6))

      new StructType()
        .add("g0", f0Type, nullable = false, withId(1))
        .add("g1", IntegerType, nullable = true, withId(3))
        // f2 without id should be matched using name matching
        .add("f2", IntegerType, nullable = true)
        // name is not matched
        .add("g2", IntegerType, nullable = true)
        // f4 without id will do name matching, but g40 will be matched using id
        .add("f4", f4Type, nullable = true)
    },

    expectedSchema =
      s"""message spark_schema {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |  optional int32 f1 = 3;
        |  optional int32 f2 = 4;
        |  optional int32 g2;
        |  required group f4 = 5 {
        |    optional int32 f40 = 6;
        |  }
        |}
      """.stripMargin)
}
