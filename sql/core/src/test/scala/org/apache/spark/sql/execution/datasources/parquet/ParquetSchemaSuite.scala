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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class ParquetSchemaTest extends ParquetTest with SharedSparkSession {

  /**
   * Checks whether the reflected Parquet message type for product type `T` conforms `messageType`.
   */
  protected def testSchemaInference[T <: Product: ClassTag: TypeTag](
      testName: String,
      messageType: String,
      binaryAsString: Boolean,
      int96AsTimestamp: Boolean,
      writeLegacyParquetFormat: Boolean,
      expectedParquetColumn: Option[ParquetColumn] = None): Unit = {
    testSchema(
      testName,
      StructType.fromAttributes(ScalaReflection.attributesFor[T]),
      messageType,
      binaryAsString,
      int96AsTimestamp,
      writeLegacyParquetFormat,
      expectedParquetColumn = expectedParquetColumn)
  }

  protected def testParquetToCatalyst(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean,
      int96AsTimestamp: Boolean,
      caseSensitive: Boolean = false,
      timestampNTZEnabled: Boolean = true,
      sparkReadSchema: Option[StructType] = None,
      expectedParquetColumn: Option[ParquetColumn] = None): Unit = {
    val converter = new ParquetToSparkSchemaConverter(
      assumeBinaryIsString = binaryAsString,
      assumeInt96IsTimestamp = int96AsTimestamp,
      caseSensitive = caseSensitive,
      timestampNTZEnabled = timestampNTZEnabled)

    test(s"sql <= parquet: $testName") {
      val actualParquetColumn = converter.convertParquetColumn(
          MessageTypeParser.parseMessageType(parquetSchema), sparkReadSchema)
      val actual = actualParquetColumn.sparkType
      val expected = sqlSchema
      assert(
        actual === expected,
        s"""Schema mismatch.
           |Expected schema: ${expected.json}
           |Actual schema:   ${actual.json}
         """.stripMargin)

      if (expectedParquetColumn.isDefined) {
        compareParquetColumn(actualParquetColumn, expectedParquetColumn.get)
      }
    }
  }

  protected def testCatalystToParquet(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      writeLegacyParquetFormat: Boolean,
      outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
        SQLConf.ParquetOutputTimestampType.INT96,
      timestampNTZEnabled: Boolean = true): Unit = {
    val converter = new SparkToParquetSchemaConverter(
      writeLegacyParquetFormat = writeLegacyParquetFormat,
      outputTimestampType = outputTimestampType,
      timestampNTZEnabled = timestampNTZEnabled)

    test(s"sql => parquet: $testName") {
      val actual = converter.convert(sqlSchema)
      val expected = MessageTypeParser.parseMessageType(parquetSchema)
      actual.checkContains(expected)
      expected.checkContains(actual)
    }
  }

  protected def testSchema(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean,
      int96AsTimestamp: Boolean,
      writeLegacyParquetFormat: Boolean,
      outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
        SQLConf.ParquetOutputTimestampType.INT96,
      expectedParquetColumn: Option[ParquetColumn] = None): Unit = {

    testCatalystToParquet(
      testName,
      sqlSchema,
      parquetSchema,
      writeLegacyParquetFormat,
      outputTimestampType)

    testParquetToCatalyst(
      testName,
      sqlSchema,
      parquetSchema,
      binaryAsString,
      int96AsTimestamp,
      expectedParquetColumn = expectedParquetColumn)
  }

  protected def compareParquetColumn(actual: ParquetColumn, expected: ParquetColumn): Unit = {
    assert(actual.sparkType == expected.sparkType, "sparkType mismatch: " +
        s"actual = ${actual.sparkType}, expected = ${expected.sparkType}")
    assert(actual.descriptor === expected.descriptor, "column descriptor mismatch: " +
        s"actual = ${actual.descriptor}, expected = ${expected.descriptor})")
    // since Parquet ColumnDescriptor equals only compares path, we'll need to compare other
    // fields explicitly here
    if (actual.descriptor.isDefined && expected.descriptor.isDefined) {
      val actualDesc = actual.descriptor.get
      val expectedDesc = expected.descriptor.get
      assert(actualDesc.getMaxRepetitionLevel == expectedDesc.getMaxRepetitionLevel)
      assert(actualDesc.getMaxRepetitionLevel == expectedDesc.getMaxRepetitionLevel)
      assert(actualDesc.getPrimitiveType === expectedDesc.getPrimitiveType)
    }

    assert(actual.repetitionLevel == expected.repetitionLevel, "repetition level mismatch: " +
        s"actual = ${actual.repetitionLevel}, expected = ${expected.repetitionLevel}")
    assert(actual.definitionLevel == expected.definitionLevel, "definition level mismatch: " +
        s"actual = ${actual.definitionLevel}, expected = ${expected.definitionLevel}")
    assert(actual.required == expected.required, "required mismatch: " +
        s"actual = ${actual.required}, expected = ${expected.required}")
    assert(actual.path == expected.path, "path mismatch: " +
        s"actual = ${actual.path}, expected = ${expected.path}")

    assert(actual.children.size == expected.children.size, "size of children mismatch: " +
        s"actual = ${actual.children.size}, expected = ${expected.children.size}")
    actual.children.zip(expected.children).foreach { case (actualChild, expectedChild) =>
      compareParquetColumn(actualChild, expectedChild)
    }
  }

  protected def primitiveParquetColumn(
      sparkType: DataType,
      parquetTypeName: PrimitiveTypeName,
      repetition: Repetition,
      repetitionLevel: Int,
      definitionLevel: Int,
      path: Seq[String],
      logicalTypeAnnotation: Option[LogicalTypeAnnotation] = None): ParquetColumn = {
    var typeBuilder = repetition match {
      case Repetition.REQUIRED => Types.required(parquetTypeName)
      case Repetition.OPTIONAL => Types.optional(parquetTypeName)
      case Repetition.REPEATED => Types.repeated(parquetTypeName)
    }
    if (logicalTypeAnnotation.isDefined) {
      typeBuilder = typeBuilder.as(logicalTypeAnnotation.get)
    }
    ParquetColumn(
      sparkType = sparkType,
      descriptor = Some(new ColumnDescriptor(path.toArray,
        typeBuilder.named(path.last), repetitionLevel, definitionLevel)),
      repetitionLevel = repetitionLevel,
      definitionLevel = definitionLevel,
      required = repetition != Repetition.OPTIONAL,
      path = path,
      children = Seq.empty)
  }
}

class ParquetSchemaInferenceSuite extends ParquetSchemaTest {
  testSchemaInference[(Boolean, Int, Long, Float, Double, Array[Byte])](
    "basic types",
    """
      |message root {
      |  required boolean _1;
      |  required int32   _2;
      |  required int64   _3;
      |  required float   _4;
      |  required double  _5;
      |  optional binary  _6;
      |}
    """.stripMargin,
    binaryAsString = false,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(
      ParquetColumn(
        sparkType = StructType.fromAttributes(
          ScalaReflection.attributesFor[(Boolean, Int, Long, Float, Double, Array[Byte])]),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = false,
        path = Seq(),
        children = Seq(
          primitiveParquetColumn(BooleanType, PrimitiveTypeName.BOOLEAN, Repetition.REQUIRED,
            0, 0, Seq("_1")),
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            0, 0, Seq("_2")),
          primitiveParquetColumn(LongType, PrimitiveTypeName.INT64, Repetition.REQUIRED,
            0, 0, Seq("_3")),
          primitiveParquetColumn(FloatType, PrimitiveTypeName.FLOAT, Repetition.REQUIRED,
            0, 0, Seq("_4")),
          primitiveParquetColumn(DoubleType, PrimitiveTypeName.DOUBLE, Repetition.REQUIRED,
            0, 0, Seq("_5")),
          primitiveParquetColumn(BinaryType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
            0, 1, Seq("_6"))
        )))
  )

  testSchemaInference[(Byte, Short, Int, Long, java.sql.Date)](
    "logical integral types",
    """
      |message root {
      |  required int32 _1 (INT_8);
      |  required int32 _2 (INT_16);
      |  required int32 _3 (INT_32);
      |  required int64 _4 (INT_64);
      |  optional int32 _5 (DATE);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(
      ParquetColumn(
        sparkType = StructType.fromAttributes(
          ScalaReflection.attributesFor[(Byte, Short, Int, Long, java.sql.Date)]),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = false,
        path = Seq(),
        children = Seq(
          primitiveParquetColumn(ByteType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            0, 0, Seq("_1"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.intType(8, true))),
          primitiveParquetColumn(ShortType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            0, 0, Seq("_2"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.intType(16, true))),
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            0, 0, Seq("_3"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.intType(32, true))),
          primitiveParquetColumn(LongType, PrimitiveTypeName.INT64, Repetition.REQUIRED,
            0, 0, Seq("_4"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.intType(64, true))),
          primitiveParquetColumn(DateType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
            0, 1, Seq("_5"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.dateType()))
        ))))

  testSchemaInference[Tuple1[String]](
    "string",
    """
      |message root {
      |  optional binary _1 (UTF8);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(
      ParquetColumn(
        sparkType = StructType.fromAttributes(
          ScalaReflection.attributesFor[Tuple1[String]]),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = false,
        path = Seq(),
        children = Seq(
          primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
            0, 1, Seq("_1"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType()))
        ))))

  testSchemaInference[Tuple1[String]](
    "binary enum as string",
    """
      |message root {
      |  optional binary _1 (ENUM);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(
      ParquetColumn(
        sparkType = StructType.fromAttributes(
          ScalaReflection.attributesFor[Tuple1[String]]),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = false,
        path = Seq(),
        children = Seq(
          primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
            0, 1, Seq("_1"), logicalTypeAnnotation = Some(LogicalTypeAnnotation.enumType()))
        ))))

  testSchemaInference[Tuple1[Seq[Int]]](
    "non-nullable array - non-standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          ArrayType(IntegerType, containsNull = false)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("_1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REPEATED,
            1, 2, Seq("_1", "array")))
      )))))

  testSchemaInference[Tuple1[Seq[Int]]](
    "non-nullable array - standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          ArrayType(IntegerType, containsNull = false)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("_1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            1, 2, Seq("_1", "list", "element"))
        ))))))

  testSchemaInference[Tuple1[Seq[Integer]]](
    "nullable array - non-standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          ArrayType(IntegerType, containsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = true),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("_1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
            1, 3, Seq("_1", "bag", "array"))
        ))))))

  testSchemaInference[Tuple1[Seq[Integer]]](
    "nullable array - standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          ArrayType(IntegerType, containsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = true),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("_1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
            1, 3, Seq("_1", "list", "element"))
        ))))))

  testSchemaInference[Tuple1[Map[Int, String]]](
    "map - standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          MapType(IntegerType, StringType, valueContainsNull = true),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("_1", "key_value", "key")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              1, 3, Seq("_1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  testSchemaInference[Tuple1[Map[Int, String]]](
    "map - non-standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          MapType(IntegerType, StringType, valueContainsNull = true),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("_1", "key_value", "key")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              1, 3, Seq("_1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
          )))))

  testSchemaInference[Tuple1[Map[(String, String), String]]](
    "map - group type key",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required group key {
      |        optional binary _1 (UTF8);
      |        optional binary _2 (UTF8);
      |      }
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          MapType(StructType(Seq(StructField("_1", StringType), StructField("_2", StringType))),
            StringType, valueContainsNull = true),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(
            StructType(Seq(StructField("_1", StringType), StructField("_2", StringType))),
            StringType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq(
            ParquetColumn(
              sparkType =
                StructType(Seq(StructField("_1", StringType), StructField("_2", StringType))),
              descriptor = None,
              repetitionLevel = 1,
              definitionLevel = 2,
              required = true,
              path = Seq("_1", "key_value", "key"),
              children = Seq(
                primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
                  1, 3, Seq("_1", "key_value", "key", "_1"),
                  logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())),
                primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
                  1, 3, Seq("_1", "key_value", "key", "_2"),
                  logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
            ),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              1, 3, Seq("_1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  testSchemaInference[Tuple1[(Int, String)]](
    "struct",
    """
      |message root {
      |  optional group _1 {
      |    required int32 _1;
      |    optional binary _2 (UTF8);
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          StructType(Seq(
            StructField("_1", IntegerType, nullable = false),
            StructField("_2", StringType)))))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = StructType(Seq(
            StructField("_1", IntegerType, nullable = false),
            StructField("_2", StringType))),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              0, 1, Seq("_1", "_1")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              0, 2, Seq("_1", "_2"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  testSchemaInference[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    "deeply nested type - non-standard",
    """
      |message root {
      |  optional group _1 (MAP_KEY_VALUE) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group bag {
      |            optional group array {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          MapType(IntegerType,
            StructType(Seq(
              StructField("_1", StringType),
              StructField("_2", ArrayType(
                StructType(Seq(
                  StructField("_1", IntegerType, nullable = false),
                  StructField("_2", DoubleType, nullable = false))))))),
            valueContainsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType =
            MapType(IntegerType,
              StructType(Seq(
                StructField("_1", StringType),
                StructField("_2", ArrayType(
                  StructType(Seq(
                    StructField("_1", IntegerType, nullable = false),
                    StructField("_2", DoubleType, nullable = false))))))),
              valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("_1", "key_value", "key")),
            ParquetColumn(
              sparkType =
                StructType(Seq(
                  StructField("_1", StringType),
                  StructField("_2", ArrayType(
                    StructType(Seq(
                      StructField("_1", IntegerType, nullable = false),
                      StructField("_2", DoubleType, nullable = false))))))),
              descriptor = None,
              repetitionLevel = 1,
              definitionLevel = 3,
              required = false,
              path = Seq("_1", "key_value", "value"),
              children = Seq(
                primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
                  1, 4, Seq("_1", "key_value", "value", "_1"),
                  logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())),
                ParquetColumn(
                  sparkType = ArrayType(
                    StructType(Seq(
                      StructField("_1", IntegerType, nullable = false),
                      StructField("_2", DoubleType, nullable = false)))),
                  descriptor = None,
                  repetitionLevel = 1,
                  definitionLevel = 4,
                  required = false,
                  path = Seq("_1", "key_value", "value", "_2"),
                  children = Seq(
                    ParquetColumn(
                      sparkType = StructType(Seq(
                        StructField("_1", IntegerType, nullable = false),
                        StructField("_2", DoubleType, nullable = false))),
                      descriptor = None,
                      repetitionLevel = 2,
                      definitionLevel = 6,
                      required = false,
                      path = Seq("_1", "key_value", "value", "_2", "bag", "array"),
                      children = Seq(
                        primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32,
                          Repetition.REQUIRED, 2, 6,
                          Seq("_1", "key_value", "value", "_2", "bag", "array", "_1")),
                        primitiveParquetColumn(DoubleType, PrimitiveTypeName.DOUBLE,
                          Repetition.REQUIRED, 2, 6,
                          Seq("_1", "key_value", "value", "_2", "bag", "array", "_2"))
                      ))))))
          ))))))

  testSchemaInference[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    "deeply nested type - standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group list {
      |            optional group element {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "_1",
          MapType(IntegerType,
            StructType(Seq(
              StructField("_1", StringType),
              StructField("_2", ArrayType(
                StructType(Seq(
                  StructField("_1", IntegerType, nullable = false),
                  StructField("_2", DoubleType, nullable = false))))))),
            valueContainsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType =
            MapType(IntegerType,
              StructType(Seq(
                StructField("_1", StringType),
                StructField("_2", ArrayType(
                  StructType(Seq(
                    StructField("_1", IntegerType, nullable = false),
                    StructField("_2", DoubleType, nullable = false))))))),
              valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("_1", "key_value", "key")),
            ParquetColumn(
              sparkType =
                StructType(Seq(
                  StructField("_1", StringType),
                  StructField("_2", ArrayType(
                    StructType(Seq(
                      StructField("_1", IntegerType, nullable = false),
                      StructField("_2", DoubleType, nullable = false))))))),
              descriptor = None,
              repetitionLevel = 1,
              definitionLevel = 3,
              required = false,
              path = Seq("_1", "key_value", "value"),
              children = Seq(
                primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
                  1, 4, Seq("_1", "key_value", "value", "_1"),
                  logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())),
                ParquetColumn(
                  sparkType = ArrayType(
                    StructType(Seq(
                      StructField("_1", IntegerType, nullable = false),
                      StructField("_2", DoubleType, nullable = false)))),
                  descriptor = None,
                  repetitionLevel = 1,
                  definitionLevel = 4,
                  required = false,
                  path = Seq("_1", "key_value", "value", "_2"),
                  children = Seq(
                    ParquetColumn(
                      sparkType = StructType(Seq(
                        StructField("_1", IntegerType, nullable = false),
                        StructField("_2", DoubleType, nullable = false))),
                      descriptor = None,
                      repetitionLevel = 2,
                      definitionLevel = 6,
                      required = false,
                      path = Seq("_1", "key_value", "value", "_2", "list", "element"),
                      children = Seq(
                        primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32,
                          Repetition.REQUIRED, 2, 6,
                          Seq("_1", "key_value", "value", "_2", "list", "element", "_1")),
                        primitiveParquetColumn(DoubleType, PrimitiveTypeName.DOUBLE,
                          Repetition.REQUIRED, 2, 6,
                          Seq("_1", "key_value", "value", "_2", "list", "element", "_2"))))))))
        ))))))

  testSchemaInference[(Option[Int], Map[Int, Option[Double]])](
    "optional types",
    """
      |message root {
      |  optional int32 _1;
      |  optional group _2 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional double value;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField("_1", IntegerType),
        StructField("_2", MapType(IntegerType, DoubleType, valueContainsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = IntegerType,
          descriptor = Some(new ColumnDescriptor(Array("_1"),
            Types.optional(PrimitiveTypeName.INT32).named("_1"), 0, 1)),
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_1"),
          children = Seq()),
        ParquetColumn(
          sparkType = MapType(IntegerType, DoubleType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("_2"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("_2", "key_value", "key")),
            primitiveParquetColumn(DoubleType, PrimitiveTypeName.DOUBLE, Repetition.OPTIONAL,
              1, 3, Seq("_2", "key_value", "value")))))
    )))
}

class ParquetSchemaSuite extends ParquetSchemaTest {
  test("DataType string parser compatibility") {
    // This is the generated string from previous versions of the Spark SQL, using the following:
    // val schema = StructType(List(
    //  StructField("c1", IntegerType, false),
    //  StructField("c2", BinaryType, true)))
    val caseClassString =
      "StructType(List(StructField(c1,IntegerType,false), StructField(c2,BinaryType,true)))"

    // scalastyle:off
    val jsonString = """{"type":"struct","fields":[{"name":"c1","type":"integer","nullable":false,"metadata":{}},{"name":"c2","type":"binary","nullable":true,"metadata":{}}]}"""
    // scalastyle:on

    val fromCaseClassString = StructType.fromString(caseClassString)
    val fromJson = StructType.fromString(jsonString)

    (fromCaseClassString, fromJson).zipped.foreach { (a, b) =>
      assert(a.name == b.name)
      assert(a.dataType === b.dataType)
      assert(a.nullable === b.nullable)
    }
  }

  test("schema merging failure error message") {
    import testImplicits._

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).write.parquet(s"$path/p=1")
      spark.range(3).select($"id" cast IntegerType as Symbol("id"))
        .write.parquet(s"$path/p=2")

      val message = intercept[SparkException] {
        spark.read.option("mergeSchema", "true").parquet(path).schema
      }.getMessage

      assert(message.contains("Failed merging schema"))
    }
  }

  // =======================================
  // Tests for parquet schema mismatch error
  // =======================================
  def testSchemaMismatch(path: String, vectorizedReaderEnabled: Boolean): SparkException = {
    import testImplicits._

    var e: SparkException = null
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorizedReaderEnabled.toString) {
      // Create two parquet files with different schemas in the same folder
      Seq(("bcd", 2)).toDF("a", "b").coalesce(1).write.mode("overwrite").parquet(s"$path/parquet")
      Seq((1, "abc")).toDF("a", "b").coalesce(1).write.mode("append").parquet(s"$path/parquet")

      e = intercept[SparkException] {
        spark.read.parquet(s"$path/parquet").collect()
      }
    }
    e
  }

  test("schema mismatch failure error message for parquet reader") {
    withTempPath { dir =>
      val e = testSchemaMismatch(dir.getCanonicalPath, vectorizedReaderEnabled = false)
      val expectedMessage = "Encountered error while reading file"
      assert(e.getCause.isInstanceOf[QueryExecutionException])
      assert(e.getCause.getCause.isInstanceOf[ParquetDecodingException])
      assert(e.getCause.getMessage.contains(expectedMessage))
    }
  }

  test("schema mismatch failure error message for parquet vectorized reader") {
    withTempPath { dir =>
      val e = testSchemaMismatch(dir.getCanonicalPath, vectorizedReaderEnabled = true)
      assert(e.getCause.isInstanceOf[QueryExecutionException])
      assert(e.getCause.getCause.isInstanceOf[SchemaColumnConvertNotSupportedException])

      // Check if the physical type is reporting correctly
      val errMsg = e.getCause.getMessage
      assert(errMsg.startsWith("Parquet column cannot be converted in file"))
      val file = errMsg.substring("Parquet column cannot be converted in file ".length,
        errMsg.indexOf(". "))
      val col = spark.read.parquet(file).schema.fields.filter(_.name == "a")
      assert(col.length == 1)
      if (col(0).dataType == StringType) {
        assert(errMsg.contains("Column: [a], Expected: int, Found: BINARY"))
      } else {
        assert(errMsg.endsWith("Column: [a], Expected: string, Found: INT32"))
      }
    }
  }

  // =======================================================
  // Tests for converting Parquet LIST to Catalyst ArrayType
  // =======================================================

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(IntegerType, containsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = true),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
            1, 3, Seq("f1", "list", "element"))))
      ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with nullable element type - 2",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      optional int32 num;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
        sparkType = StructType(Seq(
          StructField(
            "f1",
            ArrayType(IntegerType, containsNull = true)))),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = false,
        path = Seq(),
        children = Seq(ParquetColumn(
          sparkType = ArrayType(IntegerType, containsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
              1, 3, Seq("f1", "element", "num"))))
        ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(IntegerType, containsNull = false)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            1, 2, Seq("f1", "list", "element"))))
      ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 2",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      required int32 num;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(IntegerType, containsNull = false)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
            1, 2, Seq("f1", "element", "num"))))
      ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 3",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 element;
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(IntegerType, containsNull = false)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REPEATED,
            1, 2, Seq("f1", "element"))))
      ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 4",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false),
            StructField("num", IntegerType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      required binary str (UTF8);
      |      required int32 num;
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(
            StructType(Seq(
              StructField("str", StringType, nullable = false),
              StructField("num", IntegerType, nullable = false))),
            containsNull = false)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false),
            StructField("num", IntegerType, nullable = false))),
          containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(ParquetColumn(
          sparkType = StructType(Seq(
            StructField("str", StringType, nullable = false),
            StructField("num", IntegerType, nullable = false))),
          descriptor = None,
          repetitionLevel = 1,
          definitionLevel = 2,
          required = false,
          path = Seq("f1", "element"),
          children = Seq(
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.REQUIRED,
              1, 2, Seq("f1", "element", "str"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())),
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "element", "num")))))
      )))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 5 - parquet-avro style",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group array {
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(
            StructType(Seq(
              StructField("str", StringType, nullable = false))),
            containsNull = false),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(ParquetColumn(
          sparkType = StructType(Seq(
              StructField("str", StringType, nullable = false))),
          descriptor = None,
          repetitionLevel = 1,
          definitionLevel = 2,
          required = false,
          path = Seq("f1", "array"),
          children = Seq(
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.REQUIRED,
              1, 2, Seq("f1", "array", "str"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType()))))))
      ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type - 6 - parquet-thrift style",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group f1_tuple {
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(
            StructType(Seq(
              StructField("str", StringType, nullable = false))),
            containsNull = false),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 1,
        required = false,
        path = Seq("f1"),
        children = Seq(ParquetColumn(
          sparkType = StructType(Seq(
            StructField("str", StringType, nullable = false))),
          descriptor = None,
          repetitionLevel = 1,
          definitionLevel = 2,
          required = false,
          path = Seq("f1", "f1_tuple"),
          children = Seq(
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.REQUIRED,
              1, 2, Seq("f1", "f1_tuple", "str"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType()))))))
      ))))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type 7 - " +
      "parquet-protobuf primitive lists",
    new StructType()
      .add("f1", ArrayType(IntegerType, containsNull = false), nullable = false),
    """message root {
      |  repeated int32 f1;
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(IntegerType, containsNull = false), nullable = false))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(IntegerType, containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = true,
        path = Seq("f1"),
        children = Seq(
          primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REPEATED,
            1, 1, Seq("f1")))))
      )))

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type 8 - " +
      "parquet-protobuf non-primitive lists",
    {
      val elementType =
        new StructType()
          .add("c1", StringType, nullable = true)
          .add("c2", IntegerType, nullable = false)

      new StructType()
        .add("f1", ArrayType(elementType, containsNull = false), nullable = false)
    },
    """message root {
      |  repeated group f1 {
      |    optional binary c1 (UTF8);
      |    required int32 c2;
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          ArrayType(
            new StructType()
                .add("c1", StringType, nullable = true)
                .add("c2", IntegerType, nullable = false),
            containsNull = false), nullable = false))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(ParquetColumn(
        sparkType = ArrayType(
          new StructType()
              .add("c1", StringType, nullable = true)
              .add("c2", IntegerType, nullable = false),
          containsNull = false),
        descriptor = None,
        repetitionLevel = 0,
        definitionLevel = 0,
        required = true,
        path = Seq("f1"),
        children = Seq(
          ParquetColumn(
            sparkType = new StructType()
                .add("c1", StringType, nullable = true)
                .add("c2", IntegerType, nullable = false),
            descriptor = None,
            repetitionLevel = 1,
            definitionLevel = 1,
            required = true,
            path = Seq("f1"),
            children = Seq(
              primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
                1, 2, Seq("f1", "c1"),
                logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())),
              primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
                1, 1, Seq("f1", "c2")))))
        )))))

  // =======================================================
  // Tests for converting Catalyst ArrayType to Parquet LIST
  // =======================================================

  testCatalystToParquet(
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with nullable element type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with non-nullable element type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  // ====================================================
  // Tests for converting Parquet Map to Catalyst MapType
  // ====================================================

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          MapType(IntegerType, StringType, valueContainsNull = false),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = false),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "key")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType()))
          ))))))

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with non-nullable value type - 2",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group key_value {
      |      required int32 num;
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          MapType(IntegerType, StringType, valueContainsNull = false),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = false),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "num")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "str"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType()))
          ))))))

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with non-nullable value type - 3 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          MapType(IntegerType, StringType, valueContainsNull = false),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = false),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "key")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          MapType(IntegerType, StringType, valueContainsNull = true),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "key")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              1, 3, Seq("f1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with nullable value type - 2",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group key_value {
      |      required int32 num;
      |      optional binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          MapType(IntegerType, StringType, valueContainsNull = true),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "num")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              1, 3, Seq("f1", "key_value", "str"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  testParquetToCatalyst(
    "Backwards-compatibility: MAP with nullable value type - 3 - parquet-avro style",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "f1",
          MapType(IntegerType, StringType, valueContainsNull = true),
          nullable = true))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType = MapType(IntegerType, StringType, valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "key")),
            primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
              1, 3, Seq("f1", "key_value", "value"),
              logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())))
        )))))

  // ====================================================
  // Tests for converting Catalyst MapType to Parquet Map
  // ====================================================

  testCatalystToParquet(
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with non-nullable value type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 3 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testParquetToCatalyst(
    "SPARK-36935: test case insensitive when converting Parquet schema",
    StructType(Seq(StructField("F1", ShortType))),
    """message root {
      |  optional int32 f1;
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    sparkReadSchema = Some(StructType(Seq(StructField("F1", ShortType)))),
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(StructField("F1", ShortType))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        primitiveParquetColumn(ShortType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
          0, 1, Seq("f1"))
      )
    )))

  testParquetToCatalyst(
    "SPARK-36935: test case sensitive when converting Parquet schema",
    StructType(Seq(StructField("f1", IntegerType))),
    """message root {
      |  optional int32 f1;
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    caseSensitive = true,
    sparkReadSchema = Some(StructType(Seq(StructField("F1", ShortType)))),
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(StructField("f1", IntegerType))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        primitiveParquetColumn(IntegerType, PrimitiveTypeName.INT32, Repetition.OPTIONAL,
          0, 1, Seq("f1"))
      )
    )))

  testParquetToCatalyst(
    "SPARK-36935: test Spark read schema with case sensitivity",
    StructType(Seq(
      StructField(
        "F1",
        MapType(ShortType,
          StructType(Seq(
            StructField("G1", StringType),
            StructField("G2", ArrayType(
              StructType(Seq(
                StructField("H1", ByteType, nullable = false),
                StructField("H2", FloatType, nullable = false))))))),
          valueContainsNull = true)))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional group value {
      |        optional binary g1 (UTF8);
      |        optional group g2 (LIST) {
      |          repeated group list {
      |            optional group element {
      |              required int32 h1;
      |              required double h2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    sparkReadSchema =
      Some(StructType(Seq(
        StructField(
          "F1",
          MapType(ShortType,
            StructType(Seq(
              StructField("G1", StringType),
              StructField("G2", ArrayType(
                StructType(Seq(
                  StructField("H1", ByteType, nullable = false),
                  StructField("H2", FloatType, nullable = false))))))),
            valueContainsNull = true))))),
    expectedParquetColumn = Some(ParquetColumn(
      sparkType = StructType(Seq(
        StructField(
          "F1",
          MapType(ShortType,
            StructType(Seq(
              StructField("G1", StringType),
              StructField("G2", ArrayType(
                StructType(Seq(
                  StructField("H1", ByteType, nullable = false),
                  StructField("H2", FloatType, nullable = false))))))),
            valueContainsNull = true)))),
      descriptor = None,
      repetitionLevel = 0,
      definitionLevel = 0,
      required = false,
      path = Seq(),
      children = Seq(
        ParquetColumn(
          sparkType =
            MapType(ShortType,
              StructType(Seq(
                StructField("G1", StringType),
                StructField("G2", ArrayType(
                  StructType(Seq(
                    StructField("H1", ByteType, nullable = false),
                    StructField("H2", FloatType, nullable = false))))))),
              valueContainsNull = true),
          descriptor = None,
          repetitionLevel = 0,
          definitionLevel = 1,
          required = false,
          path = Seq("f1"),
          children = Seq(
            primitiveParquetColumn(ShortType, PrimitiveTypeName.INT32, Repetition.REQUIRED,
              1, 2, Seq("f1", "key_value", "key")),
            ParquetColumn(
              sparkType =
                StructType(Seq(
                  StructField("G1", StringType),
                  StructField("G2", ArrayType(
                    StructType(Seq(
                      StructField("H1", ByteType, nullable = false),
                      StructField("H2", FloatType, nullable = false))))))),
              descriptor = None,
              repetitionLevel = 1,
              definitionLevel = 3,
              required = false,
              path = Seq("f1", "key_value", "value"),
              children = Seq(
                primitiveParquetColumn(StringType, PrimitiveTypeName.BINARY, Repetition.OPTIONAL,
                  1, 4, Seq("f1", "key_value", "value", "g1"),
                  logicalTypeAnnotation = Some(LogicalTypeAnnotation.stringType())),
                ParquetColumn(
                  sparkType = ArrayType(
                    StructType(Seq(
                      StructField("H1", ByteType, nullable = false),
                      StructField("H2", FloatType, nullable = false)))),
                  descriptor = None,
                  repetitionLevel = 1,
                  definitionLevel = 4,
                  required = false,
                  path = Seq("f1", "key_value", "value", "g2"),
                  children = Seq(
                    ParquetColumn(
                      sparkType = StructType(Seq(
                        StructField("H1", ByteType, nullable = false),
                        StructField("H2", FloatType, nullable = false))),
                      descriptor = None,
                      repetitionLevel = 2,
                      definitionLevel = 6,
                      required = false,
                      path = Seq("f1", "key_value", "value", "g2", "list", "element"),
                      children = Seq(
                        primitiveParquetColumn(ByteType, PrimitiveTypeName.INT32,
                          Repetition.REQUIRED, 2, 6,
                          Seq("f1", "key_value", "value", "g2", "list", "element", "h1")),
                        primitiveParquetColumn(FloatType, PrimitiveTypeName.DOUBLE,
                          Repetition.REQUIRED, 2, 6,
                          Seq("f1", "key_value", "value", "g2", "list", "element", "h2"))))))))
          ))))))

  // =================================
  // Tests for conversion for decimals
  // =================================

  testSchema(
    "DECIMAL(1, 0) - standard",
    StructType(Seq(StructField("f1", DecimalType(1, 0)))),
    """message root {
      |  optional int32 f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(8, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(8, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(9, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(9, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(18, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(18, 3)))),
    """message root {
      |  optional int64 f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(19, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(19, 3)))),
    """message root {
      |  optional fixed_len_byte_array(9) f1 (DECIMAL(19, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = false)

  testSchema(
    "DECIMAL(1, 0) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(1, 0)))),
    """message root {
      |  optional fixed_len_byte_array(1) f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "DECIMAL(8, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(8, 3)))),
    """message root {
      |  optional fixed_len_byte_array(4) f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "DECIMAL(9, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(9, 3)))),
    """message root {
      |  optional fixed_len_byte_array(5) f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "DECIMAL(18, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(18, 3)))),
    """message root {
      |  optional fixed_len_byte_array(8) f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = true,
    writeLegacyParquetFormat = true)

  testSchema(
    "Timestamp written and read as INT64 with TIMESTAMP_MILLIS",
    StructType(Seq(StructField("f1", TimestampType))),
    """message root {
      |  optional INT64 f1 (TIMESTAMP_MILLIS);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = false,
    writeLegacyParquetFormat = true,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS)

  testSchema(
    "Timestamp written and read as INT64 with TIMESTAMP_MICROS",
    StructType(Seq(StructField("f1", TimestampType))),
    """message root {
      |  optional INT64 f1 (TIMESTAMP_MICROS);
      |}
    """.stripMargin,
    binaryAsString = true,
    int96AsTimestamp = false,
    writeLegacyParquetFormat = true,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS)

  testCatalystToParquet(
    "SPARK-36825: Year-month interval written and read as INT32",
    StructType(Seq(StructField("f1", YearMonthIntervalType()))),
    """message root {
      |  optional INT32 f1;
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "SPARK-36825: Day-time interval written and read as INT64",
    StructType(Seq(StructField("f1", DayTimeIntervalType()))),
    """message root {
      |  optional INT64 f1;
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  // The behavior of reading/writing TimestampNTZ type is independent of the configurations
  // SQLConf.PARQUET_INT96_AS_TIMESTAMP and SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE
  Seq(true, false).foreach { int96AsTimestamp =>
    Seq(INT96, TIMESTAMP_MILLIS, TIMESTAMP_MICROS).foreach { outputTsType =>
      testSchema(
        s"TimestampNTZ written and read as INT64 with TIMESTAMP_MICROS - " +
          s"int96AsTimestamp as $int96AsTimestamp, outputTimestampType: $outputTsType",
        StructType(Seq(StructField("f1", TimestampNTZType))),
        """message root {
          |  optional INT64 f1 (TIMESTAMP(MICROS,false));
          |}
        """.stripMargin,
        binaryAsString = true,
        int96AsTimestamp = int96AsTimestamp,
        writeLegacyParquetFormat = true,
        outputTimestampType = outputTsType)
    }

    testParquetToCatalyst(
      s"TimestampNTZ read as INT64 with TIMESTAMP_MILLIS - " +
        s"int96AsTimestamp as $int96AsTimestamp",
      StructType(Seq(StructField("f1", TimestampNTZType))),
      """message root {
        |  optional INT64 f1 (TIMESTAMP(MILLIS,false));
        |}
        """.stripMargin,
      binaryAsString = true,
      int96AsTimestamp = int96AsTimestamp,
      timestampNTZEnabled = true)
  }

  testCatalystToParquet(
    "TimestampNTZ Spark to Parquet conversion for complex types",
    StructType(
      Seq(
        StructField("f1", TimestampNTZType),
        StructField("f2", ArrayType(TimestampNTZType)),
        StructField("f3", StructType(Seq(StructField("f4", TimestampNTZType))))
      )
    ),
    """message spark_schema {
      |  optional int64 f1 (TIMESTAMP(MICROS,false));
      |  optional group f2 (LIST) {
      |    repeated group list {
      |      optional int64 element (TIMESTAMP(MICROS,false));
      |    }
      |  }
      |  optional group f3 {
      |    optional int64 f4 (TIMESTAMP(MICROS,false));
      |  }
      |}
      """.stripMargin,
    writeLegacyParquetFormat = false,
    timestampNTZEnabled = true)

  for (timestampNTZEnabled <- Seq(true, false)) {
    val dataType = if (timestampNTZEnabled) TimestampNTZType else TimestampType

    testParquetToCatalyst(
      "TimestampNTZ Parquet to Spark conversion for complex types, " +
        s"timestampNTZEnabled: $timestampNTZEnabled",
      StructType(
        Seq(
          StructField("f1", dataType),
          StructField("f2", ArrayType(dataType)),
          StructField("f3", StructType(Seq(StructField("f4", dataType))))
        )
      ),
      """message spark_schema {
        |  optional int64 f1 (TIMESTAMP(MICROS,false));
        |  optional group f2 (LIST) {
        |    repeated group list {
        |      optional int64 element (TIMESTAMP(MICROS,false));
        |    }
        |  }
        |  optional group f3 {
        |    optional int64 f4 (TIMESTAMP(MICROS,false));
        |  }
        |}
        """.stripMargin,
      binaryAsString = true,
      int96AsTimestamp = false,
      timestampNTZEnabled = timestampNTZEnabled)
  }

  private def testSchemaClipping(
      testName: String,
      parquetSchema: String,
      catalystSchema: StructType,
      expectedSchema: String,
      caseSensitive: Boolean = true): Unit = {
    testSchemaClipping(testName, parquetSchema, catalystSchema,
      MessageTypeParser.parseMessageType(expectedSchema), caseSensitive)
  }

  private def testSchemaClipping(
      testName: String,
      parquetSchema: String,
      catalystSchema: StructType,
      expectedSchema: MessageType,
      caseSensitive: Boolean): Unit = {
    test(s"Clipping - $testName") {
      val actual = ParquetReadSupport.clipParquetSchema(
        MessageTypeParser.parseMessageType(parquetSchema),
        catalystSchema,
        caseSensitive,
        useFieldId = false,
        timestampNTZEnabled = true)

      try {
        expectedSchema.checkContains(actual)
        actual.checkContains(expectedSchema)
      } catch { case cause: Throwable =>
        fail(
          s"""Expected clipped schema:
             |$expectedSchema
             |Actual clipped schema:
             |$actual
           """.stripMargin,
          cause)
      }
    }
  }

  testSchemaClipping(
    "simple nested struct",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |    optional int32 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType().add("f00", IntegerType, nullable = true)
      new StructType()
        .add("f0", f0Type, nullable = false)
        .add("f1", IntegerType, nullable = true)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |  }
        |  optional int32 f1;
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-protobuf style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    repeated binary f00 (UTF8);
        |    repeated group f01 {
        |      optional int32 f010;
        |      optional double f011;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f00Type = ArrayType(StringType, containsNull = false)
      val f01Type = ArrayType(
        new StructType()
          .add("f011", DoubleType, nullable = true),
        containsNull = false)

      val f0Type = new StructType()
        .add("f00", f00Type, nullable = false)
        .add("f01", f01Type, nullable = false)
      val f1Type = ArrayType(IntegerType, containsNull = true)

      new StructType()
        .add("f0", f0Type, nullable = false)
        .add("f1", f1Type, nullable = true)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    repeated binary f00 (UTF8);
        |    repeated group f01 {
        |      optional double f011;
        |    }
        |  }
        |
        |  optional group f1 (LIST) {
        |    repeated group list {
        |      optional int32 element;
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-thrift style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary f00_tuple (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group f01_tuple {
        |        optional int32 f010;
        |        optional double f011;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary f00_tuple (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group f01_tuple {
        |        optional double f011;
        |        optional int64 f012;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-avro style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary array (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group array {
        |        optional int32 f010;
        |        optional double f011;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary array (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group array {
        |        optional double f011;
        |        optional int64 f012;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-hive style array",

    parquetSchema =
      """message root {
        |  optional group f0 {
        |    optional group f00 (LIST) {
        |      repeated group bag {
        |        optional binary array_element;
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group bag {
        |        optional group array_element {
        |          optional int32 f010;
        |          optional double f011;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = true), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = true), nullable = true)

      new StructType().add("f0", f0Type, nullable = true)
    },

    expectedSchema =
      """message root {
        |  optional group f0 {
        |    optional group f00 (LIST) {
        |      repeated group bag {
        |        optional binary array_element;
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group bag {
        |        optional group array_element {
        |          optional double f011;
        |          optional int64 f012;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "2-level list of required struct",

    parquetSchema =
      s"""message root {
         |  required group f0 {
         |    required group f00 (LIST) {
         |      repeated group element {
         |        required int32 f000;
         |        optional int64 f001;
         |      }
         |    }
         |  }
         |}
       """.stripMargin,

    catalystSchema = {
      val f00ElementType =
        new StructType()
          .add("f001", LongType, nullable = true)
          .add("f002", DoubleType, nullable = false)

      val f00Type = ArrayType(f00ElementType, containsNull = false)
      val f0Type = new StructType().add("f00", f00Type, nullable = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      s"""message root {
         |  required group f0 {
         |    required group f00 (LIST) {
         |      repeated group element {
         |        optional int64 f001;
         |        required double f002;
         |      }
         |    }
         |  }
         |}
       """.stripMargin)

  testSchemaClipping(
    "standard array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group list {
        |        required group element {
        |          optional int32 f010;
        |          optional double f011;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group list {
        |        required group element {
        |          optional double f011;
        |          optional int64 f012;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "empty requested schema",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    required int32 f00;
        |    required int64 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema = new StructType(),

    expectedSchema = ParquetSchemaConverter.EMPTY_MESSAGE,
    caseSensitive = true)

  testSchemaClipping(
    "disjoint field sets",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    required int32 f00;
        |    required int64 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema =
      new StructType()
        .add(
          "f0",
          new StructType()
            .add("f02", FloatType, nullable = true)
            .add("f03", DoubleType, nullable = true),
          nullable = true),

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional float f02;
        |    optional double f03;
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-avro style map",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required group value {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val valueType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(IntegerType, valueType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required group value {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard map",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required int32 key;
        |      required group value {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val valueType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(IntegerType, valueType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required int32 key;
        |      required group value {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard map with complex key",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required group key {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |      required int32 value;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val keyType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(keyType, IntegerType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required group key {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |      required int32 value;
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "case-insensitive resolution: no ambiguity",
    parquetSchema =
      """message root {
        |  required group A {
        |    optional int32 B;
        |  }
        |  optional int32 c;
        |}
      """.stripMargin,
    catalystSchema = {
      val nestedType = new StructType().add("b", IntegerType, nullable = true)
      new StructType()
        .add("a", nestedType, nullable = true)
        .add("c", IntegerType, nullable = true)
    },
    expectedSchema =
      """message root {
        |  required group A {
        |    optional int32 B;
        |  }
        |  optional int32 c;
        |}
      """.stripMargin,
    caseSensitive = false)

    test("Clipping - case-insensitive resolution: more than one field is matched") {
      val parquetSchema =
        """message root {
          |  required group A {
          |    optional int32 B;
          |  }
          |  optional int32 c;
          |  optional int32 a;
          |}
        """.stripMargin
      val catalystSchema = {
        val nestedType = new StructType().add("b", IntegerType, nullable = true)
        new StructType()
          .add("a", nestedType, nullable = true)
          .add("c", IntegerType, nullable = true)
      }
      assertThrows[RuntimeException] {
        ParquetReadSupport.clipParquetSchema(
         MessageTypeParser.parseMessageType(parquetSchema),
          catalystSchema,
          caseSensitive = false,
          useFieldId = false,
          timestampNTZEnabled = false)
      }
    }
}
