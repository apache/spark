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

package org.apache.spark.sql.catalyst.util

import java.math.{BigDecimal => JavaBigDecimal}
import java.nio.ByteOrder.{nativeOrder, BIG_ENDIAN}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, CalendarIntervalType, Decimal, DecimalType, GeographyType, GeometryType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.{BinaryView, CalendarInterval, UTF8String}

class UnsafeRowUtilsSuite extends SparkFunSuite with SQLConfHelper {

  val testKeys: Seq[String] = Seq("key1", "key2")
  val testValues: Seq[String] = Seq("sum(key1)", "sum(key2)")

  val testOutputSchema: StructType = StructType(
    testKeys.map(createIntegerField) ++ testValues.map(createIntegerField))

  val testRow: UnsafeRow = {
    val unsafeRowProjection = UnsafeProjection.create(testOutputSchema)
    val row = unsafeRowProjection(new SpecificInternalRow(testOutputSchema))
    (testKeys ++ testValues).zipWithIndex.foreach { case (_, index) => row.setInt(index, index) }
    row
  }

  private def createIntegerField(name: String): StructField = {
    StructField(name, IntegerType, nullable = false)
  }

  test("UnsafeRow format invalidation") {
    // Pass the checking
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(testRow, testOutputSchema).isEmpty)
    // Fail for fields number not match
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(
      testRow, StructType(testKeys.map(createIntegerField))).isDefined)
    // Fail for invalid schema
    val invalidSchema = StructType(testKeys.map(createIntegerField) ++
      Seq(StructField("struct", StructType(Seq(StructField("value1", StringType, true))), true),
        StructField("value2", IntegerType, false)))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(testRow, invalidSchema).isDefined)
  }

  test("Handle special case for null variable-length Decimal") {
    val schema = StructType(StructField("d", DecimalType(19, 0), nullable = true) :: Nil)
    val unsafeRowProjection = UnsafeProjection.create(schema)
    val row = unsafeRowProjection(new SpecificInternalRow(schema))

    // row is empty at this point
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)

    // set Decimal field to precision-overflowed value
    val bigDecimalVal = Decimal(new JavaBigDecimal("12345678901234567890")) // precision=20, scale=0
    row.setDecimal(0, bigDecimalVal, 19) // should overflow and become null
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)

    // set Decimal field to valid non-null value
    val bigDecimalVal2 = Decimal(new JavaBigDecimal("1234567890123456789")) // precision=19, scale=0
    row.setDecimal(0, bigDecimalVal2, 19) // should succeed
    assert(!row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (16, 8))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)

    // set Decimal field to null explicitly, after which this field no longer supports updating
    row.setNullAt(0)
    assert(row.isNullAt(0) && UnsafeRowUtils.getOffsetAndSize(row, 0) == (0, 0))
    assert(UnsafeRowUtils.validateStructuralIntegrityWithReason(row, schema).isEmpty)
  }

  test("Better schema status message") {
    assert(UnsafeRowUtils.getStructuralIntegrityStatus(testRow, testOutputSchema)
      .contains("[UnsafeRowStatus] expectedSchema: StructType(" +
        "StructField(key1,IntegerType,false),StructField(key2,IntegerType,false)," +
        "StructField(sum(key1),IntegerType,false),StructField(sum(key2),IntegerType,false)), " +
        "expectedSchemaNumFields: 4, numFields: 4, bitSetWidthInBytes: 8, rowSizeInBytes: 40\n" +
        "fieldStatus:\n" +
        "[UnsafeRowFieldStatus] index: 0, expectedFieldType: IntegerType,"))
  }

  test("isBinaryStable on complex types containing collated strings") {
    val nonBinaryStringType = StringType(CollationFactory.collationNameToId("UTF8_LCASE"))

    // simple checks
    assert(UnsafeRowUtils.isBinaryStable(IntegerType))
    assert(UnsafeRowUtils.isBinaryStable(StringType))
    assert(!UnsafeRowUtils.isBinaryStable(nonBinaryStringType))

    assert(UnsafeRowUtils.isBinaryStable(ArrayType(IntegerType)))
    assert(UnsafeRowUtils.isBinaryStable(ArrayType(StringType)))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(nonBinaryStringType)))

    assert(UnsafeRowUtils.isBinaryStable(MapType(StringType, StringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(nonBinaryStringType, StringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(StringType, nonBinaryStringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(nonBinaryStringType, nonBinaryStringType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(nonBinaryStringType, IntegerType)))
    assert(!UnsafeRowUtils.isBinaryStable(MapType(IntegerType, nonBinaryStringType)))

    assert(UnsafeRowUtils.isBinaryStable(StructType(StructField("field", IntegerType) :: Nil)))
    assert(UnsafeRowUtils.isBinaryStable(StructType(StructField("field", StringType) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", nonBinaryStringType) :: Nil)))

    // nested complex types
    assert(UnsafeRowUtils.isBinaryStable(ArrayType(ArrayType(StringType))))
    assert(UnsafeRowUtils.isBinaryStable(ArrayType(MapType(StringType, IntegerType))))
    assert(UnsafeRowUtils.isBinaryStable(
      ArrayType(StructType(StructField("field", StringType) :: Nil))))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(ArrayType(nonBinaryStringType))))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(MapType(IntegerType, nonBinaryStringType))))
    assert(!UnsafeRowUtils.isBinaryStable(
      ArrayType(MapType(IntegerType, ArrayType(nonBinaryStringType)))))
    assert(!UnsafeRowUtils.isBinaryStable(
      ArrayType(StructType(StructField("field", nonBinaryStringType) :: Nil))))
    assert(!UnsafeRowUtils.isBinaryStable(ArrayType(StructType(
      Seq(StructField("second", IntegerType), StructField("second", nonBinaryStringType))))))

    assert(UnsafeRowUtils.isBinaryStable(MapType(ArrayType(StringType), ArrayType(IntegerType))))
    assert(UnsafeRowUtils.isBinaryStable(MapType(MapType(StringType, StringType), IntegerType)))
    assert(UnsafeRowUtils.isBinaryStable(
      MapType(StructType(StructField("field", StringType) :: Nil), IntegerType)))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(ArrayType(nonBinaryStringType), ArrayType(IntegerType))))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(IntegerType, ArrayType(nonBinaryStringType))))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(MapType(IntegerType, nonBinaryStringType), IntegerType)))
    assert(!UnsafeRowUtils.isBinaryStable(
      MapType(StructType(StructField("field", nonBinaryStringType) :: Nil), IntegerType)))

    assert(UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", ArrayType(IntegerType)) :: Nil)))
    assert(UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", MapType(StringType, IntegerType)) :: Nil)))
    assert(UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", StructType(StructField("sub", IntegerType) :: Nil)) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", ArrayType(nonBinaryStringType)) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field", MapType(nonBinaryStringType, IntegerType)) :: Nil)))
    assert(!UnsafeRowUtils.isBinaryStable(
      StructType(StructField("field",
        StructType(StructField("sub", nonBinaryStringType) :: Nil)) :: Nil)))
  }

  test("hash aggregate grouping key support for binary-unstable types") {
    val collatedString = StringType(CollationFactory.collationNameToId("UTF8_LCASE"))

    assert(Aggregate.supportsHashAggregateGroupingKey(collatedString))
    assert(Aggregate.supportsHashAggregateGroupingKey(ArrayType(collatedString)))
    assert(Aggregate.supportsHashAggregateGroupingKey(
      StructType(StructField("s", collatedString) :: Nil)))
    assert(!Aggregate.supportsHashAggregateGroupingKey(MapType(collatedString, IntegerType)))
    assert(Aggregate.supportsHashAggregateGroupingKey(StructType(Seq(
      StructField("s", collatedString),
      StructField("m", MapType(StringType, IntegerType))))))
    assert(!Aggregate.supportsHashAggregateGroupingKey(StructType(Seq(
      StructField("s", collatedString),
      StructField("m", MapType(collatedString, IntegerType))))))
  }

  test("UnsafeRowKeyOperations equality and hash contract") {
    case class KeyContractCase(
        name: String,
        schema: StructType,
        left: InternalRow,
        right: InternalRow,
        different: InternalRow)

    def collatedString(collationName: String): StringType = {
      StringType(CollationFactory.collationNameToId(collationName))
    }

    def utf8(value: String): UTF8String = UTF8String.fromString(value)

    def array(values: Any*): GenericArrayData = new GenericArrayData(values.toArray)

    val lcase = collatedString("UTF8_LCASE")
    val unicodeCI = collatedString("UNICODE_CI")
    val binaryRtrim = collatedString("UTF8_BINARY_RTRIM")
    val lcaseRtrim = collatedString("UTF8_LCASE_RTRIM")
    val geometry = GeometryType(4326)
    val geography = GeographyType(4326)
    val nestedType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("label", lcaseRtrim)))

    val testCases = Seq(
      KeyContractCase(
        "scalar LCASE string",
        StructType(StructField("s", lcase) :: Nil),
        InternalRow(utf8("AAA")),
        InternalRow(utf8("aaa")),
        InternalRow(utf8("bbb"))),
      KeyContractCase(
        "mixed primitive fields",
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("s", lcase),
          StructField("flag", BooleanType),
          StructField("count", LongType))),
        InternalRow(1, utf8("HELLO"), true, 10L),
        InternalRow(1, utf8("hello"), true, 10L),
        InternalRow(2, utf8("hello"), true, 10L)),
      KeyContractCase(
        "array with ICU collation and null element",
        StructType(StructField("a", ArrayType(unicodeCI)) :: Nil),
        InternalRow(array(utf8("HELLO"), null, utf8("WORLD"))),
        InternalRow(array(utf8("hello"), null, utf8("world"))),
        InternalRow(array(utf8("hello"), null, utf8("other")))),
      KeyContractCase(
        "nested array with ICU collation",
        StructType(StructField("a", ArrayType(ArrayType(unicodeCI))) :: Nil),
        InternalRow(array(array(utf8("HELLO")), array(utf8("WORLD")))),
        InternalRow(array(array(utf8("hello")), array(utf8("world")))),
        InternalRow(array(array(utf8("hello")), array(utf8("other"))))),
      KeyContractCase(
        "nested array with unequal lengths",
        StructType(StructField("a", ArrayType(ArrayType(unicodeCI))) :: Nil),
        InternalRow(array(array(utf8("HELLO")))),
        InternalRow(array(array(utf8("hello")))),
        InternalRow(array(array(utf8("hello"), utf8("world"))))),
      KeyContractCase(
        "nested empty and non-empty arrays",
        StructType(StructField("a", ArrayType(ArrayType(unicodeCI))) :: Nil),
        InternalRow(array(array())),
        InternalRow(array(array())),
        InternalRow(array(array(utf8("value"))))),
      KeyContractCase(
        "nested struct with LCASE RTRIM collation",
        StructType(StructField("nested", nestedType) :: Nil),
        InternalRow(InternalRow(7, utf8("VALUE"))),
        InternalRow(InternalRow(7, utf8("value   "))),
        InternalRow(InternalRow(8, utf8("value")))),
      KeyContractCase(
        "array of structs with LCASE RTRIM collation",
        StructType(StructField("a", ArrayType(nestedType)) :: Nil),
        InternalRow(array(InternalRow(7, utf8("VALUE")))),
        InternalRow(array(InternalRow(7, utf8("value   ")))),
        InternalRow(array(InternalRow(8, utf8("value"))))),
      KeyContractCase(
        "binary RTRIM string with different lengths",
        StructType(StructField("s", binaryRtrim) :: Nil),
        InternalRow(utf8("value")),
        InternalRow(utf8("value   ")),
        InternalRow(utf8("VALUE"))),
      KeyContractCase(
        "null collated field",
        StructType(Seq(StructField("s", unicodeCI), StructField("id", IntegerType))),
        InternalRow(null, 1),
        InternalRow(null, 1),
        InternalRow(utf8("value"), 1)),
      KeyContractCase(
        "collated string with opaque binary-stable fields",
        StructType(Seq(
          StructField("s", lcase),
          StructField("interval", CalendarIntervalType),
          StructField("geometry", geometry),
          StructField("geography", geography),
          StructField("payload", BinaryType))),
        InternalRow(
          utf8("HELLO"),
          new CalendarInterval(1, 2, 3),
          BinaryView.fromBytes(Array[Byte](1, 2, 3)),
          BinaryView.fromBytes(Array[Byte](4, 5, 6)),
          Array[Byte](7, 8, 9)),
        InternalRow(
          utf8("hello"),
          new CalendarInterval(1, 2, 3),
          BinaryView.fromBytes(Array[Byte](1, 2, 3)),
          BinaryView.fromBytes(Array[Byte](4, 5, 6)),
          Array[Byte](7, 8, 9)),
        InternalRow(
          utf8("hello"),
          new CalendarInterval(1, 2, 4),
          BinaryView.fromBytes(Array[Byte](1, 2, 3)),
          BinaryView.fromBytes(Array[Byte](4, 5, 6)),
          Array[Byte](7, 8, 9))))

    Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN).foreach {
      codegenMode =>
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode.toString) {
          testCases.foreach { testCase =>
            withClue(s"$codegenMode: ${testCase.name}: ") {
              val projection = UnsafeProjection.create(testCase.schema)
              val left = projection(testCase.left).copy()
              val right = projection(testCase.right).copy()
              val different = projection(testCase.different).copy()
              val keyOperations = new UnsafeRowKeyOperations(testCase.schema)

              assert(keyOperations.areEqual(left, right))
              assert(keyOperations.areEqual(right, left))
              assert(!keyOperations.areEqual(left, different))
              assert(!keyOperations.areEqual(different, left))
              assert(keyOperations.hash(left) == keyOperations.hash(right))

              val serializedOperations = keyOperations.create()
              assert(serializedOperations.equals(
                left.getBaseObject,
                left.getBaseOffset,
                left.getSizeInBytes,
                right.getBaseObject,
                right.getBaseOffset,
                right.getSizeInBytes))
              assert(serializedOperations.equals(
                right.getBaseObject,
                right.getBaseOffset,
                right.getSizeInBytes,
                left.getBaseObject,
                left.getBaseOffset,
                left.getSizeInBytes))
              assert(!serializedOperations.equals(
                left.getBaseObject,
                left.getBaseOffset,
                left.getSizeInBytes,
                different.getBaseObject,
                different.getBaseOffset,
                different.getSizeInBytes))
              assert(serializedOperations.hash(
                left.getBaseObject,
                left.getBaseOffset,
                left.getSizeInBytes) == serializedOperations.hash(
                right.getBaseObject,
                right.getBaseOffset,
                right.getSizeInBytes))
            }
          }
        }
    }
  }

  test("UnsafeRowKeyOperations raw ICU keys match materialized sort-key hashes") {
    // scalastyle:off nonascii
    val values = Seq(
      "a" * 4096,
      "short",
      "Résumé",
      "resume   ",
      "")
    // scalastyle:on nonascii

    Seq("UNICODE", "UNICODE_CI", "UNICODE_RTRIM", "UNICODE_CI_RTRIM").foreach {
      collationName =>
        val stringType = StringType(CollationFactory.collationNameToId(collationName))
        val schema = StructType(StructField("value", stringType) :: Nil)
        val projection = UnsafeProjection.create(schema)
        val keyOperations = new UnsafeRowKeyOperations(schema)
        val serializedOperations = keyOperations.create()
        val collation = CollationFactory.fetchCollation(stringType.collationId)

        values.foreach { value =>
          val row = projection(InternalRow(UTF8String.fromString(value))).copy()
          val materializedKey = collation.sortKeyFunction.apply(row.getUTF8String(0))
          val expectedHash = Murmur3_x86_32.hashUnsafeBytes(
            materializedKey,
            Platform.BYTE_ARRAY_OFFSET,
            materializedKey.length,
            42)

          withClue(s"$collationName: $value: ") {
            assert(keyOperations.hash(row) == expectedHash)
            assert(serializedOperations.hash(
              row.getBaseObject,
              row.getBaseOffset,
              row.getSizeInBytes) == expectedHash)
          }
        }
    }
  }

  test("PaddingProvider handles endianness") {
    // The following arrays contain the same 8 byte field values as represented
    // in memory on little endian and big endian platforms. They are
    // what would be seen in memory if the following values were set:
    //
    //   row.setBoolean(0, true)
    //   row.setByte(1, 0xFF.toByte)
    //   row.setShort(2, 0xFFFE.toShort)
    //   row.setInt(3, 0xFFFEFDFC.toInt)
    //   row.setFloat(4, java.lang.Float.intBitsToFloat(0xFF7FFDFC.toInt))
    //
    // Note that in either platform endianness, the values are placed in the first N bytes
    // of the 8 byte field. The difference is in the order of the bytes in the field.
    val boolLE = Array[Byte](1, 0, 0, 0, 0, 0, 0, 0)
    val byteLE = Array[Byte](0xFF.toByte, 0, 0, 0, 0, 0, 0, 0)
    val shortLE = Array[Byte](0xFE.toByte, 0xFF.toByte, 0, 0, 0, 0, 0, 0)
    val intLE = Array[Byte](0xFC.toByte, 0xFD.toByte, 0xFE.toByte, 0xFF.toByte, 0, 0, 0, 0)
    val floatLE = Array[Byte](0xFC.toByte, 0xFD.toByte, 0x7F.toByte, 0xFF.toByte, 0, 0, 0, 0)

    val boolBE = Array[Byte](1, 0, 0, 0, 0, 0, 0, 0)
    val byteBE = Array[Byte](0xFF.toByte, 0, 0, 0, 0, 0, 0, 0)
    val shortBE = Array[Byte](0xFF.toByte, 0xFE.toByte, 0, 0, 0, 0, 0, 0)
    val intBE = Array[Byte](0xFF.toByte, 0xFE.toByte, 0xFD.toByte, 0xFC.toByte, 0, 0, 0, 0)
    val floatBE = Array[Byte](0xFF.toByte, 0x7F.toByte, 0xFD.toByte, 0xFC.toByte, 0, 0, 0, 0)

    // Corrupt field values
    val boolCorruptLE = Array[Byte](2, 0, 0, 0, 0, 0, 0, 0)
    val byteCorruptLE = Array[Byte](0xFF.toByte, 1, 0, 0, 0, 0, 0, 0)
    val shortCorruptLE = Array[Byte](0xFE.toByte, 0xFF.toByte, 1, 0, 0, 0, 0, 0)
    val intCorruptLE = Array[Byte](0xFC.toByte, 0xFD.toByte, 0xFE.toByte, 0xFF.toByte, 1, 0, 0, 0)
    val floatCorruptLE = Array[Byte](0xFC.toByte, 0xFD.toByte, 0x7F.toByte, 0xFF.toByte, 1, 0, 0, 0)

    val boolCorruptBE = Array[Byte](2, 0, 0, 0, 0, 0, 0, 0)
    val byteCorruptBE = Array[Byte](0xFF.toByte, 1, 0, 0, 0, 0, 0, 0)
    val shortCorruptBE = Array[Byte](0xFF.toByte, 0xFE.toByte, 1, 0, 0, 0, 0, 0)
    val intCorruptBE = Array[Byte](0xFF.toByte, 0xFE.toByte, 0xFD.toByte, 0xFC.toByte, 1, 0, 0, 0)
    val floatCorruptBE = Array[Byte](0xFF.toByte, 0x7F.toByte, 0xFD.toByte, 0xFC.toByte, 1, 0, 0, 0)

    val numFields = 5
    val fieldsStartIdx = UnsafeRow.calculateBitSetWidthInBytes(numFields)
    val sizeInBytes = fieldsStartIdx + (numFields * 8)
    val data = new Array[Byte](sizeInBytes)
    val row = new UnsafeRow(numFields)
    row.pointTo(data, sizeInBytes)

    // Set all bits of all fields to 1 to ensure we don't miss overwiting any fields later
    row.setLong(0, 0xFFFFFFFFFFFFFFFFL)
    row.setLong(1, 0xFFFFFFFFFFFFFFFFL)
    row.setLong(2, 0xFFFFFFFFFFFFFFFFL)
    row.setLong(3, 0xFFFFFFFFFFFFFFFFL)
    row.setLong(4, 0xFFFFFFFFFFFFFFFFL)

    // The PaddingProvider implementations get the full 8 bytes of the field using
    // UnsafeRow.getLong(n) which is platform endianness dependent.
    // When testing PaddingProviderBE on little endian platforms, the big endian byte arrays
    // must be reversed so that UnsafeRow.getLong(n) returns the long value that would be seen
    // on a big endian platform.
    // The opposite is true when testing PaddingProviderLE on big endian platforms.
    def overwrite(src: Array[Byte], fieldIdx: Int, reverse: Boolean): Unit = {
      Array.copy(if (reverse) src.reverse else src, 0, data, fieldsStartIdx + (fieldIdx * 8), 8)
    }

    // Overwrite the row's data with raw little endian values
    // reversing the byte order if testing on big endian platforms.
    overwrite(boolLE, 0, nativeOrder() == BIG_ENDIAN)
    overwrite(byteLE, 1, nativeOrder() == BIG_ENDIAN)
    overwrite(shortLE, 2, nativeOrder() == BIG_ENDIAN)
    overwrite(intLE, 3, nativeOrder() == BIG_ENDIAN)
    overwrite(floatLE, 4, nativeOrder() == BIG_ENDIAN)

    assert(UnsafeRowUtils.PaddingProviderLE.getPaddingBoolean(row, 0) == 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 1, 8) == 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 2, 16) == 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 3, 32) == 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 4, 32) == 0)

    overwrite(boolCorruptLE, 0, nativeOrder() == BIG_ENDIAN)
    overwrite(byteCorruptLE, 1, nativeOrder() == BIG_ENDIAN)
    overwrite(shortCorruptLE, 2, nativeOrder() == BIG_ENDIAN)
    overwrite(intCorruptLE, 3, nativeOrder() == BIG_ENDIAN)
    overwrite(floatCorruptLE, 4, nativeOrder() == BIG_ENDIAN)

    assert(UnsafeRowUtils.PaddingProviderLE.getPaddingBoolean(row, 0) != 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 1, 8) != 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 2, 16) != 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 3, 32) != 0)
    assert(UnsafeRowUtils.PaddingProviderLE.getPadding(row, 4, 32) != 0)

    // Overwrite the row's data with raw big endian values
    // reversing the byte order if testing on little endian platforms.
    overwrite(boolBE, 0, nativeOrder() != BIG_ENDIAN)
    overwrite(byteBE, 1, nativeOrder() != BIG_ENDIAN)
    overwrite(shortBE, 2, nativeOrder() != BIG_ENDIAN)
    overwrite(intBE, 3, nativeOrder() != BIG_ENDIAN)
    overwrite(floatBE, 4, nativeOrder() != BIG_ENDIAN)

    assert(UnsafeRowUtils.PaddingProviderBE.getPaddingBoolean(row, 0) == 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 1, 8) == 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 2, 16) == 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 3, 32) == 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 4, 32) == 0)

    overwrite(boolCorruptBE, 0, nativeOrder() != BIG_ENDIAN)
    overwrite(byteCorruptBE, 1, nativeOrder() != BIG_ENDIAN)
    overwrite(shortCorruptBE, 2, nativeOrder() != BIG_ENDIAN)
    overwrite(intCorruptBE, 3, nativeOrder() != BIG_ENDIAN)
    overwrite(floatCorruptBE, 4, nativeOrder() != BIG_ENDIAN)

    assert(UnsafeRowUtils.PaddingProviderBE.getPaddingBoolean(row, 0) != 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 1, 8) != 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 2, 16) != 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 3, 32) != 0)
    assert(UnsafeRowUtils.PaddingProviderBE.getPadding(row, 4, 32) != 0)
  }
}
