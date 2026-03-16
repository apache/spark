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

package org.apache.spark.sql.execution.arrow

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.YearUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder.{encoderFor => toRowEncoder}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.{Geography => InternalGeography, Geometry => InternalGeometry}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized._
import org.apache.spark.unsafe.types.{CalendarInterval, GeographyVal, GeometryVal, UTF8String}
import org.apache.spark.util.MaybeNull

class ArrowWriterSuite extends SparkFunSuite {

  test("simple") {
    def check(
        dt: DataType,
        data: Seq[Any],
        timeZoneId: String = null,
        largeVarTypes: Boolean = false): Unit = {
      val datatype = dt match {
        case _: DayTimeIntervalType => DayTimeIntervalType()
        case _: YearMonthIntervalType => YearMonthIntervalType()
        case _: TimeType => TimeType()
        case u: UserDefinedType[_] => u.sqlType
        case tpe => tpe
      }
      val schema = new StructType().add("value", datatype, nullable = true)
      val writer = ArrowWriter.create(schema, timeZoneId)
      assert(writer.schema === schema)

      data.foreach { datum =>
        writer.write(InternalRow(datum))
      }
      writer.finish()

      val dataModified = data.map { datum =>
        dt match {
          case _: GeometryType => datum.asInstanceOf[GeometryVal].getBytes
          case _: GeographyType => datum.asInstanceOf[GeographyVal].getBytes
          case _ => datum
        }
      }

      val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))
      dataModified.zipWithIndex.foreach {
        case (null, rowId) => assert(reader.isNullAt(rowId))
        case (datum, rowId) =>
          val value = datatype match {
            case BooleanType => reader.getBoolean(rowId)
            case ByteType => reader.getByte(rowId)
            case ShortType => reader.getShort(rowId)
            case IntegerType => reader.getInt(rowId)
            case LongType => reader.getLong(rowId)
            case FloatType => reader.getFloat(rowId)
            case DoubleType => reader.getDouble(rowId)
            case DecimalType.Fixed(precision, scale) => reader.getDecimal(rowId, precision, scale)
            case StringType => reader.getUTF8String(rowId)
            case BinaryType => reader.getBinary(rowId)
            case DateType => reader.getInt(rowId)
            case TimestampType => reader.getLong(rowId)
            case TimestampNTZType => reader.getLong(rowId)
            case _: TimeType => reader.getLong(rowId)
            case _: YearMonthIntervalType => reader.getInt(rowId)
            case _: DayTimeIntervalType => reader.getLong(rowId)
            case CalendarIntervalType => reader.getInterval(rowId)
            case _: GeometryType => reader.getGeometry(rowId).getBytes
            case _: GeographyType => reader.getGeography(rowId).getBytes
          }
          assert(value === datum)
      }

      writer.root.close()
    }

    val wkbs = Seq("010100000000000000000031400000000000001c40",
      "010100000000000000000034400000000000003540")
      .map { x =>
        x.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    }

    val geographies = wkbs.map(x => InternalGeography.fromWkb(x, 4326).getValue)
    val geometries = wkbs.map(x => InternalGeometry.fromWkb(x, 0).getValue)
    val mixedGeometries = wkbs.zip(Seq(0, 4326)).map {
      case (g, srid) => InternalGeometry.fromWkb(g, srid).getValue
    }

    check(GeometryType(0), geometries)
    check(GeographyType(4326), geographies)
    check(GeometryType("ANY"), mixedGeometries)
    check(GeographyType("ANY"), geographies)
    check(BooleanType, Seq(true, null, false))
    check(ByteType, Seq(1.toByte, 2.toByte, null, 4.toByte))
    check(ShortType, Seq(1.toShort, 2.toShort, null, 4.toShort))
    check(IntegerType, Seq(1, 2, null, 4))
    check(LongType, Seq(1L, 2L, null, 4L))
    check(FloatType, Seq(1.0f, 2.0f, null, 4.0f))
    check(DoubleType, Seq(1.0d, 2.0d, null, 4.0d))
    check(DecimalType.SYSTEM_DEFAULT, Seq(Decimal(1), Decimal(2), null, Decimal(4)))
    check(StringType, Seq("a", "b", null, "d").map(UTF8String.fromString))
    check(StringType, Seq("a", "b", null, "d").map(UTF8String.fromString), null, true)
    check(BinaryType, Seq("a".getBytes(), "b".getBytes(), null, "d".getBytes()))
    check(BinaryType, Seq("a".getBytes(), "b".getBytes(), null, "d".getBytes()), null, true)
    check(DateType, Seq(0, 1, 2, null, 4))
    check(TimestampType, Seq(0L, 3.6e9.toLong, null, 8.64e10.toLong), "America/Los_Angeles")
    check(TimestampNTZType, Seq(0L, 3.6e9.toLong, null, 8.64e10.toLong))
    DataTypeTestUtils.timeTypes.foreach(check(_, Seq(0L, 4.32e4.toLong, null, 3723123456789L)))
    check(NullType, Seq(null, null, null))
    DataTypeTestUtils.yearMonthIntervalTypes
      .foreach(check(_, Seq(null, 0, 1, -1, Int.MaxValue, Int.MinValue)))
    DataTypeTestUtils.dayTimeIntervalTypes.foreach(check(_,
      Seq(null, 0L, 1000L, -1000L, (Long.MaxValue - 807L), (Long.MinValue + 808L))))
    check(CalendarIntervalType,
      Seq(new CalendarInterval(1, 2, 3),
        new CalendarInterval(11, 22, 33),
        new CalendarInterval(-1, -2, -3),
        new CalendarInterval(-11, -22, -33),
        null))
    check(new YearUDT, Seq(2020, 2021, null, 2022))
  }

  test("nested geographies") {
    def check(
      dt: StructType,
      data: Seq[InternalRow]): Unit = {
      val writer = ArrowWriter.create(dt.asInstanceOf[StructType], "UTC")

      // Write data to arrow.
      data.toSeq.foreach { datum =>
        writer.write(datum)
      }
      writer.finish()

      // Create arrow vector readers.
      val vectors = writer.root.getFieldVectors.asScala
        .map { new ArrowColumnVector(_) }.toArray.asInstanceOf[Array[ColumnVector]]

      val batch = new ColumnarBatch(vectors, writer.root.getRowCount.toInt)

      data.zipWithIndex.foreach { case (datum, i) =>
        // Read data from arrow.
        val internalRow = batch.getRow(i)

        // All nullable results first must check whether the value is null.
        if (datum.getStruct(0, 4) == null || internalRow.getStruct(0, 4) == null) {
          assert(datum.getStruct(0, 4) == null && internalRow.getStruct(0, 4) == null)
        } else {
          val expectedStruct = datum.getStruct(0, 4)
          val actualStruct = internalRow.getStruct(0, 4)
          assert(expectedStruct.getInt(0) === actualStruct.getInt(0))
          assert(expectedStruct.getInt(2) === actualStruct.getInt(2))

          if (expectedStruct.getGeography(1) == null ||
            actualStruct.getGeography(1) == null) {
            assert(expectedStruct.getGeography(1) == null && actualStruct.getGeography(1) == null)
          } else {
            assert(expectedStruct.getGeography(1).getBytes ===
              actualStruct.getGeography(1).getBytes)
          }
          if (expectedStruct.getGeography(3) == null ||
            actualStruct.getGeography(3) == null) {
            assert(expectedStruct.getGeography(3) == null && actualStruct.getGeography(3) == null)
          } else {
            assert(expectedStruct.getGeography(3).getBytes ===
              actualStruct.getGeography(3).getBytes)
          }

          if (datum.getArray(1) == null ||
            internalRow.getArray(1) == null) {
            assert(internalRow.getArray(1) == null && datum.getArray(1) == null)
          } else {
            internalRow.getArray(1).toSeq[GeographyVal](GeographyType(4326))
              .zip(datum.getArray(1).toSeq[GeographyVal](GeographyType(4326))).foreach {
                case (actual, expected) =>
                  assert(actual.getBytes === expected.getBytes)
              }
          }

          if (datum.getMap(2) == null ||
            internalRow.getMap(2) == null) {
            assert(internalRow.getMap(2) == null && datum.getMap(2) == null)
          } else {
            assert(internalRow.getMap(2).keyArray().toSeq(StringType) ===
              datum.getMap(2).keyArray().toSeq(StringType))
            internalRow.getMap(2).valueArray().toSeq[GeographyVal](GeographyType("ANY"))
              .zip(datum.getMap(2).valueArray().toSeq[GeographyVal](GeographyType("ANY"))).foreach {
                case (actual, expected) =>
                  assert((actual == null && expected == null) ||
                    actual.getBytes === expected.getBytes)
              }
          }
        }
      }

      writer.root.close()
    }

    val point1 = "010100000000000000000031400000000000001C40"
      .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    val point2 = "010100000000000000000035400000000000001E40"
      .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

    val schema = new StructType()
      .add(
        "s",
        new StructType()
          .add("i1", "int")
          .add("g0", "geography(4326)")
          .add("i2", "int")
          .add("g1", "geography(4326)"))
      .add("a", "array<geography(4326)>")
      .add("m", "map<string, geography(ANY)>")

    val maybeNull5 = MaybeNull(5)
    val maybeNull7 = MaybeNull(7)
    val maybeNull11 = MaybeNull(11)
    val maybeNull13 = MaybeNull(13)
    val maybeNull17 = MaybeNull(17)

    val nestedGeographySerializer = ExpressionEncoder(toRowEncoder(schema)).createSerializer()
    val data = Iterator
      .tabulate(100)(i =>
        nestedGeographySerializer.apply(
          (Row(
            maybeNull5(
              Row(
                i,
                maybeNull7(org.apache.spark.sql.types.Geography.fromWKB(point1)),
                i + 1,
                maybeNull11(org.apache.spark.sql.types.Geography.fromWKB(point2, 4326)))),
            maybeNull7((0 until 10).map(j =>
              org.apache.spark.sql.types.Geography.fromWKB(point2, 4326))),
            maybeNull13(
              Map((i.toString, maybeNull17(
                org.apache.spark.sql.types.Geography.fromWKB(point1, 4326)))))))))
      .map(_.copy()).toSeq

    check(schema, data)
  }

  test("nested geometries") {
    def check(
      dt: StructType,
      data: Seq[InternalRow]): Unit = {
      val writer = ArrowWriter.create(dt.asInstanceOf[StructType], "UTC")

      // Write data to arrow.
      data.toSeq.foreach { datum =>
        writer.write(datum)
      }
      writer.finish()

      // Create arrow vector readers.
      val vectors = writer.root.getFieldVectors.asScala
        .map { new ArrowColumnVector(_) }.toArray.asInstanceOf[Array[ColumnVector]]

      val batch = new ColumnarBatch(vectors, writer.root.getRowCount.toInt)
      data.zipWithIndex.foreach { case (datum, i) =>
        // Read data from arrow.
        val internalRow = batch.getRow(i)

        // All nullable results first must check whether the value is null.
        if (datum.getStruct(0, 4) == null || internalRow.getStruct(0, 4) == null) {
          assert(datum.getStruct(0, 4) == null && internalRow.getStruct(0, 4) == null)
        } else {
          val expectedStruct = datum.getStruct(0, 4)
          val actualStruct = internalRow.getStruct(0, 4)
          assert(expectedStruct.getInt(0) === actualStruct.getInt(0))
          assert(expectedStruct.getInt(2) === actualStruct.getInt(2))

          if (expectedStruct.getGeometry(1) == null ||
            actualStruct.getGeometry(1) == null) {
            assert(expectedStruct.getGeometry(1) == null && actualStruct.getGeometry(1) == null)
          } else {
            assert(expectedStruct.getGeometry(1).getBytes ===
              actualStruct.getGeometry(1).getBytes)
          }
          if (expectedStruct.getGeometry(3) == null ||
            actualStruct.getGeometry(3) == null) {
            assert(expectedStruct.getGeometry(3) == null && actualStruct.getGeometry(3) == null)
          } else {
            assert(expectedStruct.getGeometry(3).getBytes ===
              actualStruct.getGeometry(3).getBytes)
          }

          if (datum.getArray(1) == null ||
            internalRow.getArray(1) == null) {
            assert(internalRow.getArray(1) == null && datum.getArray(1) == null)
          } else {
            internalRow.getArray(1).toSeq[GeometryVal](GeometryType(0))
              .zip(datum.getArray(1).toSeq[GeometryVal](GeometryType(0))).foreach {
                case (actual, expected) =>
                  assert(actual.getBytes === expected.getBytes)
              }
          }

          if (datum.getMap(2) == null ||
            internalRow.getMap(2) == null) {
            assert(internalRow.getMap(2) == null && datum.getMap(2) == null)
          } else {
            assert(internalRow.getMap(2).keyArray().toSeq(StringType) ===
              datum.getMap(2).keyArray().toSeq(StringType))
            internalRow.getMap(2).valueArray().toSeq[GeometryVal](GeometryType("ANY"))
              .zip(datum.getMap(2).valueArray().toSeq[GeometryVal](GeometryType("ANY"))).foreach {
                case (actual, expected) =>
                  assert((actual == null && expected == null) ||
                    actual.getBytes === expected.getBytes)
              }
          }
        }
      }

      writer.root.close()
    }

    val point1 = "010100000000000000000031400000000000001C40"
      .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    val point2 = "010100000000000000000035400000000000001E40"
      .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

    val schema = new StructType()
      .add(
        "s",
        new StructType()
          .add("i1", "int")
          .add("g0", "geometry(0)")
          .add("i2", "int")
          .add("g4326", "geometry(4326)"))
      .add("a", "array<geometry(0)>")
      .add("m", "map<string, geometry(ANY)>")

    val maybeNull5 = MaybeNull(5)
    val maybeNull7 = MaybeNull(7)
    val maybeNull11 = MaybeNull(11)
    val maybeNull13 = MaybeNull(13)
    val maybeNull17 = MaybeNull(17)

    val nestedGeometrySerializer = ExpressionEncoder(toRowEncoder(schema)).createSerializer()
    val data = Iterator
      .tabulate(100) { i =>
        val mixedSrid = if (i % 2 == 0) 0 else 4326

        nestedGeometrySerializer.apply(
          (Row(
            maybeNull5(
              Row(
                i,
                maybeNull7(org.apache.spark.sql.types.Geometry.fromWKB(point1, 0)),
                i + 1,
                maybeNull11(org.apache.spark.sql.types.Geometry.fromWKB(point2, 4326)))),
            maybeNull7((0 until 10).map(j =>
              org.apache.spark.sql.types.Geometry.fromWKB(point2, 0))),
            maybeNull13(
              Map((i.toString, maybeNull17(
                org.apache.spark.sql.types.Geometry.fromWKB(point1, mixedSrid))))))))
      }.map(_.copy()).toSeq

    check(schema, data)
  }

  test("get multiple") {
    def check(dt: DataType, data: Seq[Any], timeZoneId: String = null): Unit = {
      val datatype = dt match {
        case _: DayTimeIntervalType => DayTimeIntervalType()
        case _: YearMonthIntervalType => YearMonthIntervalType()
        case _: TimeType => TimeType()
        case u: UserDefinedType[_] => u.sqlType
        case tpe => tpe
      }
      val schema = new StructType().add("value", datatype, nullable = false)
      val writer = ArrowWriter.create(schema, timeZoneId)
      assert(writer.schema === schema)

      data.foreach { datum =>
        writer.write(InternalRow(datum))
      }
      writer.finish()

      val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))
      val values = datatype match {
        case BooleanType => reader.getBooleans(0, data.size)
        case ByteType => reader.getBytes(0, data.size)
        case ShortType => reader.getShorts(0, data.size)
        case IntegerType => reader.getInts(0, data.size)
        case LongType => reader.getLongs(0, data.size)
        case FloatType => reader.getFloats(0, data.size)
        case DoubleType => reader.getDoubles(0, data.size)
        case DateType => reader.getInts(0, data.size)
        case TimestampType => reader.getLongs(0, data.size)
        case TimestampNTZType => reader.getLongs(0, data.size)
        case _: TimeType => reader.getLongs(0, data.size)
        case _: YearMonthIntervalType => reader.getInts(0, data.size)
        case _: DayTimeIntervalType => reader.getLongs(0, data.size)
      }
      assert(values === data)

      writer.root.close()
    }
    check(BooleanType, Seq(true, false))
    check(ByteType, (0 until 10).map(_.toByte))
    check(ShortType, (0 until 10).map(_.toShort))
    check(IntegerType, (0 until 10))
    check(LongType, (0 until 10).map(_.toLong))
    check(FloatType, (0 until 10).map(_.toFloat))
    check(DoubleType, (0 until 10).map(_.toDouble))
    check(DateType, (0 until 10))
    check(TimestampType, (0 until 10).map(_ * 4.32e10.toLong), "America/Los_Angeles")
    check(TimestampNTZType, (0 until 10).map(_ * 4.32e10.toLong))
    DataTypeTestUtils.timeTypes.foreach(check(_, (0 until 10).map(_ * 4.32e10.toLong)))
    DataTypeTestUtils.yearMonthIntervalTypes.foreach(check(_, (0 until 14)))
    DataTypeTestUtils.dayTimeIntervalTypes.foreach(check(_, (-10 until 10).map(_ * 1000.toLong)))
    check(new YearUDT, 2018 to 2029)
  }

  test("write multiple, over initial capacity") {
    def createArrowWriter(
        schema: StructType,
        timeZoneId: String): (ArrowWriter, Int) = {
      val arrowSchema =
        ArrowUtils.toArrowSchema(
          schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
      val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
      val vector = root.getFieldVectors.get(0)
      vector.allocateNew()
      val cap = vector.getValueCapacity
      val writer = new ArrowWriter(root, Array(ArrowWriter.createFieldWriter(vector)))
      (writer, cap)
    }
    def check(dt: DataType, data: Seq[Any], timeZoneId: String = null): Unit = {
      val dataType = dt match {
        case _: DayTimeIntervalType => DayTimeIntervalType()
        case _: YearMonthIntervalType => YearMonthIntervalType()
        case tpe => tpe
      }
      val schema = new StructType().add("value", dataType, nullable = false)
      val (writer, initialCapacity) = createArrowWriter(schema, timeZoneId)

      assert(writer.schema === schema)

      // Write more values than the initial capacity of the vector
      val iterations = (initialCapacity / data.length) + 1
      (0 until iterations).foreach { _ =>
        data.foreach { datum =>
          writer.write(InternalRow(datum))
        }
      }
      writer.finish()

      val reader = new ArrowColumnVector(writer.root.getFieldVectors.get(0))
      (0 until iterations)
        .map(i => i * data.size)
        .foreach { offset =>
        val values = dt match {
          case BooleanType => reader.getBooleans(offset, data.size)
          case ByteType => reader.getBytes(offset, data.size)
          case ShortType => reader.getShorts(offset, data.size)
          case IntegerType => reader.getInts(offset, data.size)
          case LongType => reader.getLongs(offset, data.size)
          case FloatType => reader.getFloats(offset, data.size)
          case DoubleType => reader.getDoubles(offset, data.size)
          case DateType => reader.getInts(offset, data.size)
          case TimestampType => reader.getLongs(offset, data.size)
          case TimestampNTZType => reader.getLongs(offset, data.size)
          case _: YearMonthIntervalType => reader.getInts(offset, data.size)
          case _: DayTimeIntervalType => reader.getLongs(offset, data.size)
        }
        assert(values === data)
      }
      writer.root.close()
    }
    check(BooleanType, Seq(true, false))
    check(ByteType, (0 until 10).map(_.toByte))
    check(ShortType, (0 until 10).map(_.toShort))
    check(IntegerType, (0 until 10))
    check(LongType, (0 until 10).map(_.toLong))
    check(FloatType, (0 until 10).map(_.toFloat))
    check(DoubleType, (0 until 10).map(_.toDouble))
    check(DateType, (0 until 10))
    check(TimestampType, (0 until 10).map(_ * 4.32e10.toLong), "America/Los_Angeles")
    check(TimestampNTZType, (0 until 10).map(_ * 4.32e10.toLong))
    DataTypeTestUtils.yearMonthIntervalTypes.foreach(check(_, (0 until 14)))
    DataTypeTestUtils.dayTimeIntervalTypes.foreach(check(_, (-10 until 10).map(_ * 1000.toLong)))
  }

  test("array") {
    val schema = new StructType()
      .add("arr", ArrayType(IntegerType, containsNull = true), nullable = true)
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema === schema)

    writer.write(InternalRow(ArrayData.toArrayData(Array(1, 2, 3))))
    writer.write(InternalRow(ArrayData.toArrayData(Array(4, 5))))
    writer.write(InternalRow(null))
    writer.write(InternalRow(ArrayData.toArrayData(Array.empty[Int])))
    writer.write(InternalRow(ArrayData.toArrayData(Array(6, null, 8))))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))

    val array0 = reader.getArray(0)
    assert(array0.numElements() === 3)
    assert(array0.getInt(0) === 1)
    assert(array0.getInt(1) === 2)
    assert(array0.getInt(2) === 3)

    val array1 = reader.getArray(1)
    assert(array1.numElements() === 2)
    assert(array1.getInt(0) === 4)
    assert(array1.getInt(1) === 5)

    assert(reader.isNullAt(2))

    val array3 = reader.getArray(3)
    assert(array3.numElements() === 0)

    val array4 = reader.getArray(4)
    assert(array4.numElements() === 3)
    assert(array4.getInt(0) === 6)
    assert(array4.isNullAt(1))
    assert(array4.getInt(2) === 8)

    writer.root.close()
  }

  test("nested array") {
    val schema = new StructType().add("nested", ArrayType(ArrayType(IntegerType)))
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema === schema)

    writer.write(InternalRow(ArrayData.toArrayData(Array(
      ArrayData.toArrayData(Array(1, 2, 3)),
      ArrayData.toArrayData(Array(4, 5)),
      null,
      ArrayData.toArrayData(Array.empty[Int]),
      ArrayData.toArrayData(Array(6, null, 8))))))
    writer.write(InternalRow(null))
    writer.write(InternalRow(ArrayData.toArrayData(Array.empty)))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))

    val array0 = reader.getArray(0)
    assert(array0.numElements() === 5)

    val array00 = array0.getArray(0)
    assert(array00.numElements() === 3)
    assert(array00.getInt(0) === 1)
    assert(array00.getInt(1) === 2)
    assert(array00.getInt(2) === 3)

    val array01 = array0.getArray(1)
    assert(array01.numElements() === 2)
    assert(array01.getInt(0) === 4)
    assert(array01.getInt(1) === 5)

    assert(array0.isNullAt(2))

    val array03 = array0.getArray(3)
    assert(array03.numElements() === 0)

    val array04 = array0.getArray(4)
    assert(array04.numElements() === 3)
    assert(array04.getInt(0) === 6)
    assert(array04.isNullAt(1))
    assert(array04.getInt(2) === 8)

    assert(reader.isNullAt(1))

    val array2 = reader.getArray(2)
    assert(array2.numElements() === 0)

    writer.root.close()
  }

  test("null array") {
    val schema = new StructType()
      .add("arr", ArrayType(NullType, containsNull = true), nullable = true)
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema === schema)

    writer.write(InternalRow(ArrayData.toArrayData(Array(null, null, null))))
    writer.write(InternalRow(ArrayData.toArrayData(Array(null, null))))
    writer.write(InternalRow(null))
    writer.write(InternalRow(ArrayData.toArrayData(Array.empty[Int])))
    writer.write(InternalRow(ArrayData.toArrayData(Array(null, null, null))))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))

    val array0 = reader.getArray(0)
    assert(array0.numElements() === 3)
    assert(array0.isNullAt(0))
    assert(array0.isNullAt(1))
    assert(array0.isNullAt(2))

    val array1 = reader.getArray(1)
    assert(array1.numElements() === 2)
    assert(array1.isNullAt(0))
    assert(array1.isNullAt(1))

    assert(reader.isNullAt(2))

    val array3 = reader.getArray(3)
    assert(array3.numElements() === 0)

    val array4 = reader.getArray(4)
    assert(array4.numElements() === 3)
    assert(array4.isNullAt(0))
    assert(array4.isNullAt(1))
    assert(array4.isNullAt(2))

    writer.root.close()
  }

  test("struct") {
    val schema = new StructType()
      .add("struct", new StructType().add("i", IntegerType).add("str", StringType))
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema === schema)

    writer.write(InternalRow(InternalRow(1, UTF8String.fromString("str1"))))
    writer.write(InternalRow(InternalRow(null, null)))
    writer.write(InternalRow(null))
    writer.write(InternalRow(InternalRow(4, null)))
    writer.write(InternalRow(InternalRow(null, UTF8String.fromString("str5"))))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))

    val struct0 = reader.getStruct(0)
    assert(struct0.getInt(0) === 1)
    assert(struct0.getUTF8String(1) === UTF8String.fromString("str1"))

    val struct1 = reader.getStruct(1)
    assert(struct1.isNullAt(0))
    assert(struct1.isNullAt(1))

    assert(reader.isNullAt(2))

    val struct3 = reader.getStruct(3)
    assert(struct3.getInt(0) === 4)
    assert(struct3.isNullAt(1))

    val struct4 = reader.getStruct(4)
    assert(struct4.isNullAt(0))
    assert(struct4.getUTF8String(1) === UTF8String.fromString("str5"))

    writer.root.close()
  }

  test("nested struct") {
    val schema = new StructType().add("struct",
      new StructType().add("nested", new StructType().add("i", IntegerType).add("str", StringType)))
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema === schema)

    writer.write(InternalRow(InternalRow(InternalRow(1, UTF8String.fromString("str1")))))
    writer.write(InternalRow(InternalRow(InternalRow(null, null))))
    writer.write(InternalRow(InternalRow(null)))
    writer.write(InternalRow(null))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))

    val struct00 = reader.getStruct(0).getStruct(0, 2)
    assert(struct00.getInt(0) === 1)
    assert(struct00.getUTF8String(1) === UTF8String.fromString("str1"))

    val struct10 = reader.getStruct(1).getStruct(0, 2)
    assert(struct10.isNullAt(0))
    assert(struct10.isNullAt(1))

    val struct2 = reader.getStruct(2)
    assert(struct2.isNullAt(0))

    assert(reader.isNullAt(3))

    writer.root.close()
  }

  test("null struct") {
    val schema = new StructType()
      .add("struct", new StructType().add("n1", NullType).add("n2", NullType))
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema === schema)

    writer.write(InternalRow(InternalRow(null, null)))
    writer.write(InternalRow(null))
    writer.write(InternalRow(InternalRow(null, null)))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors().get(0))

    val struct0 = reader.getStruct(0)
    assert(struct0.isNullAt(0))
    assert(struct0.isNullAt(1))

    assert(reader.isNullAt(1))

    val struct2 = reader.getStruct(2)
    assert(struct2.isNullAt(0))
    assert(struct2.isNullAt(1))

    writer.root.close()
  }

  test("map") {
    val schema = new StructType()
      .add("map", MapType(IntegerType, StringType), nullable = true)
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema == schema)

    writer.write(InternalRow(ArrayBasedMapData(
      keys = Array(1, 2, 3),
      values = Array(
        UTF8String.fromString("v2"),
        UTF8String.fromString("v3"),
        UTF8String.fromString("v4")
      )
    )))
    writer.write(InternalRow(ArrayBasedMapData(Array(43),
      Array(UTF8String.fromString("v5"))
    )))
    writer.write(InternalRow(ArrayBasedMapData(Array(43), Array(null))))
    writer.write(InternalRow(null))

    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors.get(0))
    val map0 = reader.getMap(0)
    assert(map0.numElements() == 3)
    assert(map0.keyArray().array().mkString(",") == Array(1, 2, 3).mkString(","))
    assert(map0.valueArray().array().mkString(",") == Array("v2", "v3", "v4").mkString(","))

    val map1 = reader.getMap(1)
    assert(map1.numElements() == 1)
    assert(map1.keyArray().array().mkString(",") == Array(43).mkString(","))
    assert(map1.valueArray().array().mkString(",") == Array("v5").mkString(","))

    val map2 = reader.getMap(2)
    assert(map2.numElements() == 1)
    assert(map2.keyArray().array().mkString(",") == Array(43).mkString(","))
    assert(map2.valueArray().array().mkString(",") == Array(null).mkString(","))

    val map3 = reader.getMap(3)
    assert(map3 == null)
    writer.root.close()
  }

  test("empty map") {
    val schema = new StructType()
      .add("map", MapType(IntegerType, StringType), nullable = true)
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema == schema)
    writer.write(InternalRow(ArrayBasedMapData(Array(), Array())))
    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors.get(0))

    val map0 = reader.getMap(0)
    assert(map0.numElements() == 0)
    writer.root.close()
  }

  test("null value map") {
    val schema = new StructType()
      .add("map", MapType(IntegerType, NullType), nullable = true)
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema == schema)

    writer.write(InternalRow(ArrayBasedMapData(
      keys = Array(1, 2, 3),
      values = Array(null, null, null)
    )))
    writer.write(InternalRow(ArrayBasedMapData(Array(43), Array(null))))
    writer.write(InternalRow(null))

    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors.get(0))
    val map0 = reader.getMap(0)
    assert(map0.numElements() == 3)
    assert(map0.keyArray().array().mkString(",") == Array(1, 2, 3).mkString(","))
    assert(map0.valueArray().array().mkString(",") == Array(null, null, null).mkString(","))

    val map1 = reader.getMap(1)
    assert(map1.numElements() == 1)
    assert(map1.keyArray().array().mkString(",") == Array(43).mkString(","))
    assert(map1.valueArray().array().mkString(",") == Array(null).mkString(","))

    val map2 = reader.getMap(3)
    assert(map2 == null)
    writer.root.close()
  }

  test("nested map") {
    val valueSchema = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)

    val schema = new StructType()
      .add("map",
        MapType(
          keyType = IntegerType,
          valueType = valueSchema
        ),
        nullable = true)
    val writer = ArrowWriter.create(schema, null)
    assert(writer.schema == schema)

    writer.write(InternalRow(
      ArrayBasedMapData(
        keys = Array(1),
        values = Array(InternalRow(UTF8String.fromString("jon"), 20))
      )))

    writer.write(InternalRow(
      ArrayBasedMapData(
        keys = Array(1),
        values = Array(InternalRow(UTF8String.fromString("alice"), 30))
      )))

    writer.write(InternalRow(
      ArrayBasedMapData(
        keys = Array(1),
        values = Array(InternalRow(UTF8String.fromString("bob"), 40))
      )))


    writer.finish()

    val reader = new ArrowColumnVector(writer.root.getFieldVectors.get(0))

    def stringRepr(map: ColumnarMap): String = {
      map.valueArray().getStruct(0, 2).toSeq(valueSchema).mkString(",")
    }

    val map0 = reader.getMap(0)
    assert(map0.numElements() == 1)
    assert(map0.keyArray().array().mkString(",") == Array(1).mkString(","))
    assert(stringRepr(map0) == Array("jon", "20").mkString(","))

    val map1 = reader.getMap(1)
    assert(map1.numElements() == 1)
    assert(map1.keyArray().array().mkString(",") == Array(1).mkString(","))
    assert(stringRepr(map1) == Array("alice", "30").mkString(","))

    val map2 = reader.getMap(2)
    assert(map2.numElements() == 1)
    assert(map2.keyArray().array().mkString(",") == Array(1).mkString(","))
    assert(stringRepr(map2) == Array("bob", "40").mkString(","))
  }
}
