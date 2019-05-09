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

import java.time.ZoneId

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class UnsafeArraySuite extends SparkFunSuite {

  val booleanArray = Array(false, true)
  val shortArray = Array(1.toShort, 10.toShort, 100.toShort)
  val intArray = Array(1, 10, 100)
  val longArray = Array(1.toLong, 10.toLong, 100.toLong)
  val floatArray = Array(1.1.toFloat, 2.2.toFloat, 3.3.toFloat)
  val doubleArray = Array(1.1, 2.2, 3.3)
  val stringArray = Array("1", "10", "100")
  val dateArray = Array(
    DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1")).get,
    DateTimeUtils.stringToDate(UTF8String.fromString("2016-7-26")).get)
  private def defaultTz = DateTimeUtils.defaultTimeZone()
  private def defaultZoneId = ZoneId.systemDefault()
  val timestampArray = Array(
    DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("1970-1-1 00:00:00"), defaultZoneId).get,
    DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2016-7-26 00:00:00"), defaultZoneId).get)
  val decimalArray4_1 = Array(
    BigDecimal("123.4").setScale(1, BigDecimal.RoundingMode.FLOOR),
    BigDecimal("567.8").setScale(1, BigDecimal.RoundingMode.FLOOR))
  val decimalArray20_20 = Array(
    BigDecimal("1.2345678901234567890123456").setScale(21, BigDecimal.RoundingMode.FLOOR),
    BigDecimal("2.3456789012345678901234567").setScale(21, BigDecimal.RoundingMode.FLOOR))

  val calenderintervalArray = Array(new CalendarInterval(3, 321), new CalendarInterval(1, 123))

  val intMultiDimArray = Array(Array(1), Array(2, 20), Array(3, 30, 300))
  val doubleMultiDimArray = Array(
    Array(1.1, 11.1), Array(2.2, 22.2, 222.2), Array(3.3, 33.3, 333.3, 3333.3))

  val serialArray = {
    val offset = 32
    val data = new Array[Byte](1024)
    Platform.putLong(data, offset, 1)
    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(data, offset, data.length)
    arrayData.setLong(0, 19285)
    arrayData
  }

  test("read array") {
    val unsafeBoolean = ExpressionEncoder[Array[Boolean]].resolveAndBind().
      toRow(booleanArray).getArray(0)
    assert(unsafeBoolean.isInstanceOf[UnsafeArrayData])
    assert(unsafeBoolean.numElements == booleanArray.length)
    booleanArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeBoolean.getBoolean(i) == e)
    }

    val unsafeShort = ExpressionEncoder[Array[Short]].resolveAndBind().
      toRow(shortArray).getArray(0)
    assert(unsafeShort.isInstanceOf[UnsafeArrayData])
    assert(unsafeShort.numElements == shortArray.length)
    shortArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeShort.getShort(i) == e)
    }

    val unsafeInt = ExpressionEncoder[Array[Int]].resolveAndBind().
      toRow(intArray).getArray(0)
    assert(unsafeInt.isInstanceOf[UnsafeArrayData])
    assert(unsafeInt.numElements == intArray.length)
    intArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInt.getInt(i) == e)
    }

    val unsafeLong = ExpressionEncoder[Array[Long]].resolveAndBind().
      toRow(longArray).getArray(0)
    assert(unsafeLong.isInstanceOf[UnsafeArrayData])
    assert(unsafeLong.numElements == longArray.length)
    longArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeLong.getLong(i) == e)
    }

    val unsafeFloat = ExpressionEncoder[Array[Float]].resolveAndBind().
      toRow(floatArray).getArray(0)
    assert(unsafeFloat.isInstanceOf[UnsafeArrayData])
    assert(unsafeFloat.numElements == floatArray.length)
    floatArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeFloat.getFloat(i) == e)
    }

    val unsafeDouble = ExpressionEncoder[Array[Double]].resolveAndBind().
      toRow(doubleArray).getArray(0)
    assert(unsafeDouble.isInstanceOf[UnsafeArrayData])
    assert(unsafeDouble.numElements == doubleArray.length)
    doubleArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDouble.getDouble(i) == e)
    }

    val unsafeString = ExpressionEncoder[Array[String]].resolveAndBind().
      toRow(stringArray).getArray(0)
    assert(unsafeString.isInstanceOf[UnsafeArrayData])
    assert(unsafeString.numElements == stringArray.length)
    stringArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeString.getUTF8String(i).toString().equals(e))
    }

    val unsafeDate = ExpressionEncoder[Array[Int]].resolveAndBind().
      toRow(dateArray).getArray(0)
    assert(unsafeDate.isInstanceOf[UnsafeArrayData])
    assert(unsafeDate.numElements == dateArray.length)
    dateArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDate.get(i, DateType).asInstanceOf[Int] == e)
    }

    val unsafeTimestamp = ExpressionEncoder[Array[Long]].resolveAndBind().
      toRow(timestampArray).getArray(0)
    assert(unsafeTimestamp.isInstanceOf[UnsafeArrayData])
    assert(unsafeTimestamp.numElements == timestampArray.length)
    timestampArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeTimestamp.get(i, TimestampType).asInstanceOf[Long] == e)
    }

    Seq(decimalArray4_1, decimalArray20_20).map { decimalArray =>
      val decimal = decimalArray(0)
      val schema = new StructType().add(
        "array", ArrayType(DecimalType(decimal.precision, decimal.scale)))
      val encoder = RowEncoder(schema).resolveAndBind()
      val externalRow = Row(decimalArray)
      val ir = encoder.toRow(externalRow)

      val unsafeDecimal = ir.getArray(0)
      assert(unsafeDecimal.isInstanceOf[UnsafeArrayData])
      assert(unsafeDecimal.numElements == decimalArray.length)
      decimalArray.zipWithIndex.map { case (e, i) =>
        assert(unsafeDecimal.getDecimal(i, e.precision, e.scale).toBigDecimal == e)
      }
    }

    val schema = new StructType().add("array", ArrayType(CalendarIntervalType))
    val encoder = RowEncoder(schema).resolveAndBind()
    val externalRow = Row(calenderintervalArray)
    val ir = encoder.toRow(externalRow)
    val unsafeCalendar = ir.getArray(0)
    assert(unsafeCalendar.isInstanceOf[UnsafeArrayData])
    assert(unsafeCalendar.numElements == calenderintervalArray.length)
    calenderintervalArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeCalendar.getInterval(i) == e)
    }

    val unsafeMultiDimInt = ExpressionEncoder[Array[Array[Int]]].resolveAndBind().
      toRow(intMultiDimArray).getArray(0)
    assert(unsafeMultiDimInt.isInstanceOf[UnsafeArrayData])
    assert(unsafeMultiDimInt.numElements == intMultiDimArray.length)
    intMultiDimArray.zipWithIndex.map { case (a, j) =>
      val u = unsafeMultiDimInt.getArray(j)
      assert(u.isInstanceOf[UnsafeArrayData])
      assert(u.numElements == a.length)
      a.zipWithIndex.map { case (e, i) =>
        assert(u.getInt(i) == e)
      }
    }

    val unsafeMultiDimDouble = ExpressionEncoder[Array[Array[Double]]].resolveAndBind().
      toRow(doubleMultiDimArray).getArray(0)
    assert(unsafeDouble.isInstanceOf[UnsafeArrayData])
    assert(unsafeMultiDimDouble.numElements == doubleMultiDimArray.length)
    doubleMultiDimArray.zipWithIndex.map { case (a, j) =>
      val u = unsafeMultiDimDouble.getArray(j)
      assert(u.isInstanceOf[UnsafeArrayData])
      assert(u.numElements == a.length)
      a.zipWithIndex.map { case (e, i) =>
        assert(u.getDouble(i) == e)
      }
    }
  }

  test("from primitive array") {
    val unsafeInt = UnsafeArrayData.fromPrimitiveArray(intArray)
    assert(unsafeInt.numElements == 3)
    assert(unsafeInt.getSizeInBytes ==
      ((8 + scala.math.ceil(3/64.toDouble) * 8 + 4 * 3 + 7).toInt / 8) * 8)
    intArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInt.getInt(i) == e)
    }

    val unsafeDouble = UnsafeArrayData.fromPrimitiveArray(doubleArray)
    assert(unsafeDouble.numElements == 3)
    assert(unsafeDouble.getSizeInBytes ==
      ((8 + scala.math.ceil(3/64.toDouble) * 8 + 8 * 3 + 7).toInt / 8) * 8)
    doubleArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDouble.getDouble(i) == e)
    }
  }

  test("to primitive array") {
    val intEncoder = ExpressionEncoder[Array[Int]].resolveAndBind()
    assert(intEncoder.toRow(intArray).getArray(0).toIntArray.sameElements(intArray))

    val doubleEncoder = ExpressionEncoder[Array[Double]].resolveAndBind()
    assert(doubleEncoder.toRow(doubleArray).getArray(0).toDoubleArray.sameElements(doubleArray))
  }

  test("unsafe java serialization") {
    val ser = new JavaSerializer(new SparkConf).newInstance()
    val arrayDataSer = ser.deserialize[UnsafeArrayData](ser.serialize(serialArray))
    assert(arrayDataSer.getLong(0) == 19285)
    assert(arrayDataSer.getBaseObject.asInstanceOf[Array[Byte]].length == 1024)
  }

  test("unsafe Kryo serialization") {
    val ser = new KryoSerializer(new SparkConf).newInstance()
    val arrayDataSer = ser.deserialize[UnsafeArrayData](ser.serialize(serialArray))
    assert(arrayDataSer.getLong(0) == 19285)
    assert(arrayDataSer.getBaseObject.asInstanceOf[Array[Byte]].length == 1024)
  }
}
