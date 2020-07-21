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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, IntervalUtils, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object SparkInspectors {

  def wrapper(dataType: DataType): Any => Any = dataType match {
    case ArrayType(tpe, _) =>
      val wp = wrapper(tpe)
      withNullSafe { o =>
        val array = o.asInstanceOf[ArrayData]
        val values = new java.util.ArrayList[Any](array.numElements())
        array.foreach(tpe, (_, e) => values.add(wp(e)))
        values
      }
    case MapType(keyType, valueType, _) =>
      val mt = dataType.asInstanceOf[MapType]
      val keyWrapper = wrapper(keyType)
      val valueWrapper = wrapper(valueType)
      withNullSafe { o =>
        val map = o.asInstanceOf[MapData]
        val jmap = new java.util.HashMap[Any, Any](map.numElements())
        map.foreach(mt.keyType, mt.valueType, (k, v) =>
          jmap.put(keyWrapper(k), valueWrapper(v)))
        jmap
      }
    case StringType => getStringWritable
    case IntegerType => getIntWritable
    case DoubleType => getDoubleWritable
    case BooleanType => getBooleanWritable
    case LongType => getLongWritable
    case FloatType => getFloatWritable
    case ShortType => getShortWritable
    case ByteType => getByteWritable
    case NullType => (_: Any) => null
    case BinaryType => getBinaryWritable
    case DateType => getDateWritable
    case TimestampType => getTimestampWritable
    // TODO decimal precision?
    case DecimalType() => getDecimalWritable
    case StructType(fields) =>
      val structType = dataType.asInstanceOf[StructType]
      val wrappers = fields.map(f => wrapper(f.dataType))
      withNullSafe { o =>
        val row = o.asInstanceOf[InternalRow]
        val result = new java.util.ArrayList[AnyRef](wrappers.size)
        wrappers.zipWithIndex.foreach {
          case (wrapper, i) =>
            val tpe = structType(i).dataType
            result.add(wrapper(row.get(i, tpe)).asInstanceOf[AnyRef])
        }
        result
      }
    case _: UserDefinedType[_] =>
      val sqlType = dataType.asInstanceOf[UserDefinedType[_]].sqlType
      wrapper(sqlType)
  }

  private def withNullSafe(f: Any => Any): Any => Any = {
    input =>
      if (input == null) {
        null
      } else {
        f(input)
      }
  }

  private def getStringWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[UTF8String].toString
    }

  private def getIntWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Int]
    }

  private def getDoubleWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Double]
    }

  private def getBooleanWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Boolean]
    }

  private def getLongWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Long]
    }

  private def getFloatWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Float]
    }

  private def getShortWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Short]
    }

  private def getByteWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Byte]
    }

  private def getBinaryWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Array[Byte]]
    }

  private def getDateWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      DateTimeUtils.toJavaDate(value.asInstanceOf[Int])
    }

  private def getTimestampWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long])
    }

  private def getDecimalWritable(value: Any): Any =
    if (value == null) {
      null
    } else {
      value.asInstanceOf[Decimal]
    }


  def unwrapper(
      dataType: DataType,
      conf: SQLConf,
      ioSchema: ScriptTransformationIOSchema): String => Any = {
    val converter = CatalystTypeConverters.createToCatalystConverter(dataType)
    dataType match {
      case StringType => wrapperConvertException(data => data, converter)
      case BooleanType => wrapperConvertException(data => data.toBoolean, converter)
      case ByteType => wrapperConvertException(data => data.toByte, converter)
      case BinaryType => wrapperConvertException(data => data.getBytes, converter)
      case IntegerType => wrapperConvertException(data => data.toInt, converter)
      case ShortType => wrapperConvertException(data => data.toShort, converter)
      case LongType => wrapperConvertException(data => data.toLong, converter)
      case FloatType => wrapperConvertException(data => data.toFloat, converter)
      case DoubleType => wrapperConvertException(data => data.toDouble, converter)
      case _: DecimalType => wrapperConvertException(data => BigDecimal(data), converter)
      case DateType if conf.datetimeJava8ApiEnabled =>
        wrapperConvertException(data => DateTimeUtils.stringToDate(
          UTF8String.fromString(data),
          DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
          .map(DateTimeUtils.daysToLocalDate).orNull, converter)
      case DateType => wrapperConvertException(data => DateTimeUtils.stringToDate(
        UTF8String.fromString(data),
        DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
        .map(DateTimeUtils.toJavaDate).orNull, converter)
      case TimestampType if conf.datetimeJava8ApiEnabled =>
        wrapperConvertException(data => DateTimeUtils.stringToTimestamp(
          UTF8String.fromString(data),
          DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
          .map(DateTimeUtils.microsToInstant).orNull, converter)
      case TimestampType => wrapperConvertException(data => DateTimeUtils.stringToTimestamp(
        UTF8String.fromString(data),
        DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
        .map(DateTimeUtils.toJavaTimestamp).orNull, converter)
      case CalendarIntervalType => wrapperConvertException(
        data => IntervalUtils.stringToInterval(UTF8String.fromString(data)),
        converter)
      case udt: UserDefinedType[_] =>
        wrapperConvertException(data => udt.deserialize(data), converter)
      case ArrayType(tpe, _) =>
        val un = unwrapper(tpe, conf, ioSchema)
        wrapperConvertException(data => {
          data.split(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATCOLLITEMS"))
            .map(un).toSeq
        }, converter)
      case MapType(keyType, valueType, _) =>
        val keyUnwrapper = unwrapper(keyType, conf, ioSchema)
        val valueUnwrapper = unwrapper(valueType, conf, ioSchema)
        wrapperConvertException(data => {
          val list = data.split(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATCOLLITEMS"))
          list.map { kv =>
            val kvList = kv.split(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATMAPKEYS"))
            keyUnwrapper(kvList(0)) -> valueUnwrapper(kvList(1))
          }.toMap
        }, converter)
      case StructType(fields) =>
        val unwrappers = fields.map(f => unwrapper(f.dataType, conf, ioSchema))
        wrapperConvertException(data => {
          val list = data.split(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATSTRUCTFIELD"))
          Row.fromSeq(list.zipWithIndex.map { case (data: String, i: Int) => unwrappers(i)(data) })
        }, converter)
      case _ => wrapperConvertException(data => data, converter)
    }
  }

  // Keep consistent with Hive `LazySimpleSerde`, when there is a type case error, return null
  private val wrapperConvertException: (String => Any, Any => Any) => String => Any =
    (f: String => Any, converter: Any => Any) =>
      (data: String) => converter {
        try {
          f(data)
        } catch {
          case _: Exception => null
        }
      }
}
