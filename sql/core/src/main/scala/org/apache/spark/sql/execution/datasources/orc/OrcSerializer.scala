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

package org.apache.spark.sql.execution.datasources.orc

import scala.jdk.CollectionConverters._

import org.apache.hadoop.io._
import org.apache.orc.TypeDescription
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._

/**
 * A serializer to serialize Spark rows to ORC structs.
 */
class OrcSerializer(dataSchema: StructType) {
  private val resultTypeDescription = OrcUtils.orcTypeDescription(dataSchema)
  private val result = OrcStruct.createValue(resultTypeDescription).asInstanceOf[OrcStruct]
  private val converters =
    dataSchema.map(_.dataType).zip(resultTypeDescription.getChildren.asScala).map {
      case (dt, orcType) =>
        newConverter(dt, orcType)
    }.toArray

  def serialize(row: InternalRow): OrcStruct = {
    var i = 0
    while (i < converters.length) {
      if (row.isNullAt(i)) {
        result.setFieldValue(i, null)
      } else {
        result.setFieldValue(i, converters(i)(row, i))
      }
      i += 1
    }
    result
  }

  private type Converter = (SpecializedGetters, Int) => WritableComparable[_]

  /**
   * Creates a converter to convert Catalyst data at the given ordinal to ORC values.
   */
  private def newConverter(
      dataType: DataType,
      orcType: TypeDescription,
      reuseObj: Boolean = true): Converter = dataType match {
    case NullType => (getter, ordinal) => null

    case BooleanType =>
      if (reuseObj) {
        val result = new BooleanWritable()
        (getter, ordinal) =>
          result.set(getter.getBoolean(ordinal))
          result
      } else {
        (getter, ordinal) => new BooleanWritable(getter.getBoolean(ordinal))
      }

    case ByteType =>
      if (reuseObj) {
        val result = new ByteWritable()
        (getter, ordinal) =>
          result.set(getter.getByte(ordinal))
          result
      } else {
        (getter, ordinal) => new ByteWritable(getter.getByte(ordinal))
      }

    case ShortType =>
      if (reuseObj) {
        val result = new ShortWritable()
        (getter, ordinal) =>
          result.set(getter.getShort(ordinal))
          result
      } else {
        (getter, ordinal) => new ShortWritable(getter.getShort(ordinal))
      }

    case IntegerType | _: YearMonthIntervalType =>
      if (reuseObj) {
        val result = new IntWritable()
        (getter, ordinal) =>
          result.set(getter.getInt(ordinal))
          result
      } else {
        (getter, ordinal) => new IntWritable(getter.getInt(ordinal))
      }


    case LongType | _: DayTimeIntervalType | _: TimestampNTZType | _: TimeType =>
      if (reuseObj) {
        val result = new LongWritable()
        (getter, ordinal) =>
          result.set(getter.getLong(ordinal))
          result
      } else {
        (getter, ordinal) => new LongWritable(getter.getLong(ordinal))
      }

    case FloatType =>
      if (reuseObj) {
        val result = new FloatWritable()
        (getter, ordinal) =>
          result.set(getter.getFloat(ordinal))
          result
      } else {
        (getter, ordinal) => new FloatWritable(getter.getFloat(ordinal))
      }

    case DoubleType =>
      if (reuseObj) {
        val result = new DoubleWritable()
        (getter, ordinal) =>
          result.set(getter.getDouble(ordinal))
          result
      } else {
        (getter, ordinal) => new DoubleWritable(getter.getDouble(ordinal))
      }


    // Don't reuse the result object for string and binary as it would cause extra data copy.
    case _: StringType => (getter, ordinal) =>
      new Text(getter.getUTF8String(ordinal).getBytes)

    case BinaryType => (getter, ordinal) =>
      new BytesWritable(getter.getBinary(ordinal))

    case DateType =>
      OrcShimUtils.getDateWritable(reuseObj)

    // The following cases are already expensive, reusing object or not doesn't matter.

    case TimestampType => (getter, ordinal) =>
      val ts = DateTimeUtils.toJavaTimestamp(getter.getLong(ordinal))
      val result = new OrcTimestamp(ts.getTime)
      result.setNanos(ts.getNanos)
      result

    case DecimalType.Fixed(precision, scale) =>
      OrcShimUtils.getHiveDecimalWritable(precision, scale)

    case st: StructType => (getter, ordinal) =>
      val result = OrcStruct.createValue(orcType).asInstanceOf[OrcStruct]
      val fieldConverters = st.map(_.dataType).zip(orcType.getChildren.asScala).map {
        case (dt, orcType) =>
          newConverter(dt, orcType)
      }.toArray
      val numFields = st.length
      val struct = getter.getStruct(ordinal, numFields)
      var i = 0
      while (i < numFields) {
        if (struct.isNullAt(i)) {
          result.setFieldValue(i, null)
        } else {
          result.setFieldValue(i, fieldConverters(i)(struct, i))
        }
        i += 1
      }
      result

    case ArrayType(elementType, _) => (getter, ordinal) =>
      val array = getter.getArray(ordinal)
      val numElements = array.numElements()
      val result = new OrcList[WritableComparable[_]](orcType, numElements)
      if (numElements > 0) {
        // Need to put all converted values to a list, can't reuse object.
        val elementConverter =
          newConverter(elementType, orcType.getChildren.get(0), reuseObj = false)
        var i = 0
        while (i < numElements) {
          if (array.isNullAt(i)) {
            result.add(null)
          } else {
            result.add(elementConverter(array, i))
          }
          i += 1
        }
      }
      result

    case MapType(keyType, valueType, _) => (getter, ordinal) =>
      val result = OrcStruct.createValue(orcType)
        .asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
      // Need to put all converted values to a list, can't reuse object.
      val orcChildSchema = orcType.getChildren
      val keyConverter = newConverter(keyType, orcChildSchema.get(0), reuseObj = false)
      val valueConverter = newConverter(valueType, orcChildSchema.get(1), reuseObj = false)
      val map = getter.getMap(ordinal)
      val keyArray = map.keyArray()
      val valueArray = map.valueArray()
      var i = 0
      while (i < map.numElements()) {
        val key = keyConverter(keyArray, i)
        if (valueArray.isNullAt(i)) {
          result.put(key, null)
        } else {
          result.put(key, valueConverter(valueArray, i))
        }
        i += 1
      }
      result

    case udt: UserDefinedType[_] => newConverter(udt.sqlType, orcType)

    case _ => throw SparkException.internalError(s"Unsupported data type $dataType.")
  }
}
