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

import org.apache.hadoop.io._
import org.apache.orc.TypeDescription
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

/**
 * A serializer to serialize Spark rows to ORC structs.
 */
class OrcSerializer(dataSchema: StructType) {

  private val result = createOrcValue(dataSchema).asInstanceOf[OrcStruct]
  private val converters = dataSchema.map(_.dataType).map(newConverter(_)).toArray

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


    case LongType | _: DayTimeIntervalType =>
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
    case StringType => (getter, ordinal) =>
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
      val result = createOrcValue(st).asInstanceOf[OrcStruct]
      val fieldConverters = st.map(_.dataType).map(newConverter(_))
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
      val result = createOrcValue(dataType).asInstanceOf[OrcList[WritableComparable[_]]]
      // Need to put all converted values to a list, can't reuse object.
      val elementConverter = newConverter(elementType, reuseObj = false)
      val array = getter.getArray(ordinal)
      var i = 0
      while (i < array.numElements()) {
        if (array.isNullAt(i)) {
          result.add(null)
        } else {
          result.add(elementConverter(array, i))
        }
        i += 1
      }
      result

    case MapType(keyType, valueType, _) => (getter, ordinal) =>
      val result = createOrcValue(dataType)
        .asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
      // Need to put all converted values to a list, can't reuse object.
      val keyConverter = newConverter(keyType, reuseObj = false)
      val valueConverter = newConverter(valueType, reuseObj = false)
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

    case udt: UserDefinedType[_] => newConverter(udt.sqlType)

    case _ =>
      throw QueryExecutionErrors.dataTypeUnsupportedYetError(dataType)
  }

  /**
   * Return a Orc value object for the given Spark schema.
   */
  private def createOrcValue(dataType: DataType) = {
    OrcStruct.createValue(TypeDescription.fromString(OrcFileFormat.getQuotedSchemaString(dataType)))
  }
}
