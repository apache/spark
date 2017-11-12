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
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.serde2.io.{DateWritable, HiveDecimalWritable}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.orc.OrcUtils.{getTypeDescription, withNullSafe}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[orc] class OrcSerializer(dataSchema: StructType) {

  private[this] lazy val orcStruct: OrcStruct = createOrcValue(dataSchema).asInstanceOf[OrcStruct]

  private[this] lazy val length = dataSchema.length

  private[this] val writers = dataSchema.map(_.dataType).map(makeWriter).toArray

  def serialize(row: InternalRow): OrcStruct = {
    var i = 0
    while (i < length) {
      if (row.isNullAt(i)) {
        orcStruct.setFieldValue(i, null)
      } else {
        writers(i)(row, i)
      }
      i += 1
    }
    orcStruct
  }

  private[this] def makeWriter(dataType: DataType): (SpecializedGetters, Int) => Unit = {
    dataType match {
      case BooleanType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new BooleanWritable(row.getBoolean(ordinal)))

      case ByteType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new ByteWritable(row.getByte(ordinal)))

      case ShortType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new ShortWritable(row.getShort(ordinal)))

      case IntegerType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new IntWritable(row.getInt(ordinal)))

      case LongType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new LongWritable(row.getLong(ordinal)))

      case FloatType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new FloatWritable(row.getFloat(ordinal)))

      case DoubleType =>
        (row: SpecializedGetters, ordinal: Int) =>
          orcStruct.setFieldValue(ordinal, new DoubleWritable(row.getDouble(ordinal)))

      case _ =>
        val wrapper = getWritableWrapper(dataType)
        (row: SpecializedGetters, ordinal: Int) => {
          val value = wrapper(row.get(ordinal, dataType)).asInstanceOf[WritableComparable[_]]
          orcStruct.setFieldValue(ordinal, value)
        }
    }
  }

  /**
   * Return a Orc value object for the given Spark schema.
   */
  private[this] def createOrcValue(dataType: DataType) =
    OrcStruct.createValue(getTypeDescription(dataType))

  /**
   * Convert Apache Spark InternalRow to Apache ORC OrcStruct.
   */
  private[this] def convertInternalRowToOrcStruct(row: InternalRow, schema: StructType) = {
    val wrappers = schema.map(_.dataType).map(getWritableWrapper).toArray
    val orcStruct = createOrcValue(schema).asInstanceOf[OrcStruct]

    var i = 0
    val length = schema.length
    while (i < length) {
      val fieldType = schema(i).dataType
      if (row.isNullAt(i)) {
        orcStruct.setFieldValue(i, null)
      } else {
        val field = row.get(i, fieldType)
        val fieldValue = wrappers(i)(field).asInstanceOf[WritableComparable[_]]
        orcStruct.setFieldValue(i, fieldValue)
      }
      i += 1
    }
    orcStruct
  }

  /**
   * Builds a WritableComparable-return function ahead of time according to DataType
   * to avoid pattern matching and branching costs per row.
   */
  private[this] def getWritableWrapper(dataType: DataType): Any => Any = dataType match {
    case NullType => _ => null

    case BooleanType => withNullSafe(o => new BooleanWritable(o.asInstanceOf[Boolean]))

    case ByteType => withNullSafe(o => new ByteWritable(o.asInstanceOf[Byte]))
    case ShortType => withNullSafe(o => new ShortWritable(o.asInstanceOf[Short]))
    case IntegerType => withNullSafe(o => new IntWritable(o.asInstanceOf[Int]))
    case LongType => withNullSafe(o => new LongWritable(o.asInstanceOf[Long]))

    case FloatType => withNullSafe(o => new FloatWritable(o.asInstanceOf[Float]))
    case DoubleType => withNullSafe(o => new DoubleWritable(o.asInstanceOf[Double]))

    case StringType => withNullSafe(o => new Text(o.asInstanceOf[UTF8String].getBytes))

    case BinaryType => withNullSafe(o => new BytesWritable(o.asInstanceOf[Array[Byte]]))

    case DateType =>
      withNullSafe(o => new DateWritable(DateTimeUtils.toJavaDate(o.asInstanceOf[Int])))
    case TimestampType =>
      withNullSafe { o =>
        val us = o.asInstanceOf[Long]
        var seconds = us / DateTimeUtils.MICROS_PER_SECOND
        var micros = us % DateTimeUtils.MICROS_PER_SECOND
        if (micros < 0) {
          micros += DateTimeUtils.MICROS_PER_SECOND
          seconds -= 1
        }
        val t = new OrcTimestamp(seconds * 1000)
        t.setNanos(micros.toInt * 1000)
        t
      }

    case _: DecimalType =>
      withNullSafe { o =>
        new HiveDecimalWritable(HiveDecimal.create(o.asInstanceOf[Decimal].toJavaBigDecimal))
      }

    case st: StructType =>
      withNullSafe(o => convertInternalRowToOrcStruct(o.asInstanceOf[InternalRow], st))

    case ArrayType(et, _) =>
      withNullSafe { o =>
        val data = o.asInstanceOf[ArrayData]
        val list = createOrcValue(dataType)
        for (i <- 0 until data.numElements()) {
          val d = data.get(i, et)
          val v = getWritableWrapper(et)(d).asInstanceOf[WritableComparable[_]]
          list.asInstanceOf[OrcList[WritableComparable[_]]].add(v)
        }
        list
      }

    case MapType(keyType, valueType, _) =>
      withNullSafe { o =>
        val keyWrapper = getWritableWrapper(keyType)
        val valueWrapper = getWritableWrapper(valueType)
        val data = o.asInstanceOf[MapData]
        val map = createOrcValue(dataType)
          .asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
        data.foreach(keyType, valueType, { case (k, v) =>
          map.put(
            keyWrapper(k).asInstanceOf[WritableComparable[_]],
            valueWrapper(v).asInstanceOf[WritableComparable[_]])
        })
        map
      }

    case udt: UserDefinedType[_] =>
      withNullSafe { o =>
        val udtRow = new SpecificInternalRow(Seq(udt.sqlType))
        udtRow(0) = o
        convertInternalRowToOrcStruct(
          udtRow,
          StructType(Seq(StructField("tmp", udt.sqlType)))).getFieldValue(0)
      }

    case _ =>
      throw new UnsupportedOperationException(s"$dataType is not supported yet.")
  }
}
