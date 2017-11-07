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
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.orc.OrcUtils.getTypeDescription
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[orc] class OrcSerializer(dataSchema: StructType) {

  private[this] lazy val orcStruct: OrcStruct =
    createOrcValue(dataSchema).asInstanceOf[OrcStruct]

  private[this] val writableWrappers =
    dataSchema.fields.map(f => getWritableWrapper(f.dataType))

  def serialize(row: InternalRow): OrcStruct = {
    convertInternalRowToOrcStruct(row, dataSchema, Some(writableWrappers), Some(orcStruct))
  }

  /**
   * Return a Orc value object for the given Spark schema.
   */
  private[this] def createOrcValue(dataType: DataType) =
    OrcStruct.createValue(getTypeDescription(dataType))

  /**
   * Convert Apache Spark InternalRow to Apache ORC OrcStruct.
   */
  private[this] def convertInternalRowToOrcStruct(
      row: InternalRow,
      schema: StructType,
      valueWrappers: Option[Seq[Any => Any]] = None,
      struct: Option[OrcStruct] = None): OrcStruct = {
    val wrappers =
      valueWrappers.getOrElse(schema.fields.map(_.dataType).map(getWritableWrapper).toSeq)
    val orcStruct = struct.getOrElse(createOrcValue(schema).asInstanceOf[OrcStruct])

    for (schemaIndex <- 0 until schema.length) {
      val fieldType = schema(schemaIndex).dataType
      if (row.isNullAt(schemaIndex)) {
        orcStruct.setFieldValue(schemaIndex, null)
      } else {
        val field = row.get(schemaIndex, fieldType)
        val fieldValue = wrappers(schemaIndex)(field).asInstanceOf[WritableComparable[_]]
        orcStruct.setFieldValue(schemaIndex, fieldValue)
      }
    }
    orcStruct
  }

  private[this] def withNullSafe(f: Any => Any): Any => Any = {
    input => if (input == null) null else f(input)
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
