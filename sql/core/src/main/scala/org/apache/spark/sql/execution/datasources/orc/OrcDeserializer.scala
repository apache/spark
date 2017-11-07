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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io._
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.apache.orc.storage.serde2.io.{DateWritable, HiveDecimalWritable}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[orc] class OrcDeserializer(
    dataSchema: StructType,
    requiredSchema: StructType,
    maybeMissingSchema: Option[StructType]) {

  private[this] val mutableRow = new SpecificInternalRow(requiredSchema.map(_.dataType))

  private[this] val valueWrappers = requiredSchema.fields.map(f => getValueWrapper(f.dataType))

  def deserialize(writable: OrcStruct): InternalRow = {
    convertOrcStructToInternalRow(writable, dataSchema, requiredSchema,
      maybeMissingSchema, Some(valueWrappers), Some(mutableRow))
  }

  /**
   * Convert Apache ORC OrcStruct to Apache Spark InternalRow.
   * If internalRow is not None, fill into it. Otherwise, create a SpecificInternalRow and use it.
   */
  private[this] def convertOrcStructToInternalRow(
      orcStruct: OrcStruct,
      dataSchema: StructType,
      requiredSchema: StructType,
      missingSchema: Option[StructType] = None,
      valueWrappers: Option[Seq[Any => Any]] = None,
      internalRow: Option[InternalRow] = None): InternalRow = {
    val mutableRow = internalRow.getOrElse(new SpecificInternalRow(requiredSchema.map(_.dataType)))
    val wrappers =
      valueWrappers.getOrElse(requiredSchema.fields.map(_.dataType).map(getValueWrapper).toSeq)
    var i = 0
    val len = requiredSchema.length
    val names = orcStruct.getSchema.getFieldNames
    while (i < len) {
      val name = requiredSchema(i).name
      val writable = if (missingSchema.isEmpty || missingSchema.get.getFieldIndex(name).isEmpty) {
        if (names.contains(name)) {
          orcStruct.getFieldValue(name)
        } else {
          orcStruct.getFieldValue("_col" + dataSchema.fieldIndex(name))
        }
      } else {
        null
      }
      if (writable == null) {
        mutableRow.setNullAt(i)
      } else {
        mutableRow(i) = wrappers(i)(writable)
      }
      i += 1
    }
    mutableRow
  }

  private[this] def withNullSafe(f: Any => Any): Any => Any = {
    input => if (input == null) null else f(input)
  }

  /**
   * Builds a catalyst-value return function ahead of time according to DataType
   * to avoid pattern matching and branching costs per row.
   */
  private[this] def getValueWrapper(dataType: DataType): Any => Any = dataType match {
    case NullType => _ => null

    case BooleanType => withNullSafe(o => o.asInstanceOf[BooleanWritable].get)

    case ByteType => withNullSafe(o => o.asInstanceOf[ByteWritable].get)
    case ShortType => withNullSafe(o => o.asInstanceOf[ShortWritable].get)
    case IntegerType => withNullSafe(o => o.asInstanceOf[IntWritable].get)
    case LongType => withNullSafe(o => o.asInstanceOf[LongWritable].get)

    case FloatType => withNullSafe(o => o.asInstanceOf[FloatWritable].get)
    case DoubleType => withNullSafe(o => o.asInstanceOf[DoubleWritable].get)

    case StringType =>
      withNullSafe(o => UTF8String.fromBytes(o.asInstanceOf[Text].copyBytes))

    case BinaryType =>
      withNullSafe { o =>
        val binary = o.asInstanceOf[BytesWritable]
        val bytes = new Array[Byte](binary.getLength)
        System.arraycopy(binary.getBytes, 0, bytes, 0, binary.getLength)
        bytes
      }

    case DateType =>
      withNullSafe(o => DateTimeUtils.fromJavaDate(o.asInstanceOf[DateWritable].get))
    case TimestampType =>
      withNullSafe(o => DateTimeUtils.fromJavaTimestamp(o.asInstanceOf[OrcTimestamp]))

    case DecimalType.Fixed(precision, scale) =>
      withNullSafe { o =>
        val decimal = o.asInstanceOf[HiveDecimalWritable].getHiveDecimal()
        val v = Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
        v.changePrecision(precision, scale)
        v
      }

    case _: StructType =>
      withNullSafe { o =>
        val structValue = convertOrcStructToInternalRow(
          o.asInstanceOf[OrcStruct],
          dataType.asInstanceOf[StructType],
          dataType.asInstanceOf[StructType])
        structValue
      }

    case ArrayType(elementType, _) =>
      withNullSafe { o =>
        val wrapper = getValueWrapper(elementType)
        val data = new ArrayBuffer[Any]
        o.asInstanceOf[OrcList[WritableComparable[_]]].asScala.foreach { x =>
          data += wrapper(x)
        }
        new GenericArrayData(data.toArray)
      }

    case MapType(keyType, valueType, _) =>
      withNullSafe { o =>
        val keyWrapper = getValueWrapper(keyType)
        val valueWrapper = getValueWrapper(valueType)
        val map = new java.util.TreeMap[Any, Any]
        o.asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
          .entrySet().asScala.foreach { entry =>
          map.put(keyWrapper(entry.getKey), valueWrapper(entry.getValue))
        }
        ArrayBasedMapData(map.asScala)
      }

    case udt: UserDefinedType[_] =>
      withNullSafe { o => getValueWrapper(udt.sqlType)(o) }

    case _ =>
      throw new UnsupportedOperationException(s"$dataType is not supported yet.")
  }
}
