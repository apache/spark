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
    missingColumnNames: Seq[String]) {

  private[this] val currentRow = new SpecificInternalRow(requiredSchema.map(_.dataType))

  private[this] val length = requiredSchema.length

  private[this] val fieldConverters: Array[Converter] = requiredSchema.zipWithIndex.map {
    case (f, ordinal) =>
      if (missingColumnNames.contains(f.name)) {
        null
      } else {
        newConverter(f.dataType, new RowUpdater(currentRow, ordinal))
      }
  }.toArray

  def deserialize(orcStruct: OrcStruct): InternalRow = {
    val names = orcStruct.getSchema.getFieldNames
    val fieldRefs = requiredSchema.map { f =>
      val name = f.name
      if (missingColumnNames.contains(name)) {
        null
      } else {
        if (names.contains(name)) {
          orcStruct.getFieldValue(name)
        } else {
          orcStruct.getFieldValue("_col" + dataSchema.fieldIndex(name))
        }
      }
    }.toArray

    var i = 0
    while (i < length) {
      val writable = fieldRefs(i)
      if (writable == null) {
        currentRow.setNullAt(i)
      } else {
        fieldConverters(i).set(writable)
      }
      i += 1
    }
    currentRow
  }

  private[this] def newConverter(dataType: DataType, updater: OrcDataUpdater): Converter =
    dataType match {
      case NullType =>
        new Converter {
          override def set(value: Any): Unit = updater.setNullAt()
        }

      case BooleanType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.setBoolean(value.asInstanceOf[BooleanWritable].get)
        }

      case ByteType =>
        new Converter {
          override def set(value: Any): Unit = updater.setByte(value.asInstanceOf[ByteWritable].get)
        }

      case ShortType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.setShort(value.asInstanceOf[ShortWritable].get)
        }

      case IntegerType =>
        new Converter {
          override def set(value: Any): Unit = updater.setInt(value.asInstanceOf[IntWritable].get)
        }

      case LongType =>
        new Converter {
          override def set(value: Any): Unit = updater.setLong(value.asInstanceOf[LongWritable].get)
        }

      case FloatType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.setFloat(value.asInstanceOf[FloatWritable].get)
        }

      case DoubleType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.setDouble(value.asInstanceOf[DoubleWritable].get)
        }

      case StringType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.set(UTF8String.fromBytes(value.asInstanceOf[Text].copyBytes))
        }

      case BinaryType =>
        new Converter {
          override def set(value: Any): Unit = {
            val binary = value.asInstanceOf[BytesWritable]
            val bytes = new Array[Byte](binary.getLength)
            System.arraycopy(binary.getBytes, 0, bytes, 0, binary.getLength)
            updater.set(bytes)
          }
        }

      case DateType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.set(DateTimeUtils.fromJavaDate(value.asInstanceOf[DateWritable].get))
        }

      case TimestampType =>
        new Converter {
          override def set(value: Any): Unit =
            updater.set(DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[OrcTimestamp]))
        }

      case DecimalType.Fixed(precision, scale) =>
        new Converter {
          override def set(value: Any): Unit = {
            val decimal = value.asInstanceOf[HiveDecimalWritable].getHiveDecimal()
            val v = Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
            v.changePrecision(precision, scale)
            updater.set(v)
          }
        }

      case st: StructType =>
        new Converter {
          override def set(value: Any): Unit = {
            val orcStruct = value.asInstanceOf[OrcStruct]
            val mutableRow = new SpecificInternalRow(st)
            val fieldConverters: Array[Converter] = st.zipWithIndex.map { case (f, ordinal) =>
              if (missingColumnNames.contains(f.name)) {
                null
              } else {
                newConverter(f.dataType, new RowUpdater(mutableRow, ordinal))
              }
            }.toArray

            var i = 0
            val length = st.fields.length
            while (i < length) {
              val name = st(i).name
              val writable = orcStruct.getFieldValue(name)
              if (writable == null) {
                mutableRow.setNullAt(i)
              } else {
                fieldConverters(i).set(writable)
              }
              i += 1
            }
            updater.set(mutableRow)
          }
        }

      case ArrayType(elementType, _) =>
        new Converter {
          override def set(value: Any): Unit = {
            val arrayDataUpdater = new ArrayDataUpdater(updater)
            val converter = newConverter(elementType, arrayDataUpdater)
            value.asInstanceOf[OrcList[WritableComparable[_]]].asScala.foreach { x =>
              if (x == null) {
                arrayDataUpdater.set(null)
              } else {
                converter.set(x)
              }
            }
            arrayDataUpdater.end()
          }
        }

      case MapType(keyType, valueType, _) =>
        new Converter {
          override def set(value: Any): Unit = {
            val mapDataUpdater = new MapDataUpdater(keyType, valueType, updater)
            mapDataUpdater.set(value)
            mapDataUpdater.end()
          }
        }

      case udt: UserDefinedType[_] =>
        new Converter {
          override def set(value: Any): Unit = {
            val mutableRow = new SpecificInternalRow(new StructType().add("_col1", udt.sqlType))
            val converter = newConverter(udt.sqlType, new RowUpdater(mutableRow, 0))
            converter.set(value)
            updater.set(mutableRow.get(0, dataType))
          }
        }

      case _ =>
        throw new UnsupportedOperationException(s"$dataType is not supported yet.")
    }


  // --------------------------------------------------------------------------
  // Converter and Updaters
  // --------------------------------------------------------------------------

  trait Converter {
    def set(value: Any): Unit
  }

  trait OrcDataUpdater {
    def setNullAt(): Unit = ()
    def set(value: Any): Unit = ()
    def setBoolean(value: Boolean): Unit = set(value)
    def setByte(value: Byte): Unit = set(value)
    def setShort(value: Short): Unit = set(value)
    def setInt(value: Int): Unit = set(value)
    def setLong(value: Long): Unit = set(value)
    def setDouble(value: Double): Unit = set(value)
    def setFloat(value: Float): Unit = set(value)
  }

  final class RowUpdater(row: InternalRow, i: Int) extends OrcDataUpdater {
    override def set(value: Any): Unit = row(i) = value
    override def setBoolean(value: Boolean): Unit = row.setBoolean(i, value)
    override def setByte(value: Byte): Unit = row.setByte(i, value)
    override def setShort(value: Short): Unit = row.setShort(i, value)
    override def setInt(value: Int): Unit = row.setInt(i, value)
    override def setLong(value: Long): Unit = row.setLong(i, value)
    override def setDouble(value: Double): Unit = row.setDouble(i, value)
    override def setFloat(value: Float): Unit = row.setFloat(i, value)
  }

  final class ArrayDataUpdater(updater: OrcDataUpdater) extends OrcDataUpdater {
    private val currentArray: ArrayBuffer[Any] = ArrayBuffer.empty[Any]

    override def set(value: Any): Unit = currentArray += value

    def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))
  }

  final class MapDataUpdater(
      keyType: DataType,
      valueType: DataType,
      updater: OrcDataUpdater)
    extends OrcDataUpdater {

    private val currentKeys: ArrayBuffer[Any] = ArrayBuffer.empty[Any]
    private val currentValues: ArrayBuffer[Any] = ArrayBuffer.empty[Any]

    private val keyConverter = newConverter(keyType, new OrcDataUpdater {
      override def set(value: Any): Unit = currentKeys += value
    })
    private val valueConverter = newConverter(valueType, new OrcDataUpdater {
      override def set(value: Any): Unit = currentValues += value
    })

    override def set(value: Any): Unit = {
      value.asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
        .entrySet().asScala.foreach { entry =>

        assert(entry != null)
        keyConverter.set(entry.getKey)
        assert(valueConverter != null)
        if (entry.getValue == null) {
          currentValues += null
        } else {
          valueConverter.set(entry.getValue)
        }
      }
    }

    def end(): Unit = updater.set(ArrayBasedMapData(currentKeys.toArray, currentValues.toArray))
  }
}
