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

package org.apache.spark.sql.avro

import java.math.BigDecimal
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic._
import org.apache.avro.util.Utf8

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_DAY
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
/**
 * A deserializer to deserialize data in avro format to data in catalyst format.
 */
class AvroDeserializer(
  rootAvroType: Schema,
  rootCatalystType: DataType,
  logicalTypeUpdater: AvroLogicalTypeCatalystMapper) {

  private lazy val decimalConversions = new DecimalConversion()

  private val converter: Any => Any = rootCatalystType match {
    // A shortcut for empty schema.
    case st: StructType if st.isEmpty =>
      (data: Any) => InternalRow.empty

    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      val fieldUpdater = new RowUpdater(resultRow)
      val writer = getRecordWriter(rootAvroType, st, Nil)
      (data: Any) => {
        val record = data.asInstanceOf[GenericRecord]
        writer(fieldUpdater, record)
        resultRow
      }

    case _ =>
      val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
      val fieldUpdater = new RowUpdater(tmpRow)
      val writer = newWriter(rootAvroType, rootCatalystType, Nil)
      (data: Any) => {
        writer(DataDeserializer(fieldUpdater, 0, data))
        tmpRow.get(0, rootCatalystType)
      }
  }

  def deserialize(data: Any): Any = converter(data)

  /**
   * Creates a writer to write avro values to Catalyst values at the given ordinal with the given
   * updater.
   */
  private def newWriter(
      avroType: Schema,
      catalystType: DataType,
      path: List[String]): DataDeserializer => Unit =
    (avroType.getType, catalystType) match {
      case _ if logicalTypeUpdater.deserialize.isDefinedAt(avroType.getLogicalType) =>
        logicalTypeUpdater.deserialize(avroType.getLogicalType)

      case (NULL, NullType) => dataUpdater =>
        dataUpdater.updater.setNullAt(dataUpdater.ordinal)

      // TODO: we can avoid boxing if future version of avro provide primitive accessors.
      case (BOOLEAN, BooleanType) => dataUpdater =>
        dataUpdater.updater.setBoolean(
          dataUpdater.ordinal,
          dataUpdater.value.asInstanceOf[Boolean])

      case (INT, IntegerType) => dataUpdater =>
        dataUpdater.updater.setInt(
          dataUpdater.ordinal,
          dataUpdater.value.asInstanceOf[Int])

      case (INT, DateType) => dataUpdater =>
        dataUpdater.updater.setInt(
          dataUpdater.ordinal,
          dataUpdater.value.asInstanceOf[Int]
        )

      case (LONG, LongType) => dataUpdater =>
        dataUpdater.updater.setLong(
          dataUpdater.ordinal,
          dataUpdater.value.asInstanceOf[Long])

      case (LONG, TimestampType) => avroType.getLogicalType match {
        case _: TimestampMillis => dataUpdater =>
          dataUpdater.updater.setLong(
            dataUpdater.ordinal,
            dataUpdater.value.asInstanceOf[Long] * 1000)
        case _: TimestampMicros => dataUpdater =>
          dataUpdater.updater.setLong(
            dataUpdater.ordinal,
            dataUpdater.value.asInstanceOf[Long])
        case null => dataUpdater =>
          // For backward compatibility, if the Avro type is Long and it is not logical type,
          // the value is processed as timestamp type with millisecond precision.
          dataUpdater.updater.setLong(
            dataUpdater.ordinal,
            dataUpdater.value.asInstanceOf[Long] * 1000)
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Avro logical type ${other} to Catalyst Timestamp type.")
      }

      // Before we upgrade Avro to 1.8 for logical type support, spark-avro converts Long to Date.
      // For backward compatibility, we still keep this conversion.
      case (LONG, DateType) => dataUpdater =>
        dataUpdater.updater.setInt(
          dataUpdater.ordinal,
          (dataUpdater.value.asInstanceOf[Long] / MILLIS_PER_DAY).toInt
        )

      case (FLOAT, FloatType) => dataUpdater =>
          dataUpdater.updater.setFloat(
            dataUpdater.ordinal,
            dataUpdater.value.asInstanceOf[Float])

      case (DOUBLE, DoubleType) => dataUpdater =>
        dataUpdater.updater.setDouble(
          dataUpdater.ordinal,
          dataUpdater.value.asInstanceOf[Double])

      case (STRING, StringType) => dataUpdater =>
        val str = dataUpdater.value match {
          case s: String => UTF8String.fromString(s)
          case s: Utf8 =>
            val bytes = new Array[Byte](s.getByteLength)
            System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
            UTF8String.fromBytes(bytes)
        }
        dataUpdater.updater.set(dataUpdater.ordinal, str)

      case (ENUM, StringType) => dataUpdater =>
        dataUpdater.updater.set(
          dataUpdater.ordinal,
          UTF8String.fromString(dataUpdater.value.toString))

      case (FIXED, BinaryType) => dataUpdater =>
        dataUpdater.updater.set(
          dataUpdater.ordinal,
          dataUpdater.value.asInstanceOf[GenericFixed].bytes().clone())

      case (BYTES, BinaryType) => dataUpdater =>
        val bytes = dataUpdater.value match {
          case b: ByteBuffer =>
            val bytes = new Array[Byte](b.remaining)
            b.get(bytes)
            bytes
          case b: Array[Byte] => b
          case other => throw new RuntimeException(s"$other is not a valid avro binary.")
        }
        dataUpdater.updater.set(dataUpdater.ordinal, bytes)

      case (FIXED, d: DecimalType) => dataUpdater =>
        val bigDecimal = decimalConversions.fromFixed(
          dataUpdater.value.asInstanceOf[GenericFixed],
          avroType,
          LogicalTypes.decimal(d.precision, d.scale))
        val decimal = createDecimal(bigDecimal, d.precision, d.scale)
        dataUpdater.updater.setDecimal(dataUpdater.ordinal, decimal)

      case (BYTES, d: DecimalType) => dataUpdater =>
        val bigDecimal = decimalConversions.fromBytes(
          dataUpdater.value.asInstanceOf[ByteBuffer],
          avroType,
          LogicalTypes.decimal(d.precision, d.scale))
        val decimal = createDecimal(bigDecimal, d.precision, d.scale)
        dataUpdater.updater.setDecimal(dataUpdater.ordinal, decimal)

      case (RECORD, st: StructType) =>
        val writeRecord = getRecordWriter(avroType, st, path)
        dataUpdater =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row),
            dataUpdater.value.asInstanceOf[GenericRecord])
          dataUpdater.updater.set(dataUpdater.ordinal, row)

      case (ARRAY, ArrayType(elementType, containsNull)) =>
        val elementWriter = newWriter(avroType.getElementType, elementType, path)
        dataUpdater =>
          val array = dataUpdater.value.asInstanceOf[GenericData.Array[Any]]
          val len = array.size()
          val result = createArrayData(elementType, len)
          val elementUpdater = new ArrayDataUpdater(result)

          var i = 0
          while (i < len) {
            val element = array.get(i)
            if (element == null) {
              if (!containsNull) {
                throw new RuntimeException(s"Array value at path ${path.mkString(".")} is not " +
                  "allowed to be null")
              } else {
                elementUpdater.setNullAt(i)
              }
            } else {
              elementWriter(DataDeserializer(elementUpdater, i, element))
            }
            i += 1
          }

          dataUpdater.updater.set(dataUpdater.ordinal, result)

      case (MAP, MapType(keyType, valueType, valueContainsNull)) if keyType == StringType =>
        val keyWriter = newWriter(SchemaBuilder.builder().stringType(), StringType, path)
        val valueWriter = newWriter(avroType.getValueType, valueType, path)
        dataUpdater =>
          val map = dataUpdater.value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
          val keyArray = createArrayData(keyType, map.size())
          val keyUpdater = new ArrayDataUpdater(keyArray)
          val valueArray = createArrayData(valueType, map.size())
          val valueUpdater = new ArrayDataUpdater(valueArray)
          val iter = map.entrySet().iterator()
          var i = 0
          while (iter.hasNext) {
            val entry = iter.next()
            assert(entry.getKey != null)
            keyWriter(DataDeserializer(keyUpdater, i, entry.getKey))
            if (entry.getValue == null) {
              if (!valueContainsNull) {
                throw new RuntimeException(s"Map value at path ${path.mkString(".")} is not " +
                  "allowed to be null")
              } else {
                valueUpdater.setNullAt(i)
              }
            } else {
              valueWriter(DataDeserializer(valueUpdater, i, entry.getValue))
            }
            i += 1
          }

          // The Avro map will never have null or duplicated map keys, it's safe to create a
          // ArrayBasedMapData directly here.
          dataUpdater.updater.set(
            dataUpdater.ordinal,
            new ArrayBasedMapData(keyArray, valueArray))

      case (UNION, _) =>
        val allTypes = avroType.getTypes.asScala
        val nonNullTypes = allTypes.filter(_.getType != NULL)
        val nonNullAvroType = Schema.createUnion(nonNullTypes.asJava)
        if (nonNullTypes.nonEmpty) {
          if (nonNullTypes.length == 1) {
            newWriter(nonNullTypes.head, catalystType, path)
          } else {
            nonNullTypes.map(_.getType) match {
              case Seq(a, b) if Set(a, b) == Set(INT, LONG) && catalystType == LongType =>
                dataUpdater => dataUpdater.value match {
                  case null =>
                    dataUpdater.updater.setNullAt(dataUpdater.ordinal)
                  case l: java.lang.Long =>
                    dataUpdater.updater.setLong(dataUpdater.ordinal, l)
                  case i: java.lang.Integer =>
                    dataUpdater.updater.setLong(dataUpdater.ordinal, i.longValue())
                }

              case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && catalystType == DoubleType =>
                dataUpdater => dataUpdater.value match {
                  case null =>
                    dataUpdater.updater.setNullAt(dataUpdater.ordinal)
                  case d: java.lang.Double =>
                    dataUpdater.updater.setDouble(dataUpdater.ordinal, d)
                  case f: java.lang.Float =>
                    dataUpdater.updater.setDouble(dataUpdater.ordinal, f.doubleValue())
                }

              case _ =>
                catalystType match {
                  case st: StructType if st.length == nonNullTypes.size =>
                    val fieldWriters = nonNullTypes.zip(st.fields).map {
                      case (schema, field) => newWriter(schema, field.dataType, path :+ field.name)
                    }.toArray
                    dataUpdater => {
                      val row = new SpecificInternalRow(st)
                      val fieldUpdater = new RowUpdater(row)
                      val i = GenericData.get().resolveUnion(nonNullAvroType, dataUpdater.value)
                      fieldWriters(i)(DataDeserializer(fieldUpdater, i, dataUpdater.value))
                      dataUpdater.updater.set(dataUpdater.ordinal, row)
                    }

                  case _ =>
                    throw new IncompatibleSchemaException(
                      s"Cannot convert Avro to catalyst because schema at path " +
                        s"${path.mkString(".")} is not compatible " +
                        s"(avroType = $avroType, sqlType = $catalystType).\n" +
                        s"Source Avro schema: $rootAvroType.\n" +
                        s"Target Catalyst type: $rootCatalystType")
                }
            }
          }
        } else {
          dataUpdater => dataUpdater.updater.setNullAt(dataUpdater.ordinal)
        }

      case _ =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Avro to catalyst because schema at path ${path.mkString(".")} " +
            s"is not compatible (avroType = $avroType, sqlType = $catalystType).\n" +
            s"Source Avro schema: $rootAvroType.\n" +
            s"Target Catalyst type: $rootCatalystType")
    }

  // TODO: move the following method in Decimal object on creating Decimal from BigDecimal?
  private def createDecimal(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(decimal, precision, scale)
    }
  }

  private def getRecordWriter(
      avroType: Schema,
      sqlType: StructType,
      path: List[String]): (CatalystDataUpdater, GenericRecord) => Unit = {
    val validFieldIndexes = ArrayBuffer.empty[Int]
    val fieldWriters = ArrayBuffer.empty[(CatalystDataUpdater, Any) => Unit]

    val length = sqlType.length
    var i = 0
    while (i < length) {
      val sqlField = sqlType.fields(i)
      val avroField = avroType.getField(sqlField.name)
      if (avroField != null) {
        validFieldIndexes += avroField.pos()

        val baseWriter = newWriter(avroField.schema(), sqlField.dataType, path :+ sqlField.name)
        val ordinal = i
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(DataDeserializer(fieldUpdater, ordinal, value))
          }
        }
        fieldWriters += fieldWriter
      } else if (!sqlField.nullable) {
        throw new IncompatibleSchemaException(
          s"""
             |Cannot find non-nullable field ${path.mkString(".")}.${sqlField.name} in Avro schema.
             |Source Avro schema: $rootAvroType.
             |Target Catalyst type: $rootCatalystType.
           """.stripMargin)
      }
      i += 1
    }

    (fieldUpdater, record) => {
      var i = 0
      while (i < validFieldIndexes.length) {
        fieldWriters(i)(fieldUpdater, record.get(validFieldIndexes(i)))
        i += 1
      }
    }
  }

  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

}
