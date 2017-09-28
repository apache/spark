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

package org.apache.spark.sql.hive.orc

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.orc.{OrcFile, TypeDescription}
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.serde2.io.{DateWritable, HiveDecimalWritable}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object OrcUtils {
  /**
   * Read ORC file schema. This method is used in `inferSchema`.
   */
  private[orc] def readSchema(file: Path, conf: Configuration): Option[TypeDescription] = {
    try {
      val options = OrcFile.readerOptions(conf).filesystem(FileSystem.get(conf))
      val reader = OrcFile.createReader(file, options)
      val schema = reader.getSchema
      if (schema.getFieldNames.isEmpty) {
        None
      } else {
        Some(schema)
      }
    } catch {
      case _: IOException => None
    }
  }

  /**
   * Return ORC schema with schema field name correction.
   */
  private[orc] def getSchema(dataSchema: StructType, filePath: String, conf: Configuration) = {
    val hdfsPath = new Path(filePath)
    val fs = hdfsPath.getFileSystem(conf)
    val reader = OrcFile.createReader(hdfsPath, OrcFile.readerOptions(conf).filesystem(fs))
    val rawSchema = reader.getSchema
    val orcSchema = if (!rawSchema.getFieldNames.isEmpty &&
        rawSchema.getFieldNames.asScala.forall(_.startsWith("_col"))) {
      var schemaString = rawSchema.toString
      dataSchema.zipWithIndex.foreach { case (field: StructField, index: Int) =>
        schemaString = schemaString.replace(s"_col$index:", s"${field.name}:")
      }
      TypeDescription.fromString(schemaString)
    } else {
      rawSchema
    }
    orcSchema
  }

  /**
   * Return a ORC schema string for ORCStruct.
   */
  private[orc] def getSchemaString(schema: StructType): String = {
    schema.fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")
  }

  private[orc] def getTypeDescription(dataType: DataType) = dataType match {
    case st: StructType => TypeDescription.fromString(getSchemaString(st))
    case _ => TypeDescription.fromString(dataType.catalogString)
  }

  /**
   * Return a Orc value object for the given Spark schema.
   */
  private[orc] def createOrcValue(dataType: DataType) =
    OrcStruct.createValue(getTypeDescription(dataType))

  /**
   * Convert Apache ORC OrcStruct to Apache Spark InternalRow.
   * If internalRow is not None, fill into it. Otherwise, create a SpecificInternalRow and use it.
   */
  private[orc] def convertOrcStructToInternalRow(
      orcStruct: OrcStruct,
      schema: StructType,
      valueWrappers: Option[Seq[Any => Any]] = None,
      internalRow: Option[InternalRow] = None): InternalRow = {
    val mutableRow = internalRow.getOrElse(new SpecificInternalRow(schema.map(_.dataType)))
    val wrappers = valueWrappers.getOrElse(schema.fields.map(_.dataType).map(getValueWrapper).toSeq)
    for (schemaIndex <- 0 until schema.length) {
      val writable = orcStruct.getFieldValue(schema(schemaIndex).name)
      if (writable == null) {
        mutableRow.setNullAt(schemaIndex)
      } else {
        mutableRow(schemaIndex) = wrappers(schemaIndex)(writable)
      }
    }
    mutableRow
  }

  private def withNullSafe(f: Any => Any): Any => Any = {
    input => if (input == null) null else f(input)
  }

  /**
   * Builds a catalyst-value return function ahead of time according to DataType
   * to avoid pattern matching and branching costs per row.
   */
  private[orc] def getValueWrapper(dataType: DataType): Any => Any = dataType match {
    case NullType => _ => null
    case BooleanType => withNullSafe(o => o.asInstanceOf[BooleanWritable].get)
    case ByteType => withNullSafe(o => o.asInstanceOf[ByteWritable].get)
    case ShortType => withNullSafe(o => o.asInstanceOf[ShortWritable].get)
    case IntegerType => withNullSafe(o => o.asInstanceOf[IntWritable].get)
    case LongType => withNullSafe(o => o.asInstanceOf[LongWritable].get)
    case FloatType => withNullSafe(o => o.asInstanceOf[FloatWritable].get)
    case DoubleType => withNullSafe(o => o.asInstanceOf[DoubleWritable].get)
    case StringType => withNullSafe(o => UTF8String.fromBytes(o.asInstanceOf[Text].copyBytes))
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
          dataType.asInstanceOf[StructType])
        structValue
      }
    case ArrayType(elementType, _) =>
      withNullSafe { o =>
        val wrapper = getValueWrapper(elementType)
        val data = new scala.collection.mutable.ArrayBuffer[Any]
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
    case udt: UserDefinedType[_] => withNullSafe { o => getValueWrapper(udt.sqlType)(o) }
    case _ => throw new UnsupportedOperationException(s"$dataType is not supported yet.")
  }

  /**
   * Convert Apache Spark InternalRow to Apache ORC OrcStruct.
   */
  private[orc] def convertInternalRowToOrcStruct(
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

  /**
   * Builds a WritableComparable-return function ahead of time according to DataType
   * to avoid pattern matching and branching costs per row.
   */
  private[orc] def getWritableWrapper(dataType: DataType): Any => Any = dataType match {
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
    case _ => throw new UnsupportedOperationException(s"$dataType is not supported yet.")
  }
}
