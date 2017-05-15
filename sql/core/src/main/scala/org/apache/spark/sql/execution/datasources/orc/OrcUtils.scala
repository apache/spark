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

import java.io.IOException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io._
import org.apache.orc.{OrcFile, TypeDescription}
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.serde2.io.{DateWritable, HiveDecimalWritable}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object OrcUtils extends Logging {

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    val paths = SparkHadoopUtil.get.listLeafStatuses(fs, origPath)
      .filterNot(_.isDirectory)
      .map(_.getPath)
      .filterNot(_.getName.startsWith("_"))
      .filterNot(_.getName.startsWith("."))
    paths
  }

  private[orc] def readSchema(file: Path, conf: Configuration): Option[TypeDescription] = {
    try {
      val fs = file.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val reader = OrcFile.createReader(file, readerOptions)
      val schema = reader.getSchema
      if (schema.getFieldNames.size == 0) {
        None
      } else {
        Some(schema)
      }
    } catch {
      case _: IOException => None
    }
  }

  private[orc] def readSchema(sparkSession: SparkSession, files: Seq[FileStatus])
      : Option[StructType] = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    files.map(_.getPath).flatMap(readSchema(_, conf)).headOption.map { schema =>
      logDebug(s"Reading schema from file $files, got Hive schema string: $schema")
      CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    }
  }

  private[orc] def getSchemaString(schema: StructType): String = {
    schema.fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")
  }

  private[orc] def getTypeDescription(dataType: DataType) = dataType match {
    case st: StructType => TypeDescription.fromString(getSchemaString(st))
    case _ => TypeDescription.fromString(dataType.catalogString)
  }

  /**
   * Return a missing schema in a give ORC file.
   */
  private[orc] def getMissingSchema(
      resolver: Resolver,
      dataSchema: StructType,
      partitionSchema: StructType,
      file: Path,
      conf: Configuration): Option[StructType] = {
    try {
      val fs = file.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val reader = OrcFile.createReader(file, readerOptions)
      val schema = reader.getSchema
      if (schema.getFieldNames.size == 0) {
        None
      } else {
        val orcSchema = if (schema.getFieldNames.asScala.forall(_.startsWith("_col"))) {
          logInfo("Recover ORC schema with data schema")
          var schemaString = schema.toString
          dataSchema.zipWithIndex.foreach { case (field: StructField, index: Int) =>
            schemaString = schemaString.replace(s"_col$index:", s"${field.name}:")
          }
          TypeDescription.fromString(schemaString)
        } else {
          schema
        }

        var missingSchema = new StructType
        if (dataSchema.length > orcSchema.getFieldNames.size) {
          dataSchema.filter(x => partitionSchema.getFieldIndex(x.name).isEmpty).foreach { f =>
            if (!orcSchema.getFieldNames.asScala.exists(resolver(_, f.name))) {
              missingSchema = missingSchema.add(f)
            }
          }
        }
        Some(missingSchema)
      }
    } catch {
      case _: IOException => None
    }
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

    case _ =>
      throw new UnsupportedOperationException(s"$dataType is not supported yet.")
  }
}
