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

import java.io._
import java.net.URI

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc._
import org.apache.orc.OrcFile.ReaderOptions
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.apache.orc.mapreduce._
import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.serde2.io.{DateWritable, HiveDecimalWritable}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

class DefaultSource extends OrcFileFormat

/**
 * New ORC File Format based on Apache ORC 1.4.x and above.
 */
class OrcFileFormat
  extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def shortName(): String = "orc"

  override def toString: String = "ORC"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[OrcFileFormat]

  override def inferSchema(
      spark: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val options = OrcFile.readerOptions(conf).filesystem(fs)
    files.map(_.getPath).flatMap(OrcFileFormat.readSchema(_, options)).headOption.map { schema =>
      logDebug(s"Reading schema from file $files, got Hive schema string: $schema")
      CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)

    val conf = job.getConfiguration

    val writerOptions = OrcFile.writerOptions(conf)

    conf.set(
      OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute,
      OrcFileFormat.getSchemaString(dataSchema))

    conf.set(
      OrcConf.COMPRESS.getAttribute,
      orcOptions.compressionCodec)

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(OrcConf.COMPRESS.getAttribute)
          OrcOptions.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
  }

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.orcColumnarBatchReaderEnabled &&
      conf.orcVectorizedReaderEnabled &&
      conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    // Predicate Push Down
    if (sparkSession.sessionState.conf.orcFilterPushDown) {
      OrcFilters.createFilter(dataSchema, filters).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, dataSchema.fieldNames)
        logDebug(s"Pushed Predicate: $f")
      }
    }

    // Column Selection
    val columns = if (requiredSchema.isEmpty) {
      // Due to ORC-233, we need to specify at least one.
      "0"
    } else {
      requiredSchema.map(f => dataSchema.fieldIndex(f.name)).mkString(",")
    }
    hadoopConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, columns)
    logDebug(s"${OrcConf.INCLUDE_COLUMNS.getAttribute}=$columns")

    val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val useColumnarBatchReader =
      sparkSession.sessionState.conf.orcColumnarBatchReaderEnabled &&
      supportBatch(sparkSession, resultSchema)
    val enableVectorizedReader =
      sparkSession.sessionState.conf.orcVectorizedReaderEnabled &&
        resultSchema.forall(_.dataType.isInstanceOf[AtomicType])

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val conf = broadcastedConf.value.value
      val (orcSchema, _) =
        OrcFileFormat.getSchemaAndNumberOfRows(dataSchema, file.filePath, conf)
      val useIndex = requiredSchema.fieldNames.zipWithIndex.forall { case (name, index) =>
        name.equals(orcSchema.getFieldNames.get(index))
      }

      if (orcSchema.getFieldNames.isEmpty) {
        Iterator.empty
      } else {
        val split =
          new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val taskAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
        val partitionValues = file.partitionValues

        createIterator(
          split,
          taskAttemptContext,
          orcSchema,
          requiredSchema,
          partitionSchema,
          partitionValues,
          useIndex,
          useColumnarBatchReader,
          enableVectorizedReader)
      }
    }
  }

  /**
   * Create one of the following iterators.
   *
   * - An iterator with ColumnarBatch.
   *   This is used when supportBatch(sparkSession, resultSchema) is true.
   *   Whole-stage codegen understands ColumnarBatch which offers significant
   *   performance gains.
   *
   * - An iterator with InternalRow based on ORC RowBatch.
   *   This is used when ORC_VECTORIZED_READER_ENABLED is true and
   *   the schema has only atomic fields.
   *
   * - An iterator with InternalRow based on ORC OrcMapreduceRecordReader.
   *   This is the default iterator for the other cases.
   */
  private def createIterator(
      split: FileSplit,
      taskAttemptContext: TaskAttemptContext,
      orcSchema: TypeDescription,
      requiredSchema: StructType,
      partitionSchema: StructType,
      partitionValues: InternalRow,
      useIndex: Boolean,
      columnarBatch: Boolean,
      enableVectorizedReader: Boolean): Iterator[InternalRow] = {
    val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)

    if (columnarBatch) {
      assert(enableVectorizedReader)
      val reader = new OrcColumnarBatchReader
      reader.initialize(split, taskAttemptContext)
      reader.setRequiredSchema(
        orcSchema,
        requiredSchema,
        partitionSchema,
        partitionValues,
        useIndex)
      val iter = new RecordReaderIterator(reader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
      iter.asInstanceOf[Iterator[InternalRow]]
    } else if (enableVectorizedReader) {
      val iter = new OrcRecordIterator
      iter.initialize(
        split,
        taskAttemptContext,
        orcSchema,
        requiredSchema,
        partitionSchema,
        partitionValues,
        useIndex)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))

      val unsafeProjection = UnsafeProjection.create(resultSchema)
      iter.map(unsafeProjection)
    } else {
      val orcRecordReader = OrcFileFormat.ORC_INPUT_FORMAT
        .createRecordReader(split, taskAttemptContext)
      val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))

      val mutableRow = new SpecificInternalRow(resultSchema.map(_.dataType))
      val unsafeProjection = UnsafeProjection.create(resultSchema)

      // Initialize the partition column values once.
      for (i <- requiredSchema.length until resultSchema.length) {
        val value = partitionValues.get(i - requiredSchema.length, resultSchema(i).dataType)
        mutableRow.update(i, value)
      }

      iter.map { value =>
        unsafeProjection(OrcFileFormat.convertOrcStructToInternalRow(
          value, requiredSchema, useIndex, Some(mutableRow)))
      }
    }
  }
}

/**
 * New ORC File Format companion object.
 */
private[sql] object OrcFileFormat extends Logging {
  private def checkFieldName(name: String): Unit = {
    try {
      TypeDescription.fromString(s"struct<$name:int>")
    } catch {
      case _: IllegalArgumentException =>
        throw new AnalysisException(
          s"""Column name "$name" contains invalid character(s).
             |Please use alias to rename it.
           """.stripMargin.split("\n").mkString(" ").trim)
    }
  }

  def checkFieldNames(schema: StructType): StructType = {
    schema.fieldNames.foreach(checkFieldName)
    schema
  }

  lazy val ORC_INPUT_FORMAT = new OrcInputFormat[OrcStruct]

  /**
   * Read ORC file schema. This method is used in `inferSchema`.
   */
  private[orc] def readSchema(file: Path, conf: ReaderOptions): Option[TypeDescription] = {
    try {
      val reader = OrcFile.createReader(file, conf)
      Some(reader.getSchema)
    } catch {
      case _: IOException => None
    }
  }

  /**
   * Return ORC schema with schema field name correction and the total number of rows.
   */
  private[orc] def getSchemaAndNumberOfRows(
      dataSchema: StructType,
      filePath: String,
      conf: Configuration) = {
    val hdfsPath = new Path(filePath)
    val fs = hdfsPath.getFileSystem(conf)
    val reader = OrcFile.createReader(hdfsPath, OrcFile.readerOptions(conf).filesystem(fs))
    val rawSchema = reader.getSchema
    val orcSchema = if (rawSchema.getFieldNames.asScala.forall(_.startsWith("_col"))) {
      logInfo("Recover ORC schema with data schema")
      var schemaString = rawSchema.toString
      dataSchema.zipWithIndex.foreach { case (field: StructField, index: Int) =>
        schemaString = schemaString.replace(s"_col$index:", s"${field.name}:")
      }
      TypeDescription.fromString(schemaString)
    } else {
      rawSchema
    }
    (orcSchema, reader.getNumberOfRows)
  }

  /**
   * Convert Apache ORC OrcStruct to Apache Spark InternalRow.
   * If internalRow is not None, fill into it. Otherwise, create a SpecificInternalRow and use it.
   */
  private[orc] def convertOrcStructToInternalRow(
      orcStruct: OrcStruct,
      schema: StructType,
      useIndex: Boolean = false,
      internalRow: Option[InternalRow] = None): InternalRow = {

    val mutableRow = internalRow.getOrElse(new SpecificInternalRow(schema.map(_.dataType)))

    for (schemaIndex <- 0 until schema.length) {
      val writable = if (useIndex) {
        orcStruct.getFieldValue(schemaIndex)
      } else {
        orcStruct.getFieldValue(schema(schemaIndex).name)
      }
      if (writable == null) {
        mutableRow.setNullAt(schemaIndex)
      } else {
        mutableRow(schemaIndex) = getCatalystValue(writable, schema(schemaIndex).dataType)
      }
    }

    mutableRow
  }


  private[orc] def getTypeDescription(dataType: DataType) = dataType match {
    case st: StructType => TypeDescription.fromString(getSchemaString(st))
    case _ => TypeDescription.fromString(dataType.catalogString)
  }

  /**
   * Return a ORC schema string for ORCStruct.
   */
  private[orc] def getSchemaString(schema: StructType): String = {
    schema.fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")
  }

  /**
   * Return a Orc value object for the given Spark schema.
   */
  private[orc] def createOrcValue(dataType: DataType) =
    OrcStruct.createValue(getTypeDescription(dataType))

  /**
   * Convert Apache Spark InternalRow to Apache ORC OrcStruct.
   */
  private[orc] def convertInternalRowToOrcStruct(
      row: InternalRow,
      schema: StructType,
      struct: Option[OrcStruct] = None): OrcStruct = {

    val orcStruct = struct.getOrElse(createOrcValue(schema).asInstanceOf[OrcStruct])

    for (schemaIndex <- 0 until schema.length) {
      val fieldType = schema(schemaIndex).dataType
      val fieldValue = if (row.isNullAt(schemaIndex)) {
        null
      } else {
        getWritable(row.get(schemaIndex, fieldType), fieldType)
      }
      orcStruct.setFieldValue(schemaIndex, fieldValue)
    }

    orcStruct
  }

  /**
   * Return WritableComparable from Spark catalyst values.
   */
  private[orc] def getWritable(value: Object, dataType: DataType): WritableComparable[_] = {
    if (value == null) {
      null
    } else {
      dataType match {
        case NullType => null

        case BooleanType => new BooleanWritable(value.asInstanceOf[Boolean])

        case ByteType => new ByteWritable(value.asInstanceOf[Byte])
        case ShortType => new ShortWritable(value.asInstanceOf[Short])
        case IntegerType => new IntWritable(value.asInstanceOf[Int])
        case LongType => new LongWritable(value.asInstanceOf[Long])

        case FloatType => new FloatWritable(value.asInstanceOf[Float])
        case DoubleType => new DoubleWritable(value.asInstanceOf[Double])

        case StringType => new Text(value.asInstanceOf[UTF8String].getBytes)

        case BinaryType => new BytesWritable(value.asInstanceOf[Array[Byte]])

        case DateType => new DateWritable(DateTimeUtils.toJavaDate(value.asInstanceOf[Int]))

        case TimestampType =>
          val us = value.asInstanceOf[Long]
          var seconds = us / DateTimeUtils.MICROS_PER_SECOND
          var micros = us % DateTimeUtils.MICROS_PER_SECOND
          if (micros < 0) {
            micros += DateTimeUtils.MICROS_PER_SECOND
            seconds -= 1
          }
          val t = new OrcTimestamp(seconds * 1000)
          t.setNanos(micros.toInt * 1000)
          t

        case _: DecimalType =>
          new HiveDecimalWritable(HiveDecimal.create(value.asInstanceOf[Decimal].toJavaBigDecimal))

        case st: StructType =>
          convertInternalRowToOrcStruct(value.asInstanceOf[InternalRow], st)

        case ArrayType(et, _) =>
          val data = value.asInstanceOf[ArrayData]
          val list = createOrcValue(dataType)
          for (i <- 0 until data.numElements()) {
            list.asInstanceOf[OrcList[WritableComparable[_]]]
              .add(getWritable(data.get(i, et), et))
          }
          list

        case MapType(keyType, valueType, _) =>
          val data = value.asInstanceOf[MapData]
          val map = createOrcValue(dataType)
            .asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
          data.foreach(keyType, valueType, { case (k, v) =>
            map.put(
              getWritable(k.asInstanceOf[Object], keyType),
              getWritable(v.asInstanceOf[Object], valueType))
          })
          map

        case udt: UserDefinedType[_] =>
          val udtRow = new SpecificInternalRow(Seq(udt.sqlType))
          udtRow(0) = value
          convertInternalRowToOrcStruct(udtRow,
            StructType(Seq(StructField("tmp", udt.sqlType)))).getFieldValue(0)

        case _ =>
          throw new UnsupportedOperationException(s"$dataType is not supported yet.")
      }
    }

  }

  /**
   * Return Spark Catalyst value from WritableComparable object.
   */
  private[orc] def getCatalystValue(value: WritableComparable[_], dataType: DataType): Any = {
    if (value == null) {
      null
    } else {
      dataType match {
        case NullType => null

        case BooleanType => value.asInstanceOf[BooleanWritable].get

        case ByteType => value.asInstanceOf[ByteWritable].get
        case ShortType => value.asInstanceOf[ShortWritable].get
        case IntegerType => value.asInstanceOf[IntWritable].get
        case LongType => value.asInstanceOf[LongWritable].get

        case FloatType => value.asInstanceOf[FloatWritable].get
        case DoubleType => value.asInstanceOf[DoubleWritable].get

        case StringType => UTF8String.fromBytes(value.asInstanceOf[Text].getBytes)

        case BinaryType =>
          val binary = value.asInstanceOf[BytesWritable]
          val bytes = new Array[Byte](binary.getLength)
          System.arraycopy(binary.getBytes, 0, bytes, 0, binary.getLength)
          bytes

        case DateType => DateTimeUtils.fromJavaDate(value.asInstanceOf[DateWritable].get)
        case TimestampType => DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[OrcTimestamp])

        case DecimalType.Fixed(precision, scale) =>
          val decimal = value.asInstanceOf[HiveDecimalWritable].getHiveDecimal()
          val v = Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
          v.changePrecision(precision, scale)
          v

        case _: StructType =>
          val structValue = convertOrcStructToInternalRow(
            value.asInstanceOf[OrcStruct],
            dataType.asInstanceOf[StructType])
          structValue

        case ArrayType(elementType, _) =>
          val data = new scala.collection.mutable.ArrayBuffer[Any]
          value.asInstanceOf[OrcList[WritableComparable[_]]].asScala.foreach { x =>
            data += getCatalystValue(x, elementType)
          }
          new GenericArrayData(data.toArray)

        case MapType(keyType, valueType, _) =>
          val map = new java.util.TreeMap[Any, Any]
          value
            .asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
            .entrySet().asScala.foreach { entry =>
            val k = getCatalystValue(entry.getKey, keyType)
            val v = getCatalystValue(entry.getValue, valueType)
            map.put(k, v)
          }
          ArrayBasedMapData(map.asScala)

        case udt: UserDefinedType[_] =>
          getCatalystValue(value, udt.sqlType)

        case _ =>
          throw new UnsupportedOperationException(s"$dataType is not supported yet.")
      }
    }
  }
}
