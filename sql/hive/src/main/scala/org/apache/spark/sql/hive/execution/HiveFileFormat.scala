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

package org.apache.spark.sql.hive.execution

import java.net.URI
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.{Deserializer, Serializer}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.{HadoopTableReader, HiveInspectors, HiveShim, HiveTableUtil}
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NextIterator, SerializableConfiguration, SerializableJobConf}

/**
 * `FileFormat` for reading/writing Hive tables.
 */
class HiveFileFormat(fileSinkConf: FileSinkDesc, private val tableMeta: CatalogTable)
  extends FileFormat with DataSourceRegister with Logging {

  def this() = this(null, null)

  override def shortName(): String = "hive"

  // Due to implementation limitations, we can't read a partial data file for Hive tables.
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException(s"inferSchema is not supported for hive data source.")
  }

  def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hiveQlTable = HiveClientImpl.toHiveTable(tableMeta)
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata)

    // append columns ids and names before broadcasting the hadoop conf
    addColumnMetadataToConf(hadoopConf, tableDesc, dataSchema, requiredSchema)
    SparkHadoopUtil.get.appendS3AndSparkHadoopConfigurations(
      sparkSession.sparkContext.conf, hadoopConf)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    if (tableMeta.partitionColumnNames.nonEmpty) {
      (file: PartitionedFile) => {
        val (partDeserializer, inputFormatClass, partProps) = file.partitionMeta.asInstanceOf[
          (Class[Deserializer], Class[InputFormat[Writable, Writable]], Properties)]
        val hconf = broadcastedHadoopConf.value.value
        val jobConf = HiveFileFormat.getJobConf(hconf, file.filePath, tableDesc)

        // SPARK-13709: For SerDes like AvroSerDe, some essential information (e.g. Avro schema
        // information) may be defined in table properties. Here we should merge table properties
        // and partition properties before initializing the deserializer. Note that partition
        // properties take a higher priority here. For example, a partition may have a different
        // SerDe as the one defined in table properties.
        val deserializer = partDeserializer.newInstance()
        val props = new Properties(tableDesc.getProperties)
        partProps.asScala.foreach {
          case (key, value) => props.setProperty(key, value)
        }
        deserializer.initialize(hconf, props)
        // get the table deserializer
        val tableSerDe = tableDesc.getDeserializerClass.newInstance()
        tableSerDe.initialize(hconf, tableDesc.getProperties)

        val inputFormat = ReflectionUtils.newInstance(inputFormatClass, jobConf)
        inputFormat match {
          case c: Configurable => c.setConf(jobConf)
          case _ =>
        }

        val mutableRow = new SpecificInternalRow(requiredSchema.map(_.dataType))

        val inputSplits = inputFormat.getSplits(jobConf, 0)
        inputSplits.iterator.flatMap { split =>
          val iter = HiveFileFormat.readSplit(split, inputFormat, jobConf)
          HadoopTableReader.fillObject(
            iter, deserializer, mutableRow, requiredSchema.map(_.name), tableSerDe)
        }
      }
    } else {
      val inputFormatClass = tableDesc.getInputFileFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

      (file: PartitionedFile) => {
        val hconf = broadcastedHadoopConf.value.value
        val jobConf = HiveFileFormat.getJobConf(hconf, file.filePath, tableDesc)

        val deserializer = tableDesc.getDeserializerClass.newInstance()
        deserializer.initialize(hconf, tableDesc.getProperties)

        val inputFormat = ReflectionUtils.newInstance(inputFormatClass, jobConf)
        inputFormat match {
          case c: Configurable => c.setConf(jobConf)
          case _ =>
        }

        val mutableRow = new SpecificInternalRow(requiredSchema.map(_.dataType))

        val inputSplits = inputFormat.getSplits(jobConf, 0)
        inputSplits.toIterator.flatMap { split =>
          val iter = HiveFileFormat.readSplit(split, inputFormat, jobConf)
          HadoopTableReader.fillObject(
            iter, deserializer, mutableRow, requiredSchema.map(_.name), deserializer)
        }
      }
    }
  }

  private def addColumnMetadataToConf(
      hiveConf: Configuration,
      tableDesc: TableDesc,
      dataSchema: StructType,
      requiredSchema: StructType) {
    // Specifies needed column IDs for those non-partitioning columns.
    val neededColumnIDs = requiredSchema.map(f => dataSchema.fieldIndex(f.name)).map(Int.box)

    HiveShim.appendReadColumns(hiveConf, neededColumnIDs, requiredSchema.map(_.name))

    val deserializer = tableDesc.getDeserializerClass.newInstance
    deserializer.initialize(hiveConf, tableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs.asScala
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, dataSchema.map(_.name).mkString(","))
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val tableDesc = fileSinkConf.getTableInfo
    conf.set("mapred.output.format.class", tableDesc.getOutputFileFormatClassName)

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = sparkSession.sparkContext.conf.getBoolean("spark.speculation", false)
    val outputCommitterClass = conf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    // Add table properties from storage handler to hadoopConf, so any custom storage
    // handler settings can be set to hadoopConf
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, conf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, conf)

    // Avoid referencing the outer object.
    val fileSinkConfSer = fileSinkConf
    new OutputWriterFactory {
      private val jobConf = new SerializableJobConf(new JobConf(conf))
      @transient private lazy val outputFormat =
        jobConf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

      override def getFileExtension(context: TaskAttemptContext): String = {
        Utilities.getFileExtension(jobConf.value, fileSinkConfSer.getCompressed, outputFormat)
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new HiveOutputWriter(path, fileSinkConfSer, jobConf.value, dataSchema)
      }
    }
  }

  override def equals(o: Any): Boolean = o match {
    case h: HiveFileFormat => this.tableMeta == h.tableMeta
    case _ => false
  }

  override def hashCode(): Int = this.tableMeta.hashCode()
}

class HiveOutputWriter(
    path: String,
    fileSinkConf: FileSinkDesc,
    jobConf: JobConf,
    dataSchema: StructType) extends OutputWriter with HiveInspectors {

  private def tableDesc = fileSinkConf.getTableInfo

  private val serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  private val hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
    jobConf,
    tableDesc,
    serializer.getSerializedClass,
    fileSinkConf,
    new Path(path),
    Reporter.NULL)

  private val standardOI = ObjectInspectorUtils
    .getStandardObjectInspector(
      tableDesc.getDeserializer.getObjectInspector,
      ObjectInspectorCopyOption.JAVA)
    .asInstanceOf[StructObjectInspector]

  private val fieldOIs =
    standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = dataSchema.map(_.dataType).toArray
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
  private val outputData = new Array[Any](fieldOIs.length)

  override def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fieldOIs.length) {
      outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
      i += 1
    }
    hiveWriter.write(serializer.serialize(outputData, standardOI))
  }

  override def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    hiveWriter.close(false)
  }
}

object HiveFileFormat {
  def getJobConf(conf: Configuration, path: String, tableDesc: TableDesc): JobConf = {
    val jobConf = new JobConf(conf)
    FileInputFormat.setInputPaths(jobConf, new Path(new URI(path)))
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    jobConf
  }

  def readSplit(
      split: InputSplit,
      inputFormat: InputFormat[Writable, Writable],
      jobConf: JobConf): Iterator[Writable] = {
    val reader = inputFormat.getRecordReader(split, jobConf, Reporter.NULL)
    new NextIterator[Writable] {
      private val key = reader.createKey()
      private val value = reader.createValue()

      override protected def getNext(): Writable = {
        finished = !reader.next(key, value)
        value
      }

      override protected def close(): Unit = {
        reader.close()
      }
    }
  }
}
