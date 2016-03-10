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

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.ql.io.orc.OrcFile.OrcTableProperties
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{InputFormat => MapRedInputFormat, JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.{HiveInspectors, HiveMetastoreTypes, HiveShim}
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

private[sql] class DefaultSource extends FileFormat with DataSourceRegister {

  override def shortName(): String = "orc"

  override def toString: String = "ORC"

  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    OrcFileOperator.readSchema(
      files.map(_.getPath.toUri.toString), Some(sqlContext.sparkContext.hadoopConfiguration))
  }

  override def prepareWrite(
      sqlContext: SQLContext,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val compressionCodec: Option[String] = options
        .get("compression")
        .map { codecName =>
          // Validate if given compression codec is supported or not.
          val shortOrcCompressionCodecNames = OrcRelation.shortOrcCompressionCodecNames
          if (!shortOrcCompressionCodecNames.contains(codecName.toLowerCase)) {
            val availableCodecs = shortOrcCompressionCodecNames.keys.map(_.toLowerCase)
            throw new IllegalArgumentException(s"Codec [$codecName] " +
                s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
          }
          codecName.toLowerCase
        }

    compressionCodec.foreach { codecName =>
      job.getConfiguration.set(
        OrcTableProperties.COMPRESSION.getPropName,
        OrcRelation
            .shortOrcCompressionCodecNames
            .getOrElse(codecName, CompressionKind.NONE).name())
    }

    job.getConfiguration match {
      case conf: JobConf =>
        conf.setOutputFormat(classOf[OrcOutputFormat])
      case conf =>
        conf.setClass(
          "mapred.output.format.class",
          classOf[OrcOutputFormat],
          classOf[MapRedOutputFormat[_, _]])
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, bucketId, dataSchema, context)
      }
    }
  }

  override def buildInternalScan(
      sqlContext: SQLContext,
      dataSchema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      bucketSet: Option[BitSet],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration],
      options: Map[String, String]): RDD[InternalRow] = {
    val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    OrcTableScan(sqlContext, output, filters, inputFiles).execute()
  }
}

private[orc] class OrcOutputWriter(
    path: String,
    bucketId: Option[Int],
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with HiveInspectors {

  private val serializer = {
    val table = new Properties()
    table.setProperty("columns", dataSchema.fieldNames.mkString(","))
    table.setProperty("columns.types", dataSchema.map { f =>
      HiveMetastoreTypes.toMetastoreType(f.dataType)
    }.mkString(":"))

    val serde = new OrcSerde
    val configuration = context.getConfiguration
    serde.initialize(configuration, table)
    serde
  }

  // Object inspector converted from the schema of the relation to be written.
  private val structOI = {
    val typeInfo =
      TypeInfoUtils.getTypeInfoFromTypeString(
        HiveMetastoreTypes.toMetastoreType(dataSchema))

    OrcStruct.createObjectInspector(typeInfo.asInstanceOf[StructTypeInfo])
      .asInstanceOf[SettableStructObjectInspector]
  }

  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter: RecordWriter[NullWritable, Writable] = {
    recordWriterInstantiated = true

    val conf = context.getConfiguration
    val uniqueWriteJobId = conf.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = context.getTaskAttemptID
    val partition = taskAttemptId.getTaskID.getId
    val bucketString = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
    val compressionExtension = {
      val name = conf.get(OrcTableProperties.COMPRESSION.getPropName)
      OrcRelation.extensionsForCompressionCodecNames.getOrElse(name, "")
    }
    // It has the `.orc` extension at the end because (de)compression tools
    // such as gunzip would not be able to decompress this as the compression
    // is not applied on this whole file but on each "stream" in ORC format.
    val filename = f"part-r-$partition%05d-$uniqueWriteJobId$bucketString$compressionExtension.orc"

    new OrcOutputFormat().getRecordWriter(
      new Path(path, filename).getFileSystem(conf),
      conf.asInstanceOf[JobConf],
      new Path(path, filename).toString,
      Reporter.NULL
    ).asInstanceOf[RecordWriter[NullWritable, Writable]]
  }

  override def write(row: Row): Unit =
    throw new UnsupportedOperationException("call writeInternal")

  private def wrapOrcStruct(
      struct: OrcStruct,
      oi: SettableStructObjectInspector,
      row: InternalRow): Unit = {
    val fieldRefs = oi.getAllStructFieldRefs
    var i = 0
    while (i < fieldRefs.size) {

      oi.setStructFieldData(
        struct,
        fieldRefs.get(i),
        wrap(
          row.get(i, dataSchema(i).dataType),
          fieldRefs.get(i).getFieldObjectInspector,
          dataSchema(i).dataType))
      i += 1
    }
  }

  val cachedOrcStruct = structOI.create().asInstanceOf[OrcStruct]

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    wrapOrcStruct(cachedOrcStruct, structOI, row)

    recordWriter.write(
      NullWritable.get(),
      serializer.serialize(cachedOrcStruct, structOI))
  }

  override def close(): Unit = {
    if (recordWriterInstantiated) {
      recordWriter.close(Reporter.NULL)
    }
  }
}

private[orc] case class OrcTableScan(
    @transient sqlContext: SQLContext,
    attributes: Seq[Attribute],
    filters: Array[Filter],
    @transient inputPaths: Array[FileStatus])
  extends Logging
  with HiveInspectors {

  private def addColumnIds(
      dataSchema: StructType,
      output: Seq[Attribute],
      conf: Configuration): Unit = {
    val ids = output.map(a => dataSchema.fieldIndex(a.name): Integer)
    val (sortedIds, sortedNames) = ids.zip(attributes.map(_.name)).sorted.unzip
    HiveShim.appendReadColumns(conf, sortedIds, sortedNames)
  }

  // Transform all given raw `Writable`s into `InternalRow`s.
  private def fillObject(
      path: String,
      conf: Configuration,
      iterator: Iterator[Writable],
      nonPartitionKeyAttrs: Seq[Attribute]): Iterator[InternalRow] = {
    val deserializer = new OrcSerde
    val maybeStructOI = OrcFileOperator.getObjectInspector(path, Some(conf))
    val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))
    val unsafeProjection = UnsafeProjection.create(StructType.fromAttributes(nonPartitionKeyAttrs))

    // SPARK-8501: ORC writes an empty schema ("struct<>") to an ORC file if the file contains zero
    // rows, and thus couldn't give a proper ObjectInspector.  In this case we just return an empty
    // partition since we know that this file is empty.
    maybeStructOI.map { soi =>
      val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.zipWithIndex.map {
        case (attr, ordinal) =>
          soi.getStructFieldRef(attr.name) -> ordinal
      }.unzip
      val unwrappers = fieldRefs.map(unwrapperFor)
      // Map each tuple to a row object
      iterator.map { value =>
        val raw = deserializer.deserialize(value)
        var i = 0
        while (i < fieldRefs.length) {
          val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        }
        unsafeProjection(mutableRow)
      }
    }.getOrElse {
      Iterator.empty
    }
  }

  def execute(): RDD[InternalRow] = {
    val job = Job.getInstance(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    // Tries to push down filters if ORC filter push-down is enabled
    if (sqlContext.conf.orcFilterPushDown) {
      OrcFilters.createFilter(filters).foreach { f =>
        conf.set(OrcTableScan.SARG_PUSHDOWN, f.toKryo)
        conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }

    // Figure out the actual schema from the ORC source (without partition columns) so that we
    // can pick the correct ordinals.  Note that this assumes that all files have the same schema.
    val orcFormat = new DefaultSource
    val dataSchema =
      orcFormat
          .inferSchema(sqlContext, Map.empty, inputPaths)
          .getOrElse(sys.error("Failed to read schema from target ORC files."))
    // Sets requested columns
    addColumnIds(dataSchema, attributes, conf)

    if (inputPaths.isEmpty) {
      // the input path probably be pruned, return an empty RDD.
      return sqlContext.sparkContext.emptyRDD[InternalRow]
    }
    FileInputFormat.setInputPaths(job, inputPaths.map(_.getPath): _*)

    val inputFormatClass =
      classOf[OrcInputFormat]
        .asInstanceOf[Class[_ <: MapRedInputFormat[NullWritable, Writable]]]

    val rdd = sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      inputFormatClass,
      classOf[NullWritable],
      classOf[Writable]
    ).asInstanceOf[HadoopRDD[NullWritable, Writable]]

    val wrappedConf = new SerializableConfiguration(conf)

    rdd.mapPartitionsWithInputSplit { case (split: OrcSplit, iterator) =>
      val writableIterator = iterator.map(_._2)
      fillObject(split.getPath.toString, wrappedConf.value, writableIterator, attributes)
    }
  }
}

private[orc] object OrcTableScan {
  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"
}

private[orc] object OrcRelation {
  // The ORC compression short names
  val shortOrcCompressionCodecNames = Map(
    "none" -> CompressionKind.NONE,
    "uncompressed" -> CompressionKind.NONE,
    "snappy" -> CompressionKind.SNAPPY,
    "zlib" -> CompressionKind.ZLIB,
    "lzo" -> CompressionKind.LZO)

  // The extensions for ORC compression codecs
  val extensionsForCompressionCodecNames = Map(
    CompressionKind.NONE.name -> "",
    CompressionKind.SNAPPY.name -> ".snappy",
    CompressionKind.ZLIB.name -> ".zlib",
    CompressionKind.LZO.name -> ".lzo"
  )
}

