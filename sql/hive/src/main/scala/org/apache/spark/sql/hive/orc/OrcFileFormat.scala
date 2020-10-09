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

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.serde2.objectinspector.{SettableStructObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.orc.OrcConf.COMPRESS

import org.apache.spark.{SPARK_VERSION_SHORT, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SPARK_VERSION_METADATA_KEY
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.OrcOptions
import org.apache.spark.sql.hive.{HiveInspectors, HiveShim}
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * `FileFormat` for reading ORC files. If this is moved or renamed, please update
 * `DataSource`'s backwardCompatibilityMap.
 */
class OrcFileFormat extends FileFormat with DataSourceRegister with Serializable {

  override def shortName(): String = "orc"

  override def toString: String = "ORC"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
    OrcFileOperator.readSchema(
      files.map(_.getPath.toString),
      Some(sparkSession.sessionState.newHadoopConfWithOptions(options)),
      ignoreCorruptFiles
    )
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)

    val configuration = job.getConfiguration

    configuration.set(COMPRESS.getAttribute, orcOptions.compressionCodec)
    configuration match {
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
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
          OrcFileFormat.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    if (sparkSession.sessionState.conf.orcFilterPushDown) {
      // Sets pushed predicates
      OrcFilters.createFilter(requiredSchema, filters.toArray).foreach { f =>
        hadoopConf.set(OrcFileFormat.SARG_PUSHDOWN, f.toKryo)
        hadoopConf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value

      val filePath = new Path(new URI(file.filePath))

      // SPARK-8501: Empty ORC files always have an empty schema stored in their footer. In this
      // case, `OrcFileOperator.readSchema` returns `None`, and we can't read the underlying file
      // using the given physical schema. Instead, we simply return an empty iterator.
      val isEmptyFile =
        OrcFileOperator.readSchema(Seq(filePath.toString), Some(conf), ignoreCorruptFiles).isEmpty
      if (isEmptyFile) {
        Iterator.empty
      } else {
        OrcFileFormat.setRequiredColumns(conf, dataSchema, requiredSchema)

        val orcRecordReader = {
          val job = Job.getInstance(conf)
          FileInputFormat.setInputPaths(job, file.filePath)

          val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
          // Custom OrcRecordReader is used to get
          // ObjectInspector during recordReader creation itself and can
          // avoid NameNode call in unwrapOrcStructs per file.
          // Specifically would be helpful for partitioned datasets.
          val orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf))
          new SparkOrcNewRecordReader(orcReader, conf, fileSplit.getStart, fileSplit.getLength)
        }

        val recordsIterator = new RecordReaderIterator[OrcStruct](orcRecordReader)
        Option(TaskContext.get())
          .foreach(_.addTaskCompletionListener[Unit](_ => recordsIterator.close()))

        // Unwraps `OrcStruct`s to `UnsafeRow`s
        OrcFileFormat.unwrapOrcStructs(
          conf,
          dataSchema,
          requiredSchema,
          Some(orcRecordReader.getObjectInspector.asInstanceOf[StructObjectInspector]),
          recordsIterator)
      }
    }
  }

  override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType, isReadPath) }

    case ArrayType(elementType, _) => supportDataType(elementType, isReadPath)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType, isReadPath) && supportDataType(valueType, isReadPath)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType, isReadPath)

    case _: NullType => isReadPath

    case _ => false
  }
}

private[orc] class OrcSerializer(dataSchema: StructType, conf: Configuration)
  extends HiveInspectors {

  def serialize(row: InternalRow): Writable = {
    wrapOrcStruct(cachedOrcStruct, structOI, row)
    serializer.serialize(cachedOrcStruct, structOI)
  }

  private[this] val serializer = {
    val table = new Properties()
    table.setProperty("columns", dataSchema.fieldNames.mkString(","))
    table.setProperty("columns.types", dataSchema.map(_.dataType.catalogString).mkString(":"))

    val serde = new OrcSerde
    serde.initialize(conf, table)
    serde
  }

  // Object inspector converted from the schema of the relation to be serialized.
  private[this] val structOI = {
    val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(dataSchema.catalogString)
    OrcStruct.createObjectInspector(typeInfo.asInstanceOf[StructTypeInfo])
      .asInstanceOf[SettableStructObjectInspector]
  }

  private[this] val cachedOrcStruct = structOI.create().asInstanceOf[OrcStruct]

  // Wrapper functions used to wrap Spark SQL input arguments into Hive specific format
  private[this] val wrappers = dataSchema.zip(structOI.getAllStructFieldRefs().asScala.toSeq).map {
    case (f, i) => wrapperFor(i.getFieldObjectInspector, f.dataType)
  }

  private[this] def wrapOrcStruct(
      struct: OrcStruct,
      oi: SettableStructObjectInspector,
      row: InternalRow): Unit = {
    val fieldRefs = oi.getAllStructFieldRefs
    var i = 0
    val size = fieldRefs.size
    while (i < size) {

      oi.setStructFieldData(
        struct,
        fieldRefs.get(i),
        wrappers(i)(row.get(i, dataSchema(i).dataType))
      )
      i += 1
    }
  }
}

private[orc] class OrcOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter {

  private[this] val serializer = new OrcSerializer(dataSchema, context.getConfiguration)

  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter: RecordWriter[NullWritable, Writable] = {
    recordWriterInstantiated = true
    new OrcOutputFormat().getRecordWriter(
      new Path(path).getFileSystem(context.getConfiguration),
      context.getConfiguration.asInstanceOf[JobConf],
      path,
      Reporter.NULL
    ).asInstanceOf[RecordWriter[NullWritable, Writable]]
  }

  override def write(row: InternalRow): Unit = {
    recordWriter.write(NullWritable.get(), serializer.serialize(row))
  }

  override def close(): Unit = {
    if (recordWriterInstantiated) {
      // Hive 1.2.1 ORC initializes its private `writer` field at the first write.
      OrcFileFormat.addSparkVersionMetadata(recordWriter)
      recordWriter.close(Reporter.NULL)
    }
  }
}

private[orc] object OrcFileFormat extends HiveInspectors with Logging {
  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"

  // The extensions for ORC compression codecs
  val extensionsForCompressionCodecNames = Map(
    "NONE" -> "",
    "SNAPPY" -> ".snappy",
    "ZLIB" -> ".zlib",
    "LZO" -> ".lzo")

  def unwrapOrcStructs(
      conf: Configuration,
      dataSchema: StructType,
      requiredSchema: StructType,
      maybeStructOI: Option[StructObjectInspector],
      iterator: Iterator[Writable]): Iterator[InternalRow] = {
    val deserializer = new OrcSerde
    val mutableRow = new SpecificInternalRow(requiredSchema.map(_.dataType))
    val unsafeProjection = UnsafeProjection.create(requiredSchema)

    def unwrap(oi: StructObjectInspector): Iterator[InternalRow] = {
      val (fieldRefs, fieldOrdinals) = requiredSchema.zipWithIndex.map {
        case (field, ordinal) =>
          var ref = oi.getStructFieldRef(field.name)
          if (ref == null) {
            ref = oi.getStructFieldRef("_col" + dataSchema.fieldIndex(field.name))
          }
          ref -> ordinal
      }.unzip

      val unwrappers = fieldRefs.map(r => if (r == null) null else unwrapperFor(r))

      iterator.map { value =>
        val raw = deserializer.deserialize(value)
        var i = 0
        val length = fieldRefs.length
        while (i < length) {
          val fieldRef = fieldRefs(i)
          val fieldValue = if (fieldRef == null) null else oi.getStructFieldData(raw, fieldRef)
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        }
        unsafeProjection(mutableRow)
      }
    }

    maybeStructOI.map(unwrap).getOrElse(Iterator.empty)
  }

  def setRequiredColumns(
      conf: Configuration, dataSchema: StructType, requestedSchema: StructType): Unit = {
    val ids = requestedSchema.map(a => dataSchema.fieldIndex(a.name): Integer)
    val (sortedIDs, sortedNames) = ids.zip(requestedSchema.fieldNames).sorted.unzip
    HiveShim.appendReadColumns(conf, sortedIDs, sortedNames)
  }

  /**
   * Add a metadata specifying Spark version.
   */
  def addSparkVersionMetadata(recordWriter: RecordWriter[NullWritable, Writable]): Unit = {
    try {
      val writerField = recordWriter.getClass.getDeclaredField("writer")
      writerField.setAccessible(true)
      val writer = writerField.get(recordWriter).asInstanceOf[Writer]
      writer.addUserMetadata(SPARK_VERSION_METADATA_KEY, UTF_8.encode(SPARK_VERSION_SHORT))
    } catch {
      case NonFatal(e) => log.warn(e.toString, e)
    }
  }
}
