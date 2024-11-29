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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.serde2.objectinspector
import org.apache.hadoop.hive.serde2.objectinspector.{SettableStructObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.orc.OrcConf
import org.apache.orc.OrcConf.COMPRESS

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OrcFilters, OrcOptions, OrcUtils}
import org.apache.spark.sql.hive.{HiveInspectors, HiveShim}
import org.apache.spark.sql.sources._
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
    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)
    if (orcOptions.mergeSchema) {
      SchemaMergeUtils.mergeSchemasInParallel(
        sparkSession, options, files, OrcFileOperator.readOrcSchemasInParallel)
    } else {
      OrcFileOperator.readSchema(
        files.map(_.getPath.toString),
        Some(sparkSession.sessionState.newHadoopConfWithOptions(options)),
        orcOptions.ignoreCorruptFiles
      )
    }
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
          OrcUtils.extensionsForCompressionCodecNames.getOrElse(name, "")
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
      OrcFilters.createFilter(requiredSchema, filters).foreach { f =>
        hadoopConf.set(OrcFileFormat.SARG_PUSHDOWN, toKryo(f))
        hadoopConf.setBoolean("hive.optimize.index.filter", true)
      }
    }

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val ignoreCorruptFiles =
      new OrcOptions(options, sparkSession.sessionState.conf).ignoreCorruptFiles

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value

      val filePath = file.toPath

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
          FileInputFormat.setInputPaths(job, file.urlEncodedPath)

          // Custom OrcRecordReader is used to get
          // ObjectInspector during recordReader creation itself and can
          // avoid NameNode call in unwrapOrcStructs per file.
          // Specifically would be helpful for partitioned datasets.
          val orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf))
          new SparkOrcNewRecordReader(orcReader, conf, file.start, file.length)
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

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: VariantType => false

    case _: AnsiIntervalType => false

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }

  // HIVE-11253 moved `toKryo` from `SearchArgument` to `storage-api` module.
  // This is copied from Hive 1.2's SearchArgumentImpl.toKryo().
  private def toKryo(sarg: SearchArgument): String = {
    val kryo = new Kryo()
    val out = new Output(4 * 1024, 10 * 1024 * 1024)
    kryo.writeObject(out, sarg)
    out.close()
    Base64.encodeBase64String(out.toBytes)
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
  val structOI = {
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
    val path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private[this] val serializer = new OrcSerializer(dataSchema, context.getConfiguration)

  private val recordWriter: RecordWriter[NullWritable, Writable] = {
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
    try {
      OrcUtils.addSparkVersionMetadata(getOrCreateInternalWriter())
    } catch {
      case NonFatal(e) => log.warn(e.toString, e)
    }
    recordWriter.close(Reporter.NULL)
  }

  private def getOrCreateInternalWriter(): Writer = {
    val writerField = recordWriter.getClass.getDeclaredField("writer")
    writerField.setAccessible(true)
    var writer = writerField.get(recordWriter).asInstanceOf[Writer]
    if (writer == null) {
      // Hive ORC initializes its private `writer` field at the first write.
      // For empty write task, we need to create it manually to record our meta.
      val options = OrcFile.writerOptions(context.getConfiguration)
      options.inspector(serializer.structOI)
      writer = OrcFile.createWriter(new Path(path), options)
      // set the writer to make it flush meta on close
      writerField.set(recordWriter, writer)
    }
    writer
  }
}

private[orc] object OrcFileFormat extends HiveInspectors with Logging {
  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"

  def unwrapOrcStructs(
      conf: Configuration,
      dataSchema: StructType,
      requiredSchema: StructType,
      maybeStructOI: Option[StructObjectInspector],
      iterator: Iterator[Writable]): Iterator[InternalRow] = {
    val deserializer = new OrcSerde
    val mutableRow = new SpecificInternalRow(requiredSchema.map(_.dataType))
    val unsafeProjection = UnsafeProjection.create(requiredSchema)
    val forcePositionalEvolution = OrcConf.FORCE_POSITIONAL_EVOLUTION.getBoolean(conf)

    def unwrap(oi: StructObjectInspector): Iterator[InternalRow] = {
      val (fieldRefs, fieldOrdinals) = requiredSchema.zipWithIndex.map {
        case (field, ordinal) =>
          var ref: objectinspector.StructField = null
          if (forcePositionalEvolution) {
            ref = oi.getAllStructFieldRefs.get(dataSchema.fieldIndex(field.name))
          } else {
            ref = oi.getStructFieldRef(field.name)
            if (ref == null) {
              ref = oi.getStructFieldRef("_col" + dataSchema.fieldIndex(field.name))
            }
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
}
