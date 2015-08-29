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

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcOutputFormat, OrcSerde, OrcSplit}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{InputFormat => MapRedInputFormat, JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors, HiveMetastoreTypes, HiveShim}
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration

/* Implicit conversions */
import scala.collection.JavaConversions._

private[sql] class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "orc"

  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    assert(
      sqlContext.isInstanceOf[HiveContext],
      "The ORC data source can only be used with HiveContext.")

    new OrcRelation(paths, dataSchema, None, partitionColumns, parameters)(sqlContext)
  }
}

private[orc] class OrcOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with SparkHadoopMapRedUtil with HiveInspectors {

  private val serializer = {
    val table = new Properties()
    table.setProperty("columns", dataSchema.fieldNames.mkString(","))
    table.setProperty("columns.types", dataSchema.map { f =>
      HiveMetastoreTypes.toMetastoreType(f.dataType)
    }.mkString(":"))

    val serde = new OrcSerde
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    serde.initialize(configuration, table)
    serde
  }

  // Object inspector converted from the schema of the relation to be written.
  private val structOI = {
    val typeInfo =
      TypeInfoUtils.getTypeInfoFromTypeString(
        HiveMetastoreTypes.toMetastoreType(dataSchema))

    TypeInfoUtils
      .getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
      .asInstanceOf[StructObjectInspector]
  }

  // Used to hold temporary `Writable` fields of the next row to be written.
  private val reusableOutputBuffer = new Array[Any](dataSchema.length)

  // Used to convert Catalyst values into Hadoop `Writable`s.
  private val wrappers = structOI.getAllStructFieldRefs.zip(dataSchema.fields.map(_.dataType))
    .map { case (ref, dt) =>
      wrapperFor(ref.getFieldObjectInspector, dt)
    }.toArray

  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter: RecordWriter[NullWritable, Writable] = {
    recordWriterInstantiated = true

    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    val uniqueWriteJobId = conf.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
    val partition = taskAttemptId.getTaskID.getId
    val filename = f"part-r-$partition%05d-$uniqueWriteJobId.orc"

    new OrcOutputFormat().getRecordWriter(
      new Path(path, filename).getFileSystem(conf),
      conf.asInstanceOf[JobConf],
      new Path(path, filename).toString,
      Reporter.NULL
    ).asInstanceOf[RecordWriter[NullWritable, Writable]]
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    var i = 0
    while (i < row.numFields) {
      reusableOutputBuffer(i) = wrappers(i)(row.get(i, dataSchema(i).dataType))
      i += 1
    }

    recordWriter.write(
      NullWritable.get(),
      serializer.serialize(reusableOutputBuffer, structOI))
  }

  override def close(): Unit = {
    if (recordWriterInstantiated) {
      recordWriter.close(Reporter.NULL)
    }
  }
}

private[sql] class OrcRelation(
    override val paths: Array[String],
    maybeDataSchema: Option[StructType],
    maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    @transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec)
  with Logging {

  private[sql] def this(
      paths: Array[String],
      maybeDataSchema: Option[StructType],
      maybePartitionSpec: Option[PartitionSpec],
      parameters: Map[String, String])(
      sqlContext: SQLContext) = {
    this(
      paths,
      maybeDataSchema,
      maybePartitionSpec,
      maybePartitionSpec.map(_.partitionColumns),
      parameters)(sqlContext)
  }

  override val dataSchema: StructType = maybeDataSchema.getOrElse {
    OrcFileOperator.readSchema(
      paths.head, Some(sqlContext.sparkContext.hadoopConfiguration))
  }

  override def needConversion: Boolean = false

  override def equals(other: Any): Boolean = other match {
    case that: OrcRelation =>
      paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema &&
        partitionColumns == that.partitionColumns
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus]): RDD[Row] = {
    val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    OrcTableScan(output, this, filters, inputPaths).execute().asInstanceOf[RDD[Row]]
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
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
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }
    }
  }
}

private[orc] case class OrcTableScan(
    attributes: Seq[Attribute],
    @transient relation: OrcRelation,
    filters: Array[Filter],
    @transient inputPaths: Array[FileStatus])
  extends Logging
  with HiveInspectors {

  @transient private val sqlContext = relation.sqlContext

  private def addColumnIds(
      output: Seq[Attribute],
      relation: OrcRelation,
      conf: Configuration): Unit = {
    val ids = output.map(a => relation.dataSchema.fieldIndex(a.name): Integer)
    val (sortedIds, sortedNames) = ids.zip(attributes.map(_.name)).sorted.unzip
    HiveShim.appendReadColumns(conf, sortedIds, sortedNames)
  }

  // Transform all given raw `Writable`s into `InternalRow`s.
  private def fillObject(
      path: String,
      conf: Configuration,
      iterator: Iterator[Writable],
      nonPartitionKeyAttrs: Seq[(Attribute, Int)],
      mutableRow: MutableRow): Iterator[InternalRow] = {
    val deserializer = new OrcSerde
    val maybeStructOI = OrcFileOperator.getObjectInspector(path, Some(conf))

    // SPARK-8501: ORC writes an empty schema ("struct<>") to an ORC file if the file contains zero
    // rows, and thus couldn't give a proper ObjectInspector.  In this case we just return an empty
    // partition since we know that this file is empty.
    maybeStructOI.map { soi =>
      val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map {
        case (attr, ordinal) =>
          soi.getStructFieldRef(attr.name.toLowerCase) -> ordinal
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
        mutableRow: InternalRow
      }
    }.getOrElse {
      Iterator.empty
    }
  }

  def execute(): RDD[InternalRow] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    // Tries to push down filters if ORC filter push-down is enabled
    if (sqlContext.conf.orcFilterPushDown) {
      OrcFilters.createFilter(filters).foreach { f =>
        conf.set(OrcTableScan.SARG_PUSHDOWN, f.toKryo)
        conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }

    // Sets requested columns
    addColumnIds(attributes, relation, conf)

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
      val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))
      fillObject(
        split.getPath.toString,
        wrappedConf.value,
        iterator.map(_._2),
        attributes.zipWithIndex,
        mutableRow)
    }
  }
}

private[orc] object OrcTableScan {
  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"
}
