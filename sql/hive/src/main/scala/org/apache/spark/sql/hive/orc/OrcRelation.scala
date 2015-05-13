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

import java.util.Objects

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcSerde, OrcOutputFormat}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfoUtils, TypeInfo}
import org.apache.hadoop.io.{Writable, NullWritable}
import org.apache.hadoop.mapred.{RecordWriter, Reporter, JobConf}
import org.apache.hadoop.mapreduce.{TaskID, TaskAttemptContext}
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import scala.collection.JavaConversions._


private[sql] class DefaultSource extends FSBasedRelationProvider {

  def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): FSBasedRelation ={
    val partitionSpec = partitionColumns.map(PartitionSpec(_, Seq.empty[Partition]))
    OrcRelation(paths, parameters,
      schema, partitionSpec)(sqlContext)
  }
}


private[sql] class OrcOutputWriter extends OutputWriter with SparkHadoopMapRedUtil {
  var recordWriter: RecordWriter[NullWritable, Writable] = _
  var taskAttemptContext: TaskAttemptContext = _
  var serializer: OrcSerde = _
  var wrappers: Array[Any => Any] = _
  var created = false
  var path: String = _
  var dataSchema: StructType = _
  var fieldOIs: Array[ObjectInspector] = _
  var standardOI: StructObjectInspector = _


  override def init(path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): Unit = {
    this.path = path
    this.dataSchema = dataSchema
    taskAttemptContext = context
  }

  // Avoid create empty file without schema attached
  private def initWriter() = {
    if (!created) {
      created = true
      val conf = taskAttemptContext.getConfiguration
      val outputFormat = new OrcOutputFormat()
      val taskId: TaskID = taskAttemptContext.getTaskAttemptID.getTaskID
      val partition: Int = taskId.getId
      val filename = s"part-r-${partition}-${System.currentTimeMillis}.orc"
      val file = new Path(path, filename)
      val fs = file.getFileSystem(conf)
      val orcSchema = HiveMetastoreTypes.toMetastoreType(dataSchema)

      serializer = new OrcSerde
      val typeInfo: TypeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(orcSchema)
      standardOI = TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
        .asInstanceOf[StructObjectInspector]
      fieldOIs = standardOI
        .getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      wrappers = fieldOIs.map(HadoopTypeConverter.wrappers)
      recordWriter = {
        outputFormat.getRecordWriter(fs,
          conf.asInstanceOf[JobConf],
          file.toUri.getPath, Reporter.NULL)
          .asInstanceOf[org.apache.hadoop.mapred.RecordWriter[NullWritable, Writable]]
      }
    }
  }
  override def write(row: Row): Unit = {
    initWriter()
    var i = 0
    val outputData = new Array[Any](fieldOIs.length)
    while (i < row.length) {
      outputData(i) = wrappers(i)(row(i))
      i += 1
    }
    val writable = serializer.serialize(outputData, standardOI)
    recordWriter.write(NullWritable.get(), writable)
  }

  override def close(): Unit = {
    if (recordWriter != null) {
      recordWriter.close(Reporter.NULL)
    }
  }
}


@DeveloperApi
private[sql] case class OrcRelation(override val paths: Array[String],
    parameters: Map[String, String],
    maybeSchema: Option[StructType] = None,
    maybePartitionSpec: Option[PartitionSpec] = None)(
    @transient val sqlContext: SQLContext)
  extends FSBasedRelation(paths, maybePartitionSpec)
  with Logging {
  self: Product =>
  @transient val conf = sqlContext.sparkContext.hadoopConfiguration


  override def dataSchema: StructType =
    maybeSchema.getOrElse(OrcFileOperator.readSchema(paths(0), Some(conf)))

  override def outputWriterClass: Class[_ <: OutputWriter] = classOf[OrcOutputWriter]
  /** Attributes */
  var output: Seq[Attribute] = schema.toAttributes

  override def needConversion: Boolean = false

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
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
      maybePartitionSpec)
  }
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String]): RDD[Row] = {
    val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    OrcTableScan(output, this, filters, inputPaths).execute()
  }
}

private[sql] object OrcRelation extends Logging {
  // Default partition name to use when the partition column value is null or empty string.
  val DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__"
}
