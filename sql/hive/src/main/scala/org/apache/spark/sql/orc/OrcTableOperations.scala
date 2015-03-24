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
import java.text.SimpleDateFormat
import java.util._
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, CompressionKind, OrcInputFormat, OrcSerde, OrcOutputFormat}
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfoUtils, TypeInfo}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{Reporter, JobConf}
import org.apache.hadoop.mapreduce.{Job, TaskID}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, FileOutputCommitter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode => LogicalUnaryNode, LogicalPlan}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{LeafNode, UnaryNode, SparkPlan}
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql.hive.{HadoopTypeConverter, HiveShim, HiveMetastoreTypes}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, TaskContext, SerializableWritable}
import scala.collection.JavaConversions._

case class InsertIntoOrcTable(relation: OrcRelation,
                              data: DataFrame,
                              overwrite: Boolean = false)
  extends SparkHadoopMapReduceUtil with Logging {

  @transient val sqlContext = relation.sqlContext

  def output = data.schema.toAttributes

  // Inserts all rows into the Orc file.
  def execute() = {
    val childRdd = data.rdd
    assert(childRdd != null)
    val structType =  StructType.fromAttributes(relation.output)
    val orcSchema = HiveMetastoreTypes.toMetastoreType(structType)

    val rdd = childRdd.mapPartitions { iter =>
      val serializer = new OrcSerde
      val typeInfo: TypeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(orcSchema)
      val standardOI = TypeInfoUtils
        .getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
        .asInstanceOf[StructObjectInspector]
      val fieldOIs = standardOI
        .getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      val wrappers = fieldOIs.map(HadoopTypeConverter.wrappers)
      val outputData = new Array[Any](fieldOIs.length)
      iter.map { row =>
        var i = 0
        while (i < row.length) {
          outputData(i) = wrappers(i)(row(i))
          i += 1
        }
        serializer.serialize(outputData, standardOI)
      }
    }

    @transient val job = new Job(sqlContext.sparkContext.hadoopConfiguration)

    val conf = job.getConfiguration
    if (overwrite) {
      OrcFileOperator.deletePath(relation.path, conf)
    }
    conf.set("mapred.output.dir", relation.path)
    saveAsHadoopFile(rdd: RDD[Writable], conf)
    childRdd
  }

  def saveAsHadoopFile(rdd: RDD[Writable], conf: Configuration) {
    //val job = new Job(conf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()
    val wrappedConf = new SerializableWritable(conf)
    val outfmt = classOf[OrcOutputFormat]
    val jobFormat = outfmt.newInstance
    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val output: Path = FileOutputFormat.getOutputPath(jobTaskContext)

    def writeShard(context: TaskContext,
                   iter: scala.collection.Iterator[Writable]): Int = {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      // "reduce task" <split #> <attempt # = spark task #>
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false,
        context.partitionId, attemptNumber)
      val hadoopTaskContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = outfmt.newInstance
      val taskId: TaskID = hadoopTaskContext.getTaskAttemptID.getTaskID
      val partition: Int = taskId.getId
      val filename = s"part-r-${partition}-${System.currentTimeMillis}.orc"
      val output: Path = FileOutputFormat.getOutputPath(hadoopTaskContext)
      val committer = new FileOutputCommitter(output, hadoopTaskContext)
      val path = new Path(committer.getWorkPath, filename)
      // avoid create empty file without schema attached
      if (iter.hasNext) {
        val fs = output.getFileSystem(wrappedConf.value)
        val writer = format.getRecordWriter(fs,
          wrappedConf.value.asInstanceOf[JobConf],
          path.toUri.getPath, Reporter.NULL)
          .asInstanceOf[org.apache.hadoop.mapred.RecordWriter[NullWritable, Writable]]
        try {
          while (iter.hasNext) {
            val row = iter.next()
            writer.write(NullWritable.get(), row)
          }
        } finally {
          writer.close(Reporter.NULL)
        }
        committer.commitTask(hadoopTaskContext)
        1
      } else {
        0
      }
      1
    }
    val jobCommitter = new FileOutputCommitter(output, jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    sqlContext.sparkContext.runJob(rdd, writeShard _)
    jobCommitter.commitJob(jobTaskContext)
  }
}

@DeveloperApi
case class OrcTableScan(attributes: Seq[Attribute],
                        relation: OrcRelation,
                        predicates: Seq[Expression]) extends Logging{

  val output = attributes
  @transient val sqlContext = relation.sqlContext

  def addColumnIds(output: Seq[Attribute],
                   relation: OrcRelation, conf: Configuration) {
    val ids =
      output.map(a =>
        relation.output.indexWhere(_.name == a.name): Integer)
        .filter(index => index >= 0)
    val names = attributes.map(_.name)
    val sorted = ids.zip(names).sorted
    HiveShim.appendReadColumns(conf, sorted.map(_._1), sorted.map(_._2))
  }

  def buildFilter(job: Job):  Seq[Expression] => Seq[Expression] = {
    if (ORC_FILTER_PUSHDOWN_ENABLED) {
      val conf: Configuration = job.getConfiguration
      (filters: Seq[Expression]) => {
        val recordFilter = OrcFilters.createFilter(filters)
        if (recordFilter.isDefined) {
          /**
           * To test it, we can set follows so that the reader
           * will not read whole file if small
           * sparkContext.hadoopConfiguration.setInt(
           * "mapreduce.input.fileinputformat.split.maxsize", 50)
           */
          conf.set(SARG_PUSHDOWN, toKryo(recordFilter.get))
          conf.setBoolean("hive.optimize.index.filter", true)
        }
        filters
      }
    } else {
      identity[Seq[Expression]] _
    }
  }

  // Transform all given raw `Writable`s into `Row`s.
  def fillObject(conf: Configuration,
                 iterator: scala.collection.Iterator[Writable],
                 nonPartitionKeyAttrs: Seq[(Attribute, Int)],
                 mutableRow: MutableRow): Iterator[Row] = {
    val schema =  StructType.fromAttributes(relation.output)
    val orcSchema = HiveMetastoreTypes.toMetastoreType(schema)
    val deserializer = new OrcSerde
    val typeInfo: TypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(orcSchema)
    val soi = OrcFileOperator.getObjectInspector(relation.path, Some(conf))
    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map {
      case (attr, ordinal) =>
        soi.getStructFieldRef(attr.name.toLowerCase) -> ordinal
    }.unzip
    val unwrappers = HadoopTypeConverter.unwrappers(fieldRefs)
    logDebug("Converting raw data to row")
    // Map each tuple to a row object
    iterator.map { value =>
      val raw = deserializer.deserialize(value)
      logDebug("Raw data: " + raw)
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
      logDebug("Mutable row: " + mutableRow)
      mutableRow: Row
    }
  }

  def execute(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)
    buildFilter(job)(predicates)
    val conf: Configuration = job.getConfiguration
    val fileList = OrcFileOperator.listOrcFiles(relation.path, conf)
    addColumnIds(output, relation, conf)
    for (path <- fileList if !path.getName.startsWith("_")) {
      FileInputFormat.addInputPath(job, path)
    }

    val inputClass = classOf[OrcInputFormat].asInstanceOf[
      Class[_ <: org.apache.hadoop.mapred.InputFormat[NullWritable, Writable]]]

    val rdd = sc.hadoopRDD(conf.asInstanceOf[JobConf],
      inputClass, classOf[NullWritable], classOf[Writable]).map(_._2)
    // orc optimize too much even the getPartition part, in multiple query, only the
    // partition not trimmed is visible in the first query

    val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))
    val wrappedConf = new SerializableWritable(conf)
    val rowRdd: RDD[Row] = rdd.mapPartitions { iter =>
        fillObject(wrappedConf.value, iter, attributes.zipWithIndex, mutableRow)
    }
      rowRdd
  }
}
