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
package org.apache.spark.sql.hbase.execution

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{Job, RecordWriter}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hbase.HBasePartitioner.HBaseRawOrdering
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.util.{HBaseKVHelper, Util}
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, SerializableWritable, SparkEnv, TaskContext}

import scala.collection.mutable.ArrayBuffer

@DeveloperApi
case class AlterDropColCommand(tableName: String, columnName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val context = sqlContext.asInstanceOf[HBaseSQLContext]
    context.catalog.alterTableDropNonKey(tableName, columnName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case class AlterAddColCommand(tableName: String,
                              colName: String,
                              colType: String,
                              colFamily: String,
                              colQualifier: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val context = sqlContext.asInstanceOf[HBaseSQLContext]
    context.catalog.alterTableAddNonKey(tableName,
      NonKeyColumn(
        colName, context.catalog.getDataType(colType), colFamily, colQualifier)
    )
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case class DropHbaseTableCommand(tableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val context = sqlContext.asInstanceOf[HBaseSQLContext]
    context.catalog.deleteTable(tableName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case object ShowTablesCommand extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val context = sqlContext.asInstanceOf[HBaseSQLContext]
    val buffer = new ArrayBuffer[Row]()
    val tables = context.catalog.getAllTableName
    tables.foreach(x => buffer.append(Row(x)))
    buffer.toSeq
  }

  override def output: Seq[Attribute] = StructType(Seq(StructField("", StringType))).toAttributes
}

@DeveloperApi
case class DescribeTableCommand(tableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val context = sqlContext.asInstanceOf[HBaseSQLContext]
    val buffer = new ArrayBuffer[Row]()
    val relation = context.catalog.getTable(tableName)
    if (relation.isDefined) {
      relation.get.allColumns.foreach {
        case keyColumn: KeyColumn =>
          buffer.append(Row(keyColumn.sqlName, keyColumn.dataType.toString,
            "KEY COLUMN", keyColumn.order.toString))
        case nonKeyColumn: NonKeyColumn =>
          buffer.append(Row(nonKeyColumn.sqlName, nonKeyColumn.dataType.toString,
            "NON KEY COLUMN", nonKeyColumn.family, nonKeyColumn.qualifier))
      }
      buffer.toSeq
    } else {
      sys.error(s"can not find table $tableName")
    }
  }

  override def output: Seq[Attribute] =
    StructType(Seq.fill(5)(StructField("", StringType))).toAttributes
}

@DeveloperApi
case class InsertValueIntoTableCommand(tableName: String, valueSeq: Seq[String])
  extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    val keyBytes = new Array[(Array[Byte], DataType)](relation.keyColumns.size)
    val valueBytes = new Array[HBaseRawType](relation.nonKeyColumns.size)
    val lineBuffer = HBaseKVHelper.createLineBuffer(relation.output)
    HBaseKVHelper.string2KV(valueSeq, relation, lineBuffer, keyBytes, valueBytes)
    val rowKey = HBaseKVHelper.encodingRawKeyColumns(keyBytes)
    val put = new Put(rowKey)
    for (i <- 0 until valueBytes.length) {
      val value = valueBytes(i)
      if (value != null) {
        val nkc = relation.nonKeyColumns(i)
        put.add(nkc.familyRaw, nkc.qualifierRaw, value)
      }
    }
    relation.htable.put(put)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case class BulkLoadIntoTableCommand(
    path: String,
    tableName: String,
    isLocal: Boolean,
    delimiter: Option[String]) extends RunnableCommand with SparkHadoopMapReduceUtil with Logging {

  override def run(sqlContext: SQLContext) = {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    val hbContext = sqlContext.asInstanceOf[HBaseSQLContext]

    // tmp path for storing HFile
    val tmpPath = Util.getTempFilePath(hbContext.configuration, relation.tableName)
    val job = new Job(hbContext.configuration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", tmpPath)

    val conf = job.getConfiguration

    val hadoopReader = if (isLocal) {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(path)).getCanonicalPath
      new HadoopReader(hbContext.sparkContext, pathString, delimiter)(relation)
    } else {
      new HadoopReader(hbContext.sparkContext, path, delimiter)(relation)
    }

    val splitKeys = relation.getRegionStartKeys.toArray
    val wrappedConf = new SerializableWritable(conf)

    val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    val partitioner = new HBasePartitioner(splitKeys)
    val ordering = Ordering[HBaseRawType]
    val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)

    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = shuffled.id
    val jobFormat = new HFileOutputFormat2

    if (SparkEnv.get.conf.getBoolean("spark.hadoop.validateOutputSpecs", defaultValue = true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard =
      (context: TaskContext, iter: Iterator[(HBaseRawType, Array[HBaseRawType])]) => {
        val config = wrappedConf.value
        /* "reduce task" <split #> <attempt # = spark task #> */
        val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
          context.attemptNumber)
        val hadoopContext = newTaskAttemptContext(config, attemptId)
        val format = new HFileOutputFormat2
        format match {
          case c: Configurable => c.setConf(config)
          case _ => ()
        }
        val committer = format.getOutputCommitter(hadoopContext).asInstanceOf[FileOutputCommitter]
        committer.setupTask(hadoopContext)

        val writer = format.getRecordWriter(hadoopContext).
          asInstanceOf[RecordWriter[ImmutableBytesWritable, KeyValue]]
        val bytesWritable = new ImmutableBytesWritable
        var recordsWritten = 0L
        var kv: (HBaseRawType, Array[HBaseRawType]) = null
        var prevK: HBaseRawType = null
        try {
          while (iter.hasNext) {
            kv = iter.next()
            if (prevK != null && Bytes.compareTo(kv._1, prevK) == 0) {
              // force flush because we cannot guarantee intra-row ordering
              writer.write(null, null)
            }

            for (i <- 0 until kv._2.size) {
              val nkc = relation.nonKeyColumns(i)
              if (kv._2(i) != null) {
                bytesWritable.set(kv._1)
                writer.write(bytesWritable, new KeyValue(kv._1, nkc.familyRaw,
                  nkc.qualifierRaw, kv._2(i)))
              }
            }
            recordsWritten += 1

            prevK = kv._1
          }
        } finally {
          writer.close(hadoopContext)
        }

        committer.commitTask(hadoopContext)
        recordsWritten
      }: Long

    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    logDebug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")
    hbContext.sparkContext.runJob(shuffled, writeShard)
    logDebug(s"finished BulkLoad : ${System.currentTimeMillis()}")
    jobCommitter.commitJob(jobTaskContext)
    val tablePath = new Path(tmpPath)
    val load = new LoadIncrementalHFiles(conf)
    logDebug(s"finish BulkLoad on table ${relation.htable.getName}: ${System.currentTimeMillis()}")
    load.doBulkLoad(tablePath, relation.htable)
    Seq.empty[Row]
  }

  override def output = Nil
}

@DeveloperApi
case class ParallelizedBulkLoadIntoTableCommand(
    inputPath: String,
    tableName: String,
    isLocal: Boolean,
    delimiter: Option[String]) extends RunnableCommand with SparkHadoopMapReduceUtil with Logging {
  override def run(sqlContext: SQLContext) = {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    val hbContext = sqlContext.asInstanceOf[HBaseSQLContext]

    // tmp path for storing HFile
    val tmpPath = Util.getTempFilePath(hbContext.configuration, relation.tableName)
    val job = new Job(hbContext.configuration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", tmpPath)

    val conf = job.getConfiguration

    val hadoopReader = if (isLocal) {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(inputPath)).getCanonicalPath
      new HadoopReader(hbContext.sparkContext, pathString, delimiter)(relation)
    } else {
      new HadoopReader(hbContext.sparkContext, inputPath, delimiter)(relation)
    }

    val splitKeys = relation.getRegionStartKeys.toArray
    val wrappedConf = new SerializableWritable(conf)

    val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    val partitioner = new HBasePartitioner(splitKeys)
    val ordering = Ordering[HBaseRawType]
    val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)

    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = shuffled.id
    val jobFormat = new HFileOutputFormat2

    if (SparkEnv.get.conf.getBoolean("spark.hadoop.validateOutputSpecs", defaultValue = true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard =
      (context: TaskContext, iter: Iterator[(HBaseRawType, Array[HBaseRawType])]) => {
      val config = wrappedConf.value
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        context.attemptNumber)
      val hadoopContext = newTaskAttemptContext(config, attemptId)
      val format = new HFileOutputFormat2
      format match {
        case c: Configurable => c.setConf(config)
        case _ => ()
      }
      val committer = format.getOutputCommitter(hadoopContext).asInstanceOf[FileOutputCommitter]
      committer.setupTask(hadoopContext)

      val writer = format.getRecordWriter(hadoopContext).
        asInstanceOf[RecordWriter[ImmutableBytesWritable, KeyValue]]
      val bytesWritable = new ImmutableBytesWritable
      var recordsWritten = 0L
      var kv: (HBaseRawType, Array[HBaseRawType]) = null
      var prevK: HBaseRawType = null
      try {
        while (iter.hasNext) {
          kv = iter.next()

          if (prevK != null && Bytes.compareTo(kv._1, prevK) == 0) {
            // force flush because we cannot guarantee intra-row ordering
            writer.write(null, null)
          }

          for (i <- 0 until kv._2.size) {
            val nkc = relation.nonKeyColumns(i)
            if (kv._2(i) != null) {
              bytesWritable.set(kv._1)
              writer.write(bytesWritable, new KeyValue(kv._1, nkc.familyRaw,
                nkc.qualifierRaw, kv._2(i)))
            }
          }
          recordsWritten += 1

          prevK = kv._1
        }
      } finally {
        writer.close(hadoopContext)
      }

      committer.commitTask(hadoopContext)

      val targetPath = committer.getCommittedTaskPath(hadoopContext)
      val load = new LoadIncrementalHFiles(config)
      val htable = relation.htable
      // there maybe no target path
      logInfo(s"written $recordsWritten records")
      if(recordsWritten > 0) {
        load.doBulkLoad(targetPath, htable)
      }
      1
    }: Int

    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    logDebug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")
    hbContext.sparkContext.runJob(shuffled, writeShard)
    logDebug(s"finished BulkLoad : ${System.currentTimeMillis()}")
    jobCommitter.commitJob(jobTaskContext)

    Seq.empty[Row]
  }

  override def output = Nil
}
