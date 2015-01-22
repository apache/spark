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

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, RecordWriter}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.HBasePartitioner.HBaseRawOrdering
import org.apache.spark.sql.hbase.util.{HBaseKVHelper, Util}
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.{Logging, SerializableWritable, SparkEnv}

import scala.collection.JavaConversions._
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
     delimiter: Option[String]) extends RunnableCommand with Logging {

  private[hbase] def makeBulkLoadRDD(
       splitKeys: Array[HBaseRawType],
       hadoopReader: HadoopReader,
       job: Job,
       tmpPath: String,
       relation: HBaseRelation) = {
    val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    val partitioner = new HBasePartitioner(splitKeys)
    val ordering = Ordering[HBaseRawType]
    val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)
    val bulkLoadRDD = shuffled.mapPartitions { iter =>
      // the rdd now already sort by key, to sort by value
      logDebug(s"after shuffle, sort by value, begin: ${System.currentTimeMillis()}")
      val map = new java.util.TreeSet[KeyValue](KeyValue.COMPARATOR)
      var preKV: (HBaseRawType, Array[HBaseRawType]) = null
      var nowKV: (HBaseRawType, Array[HBaseRawType]) = null
      val ret = new ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
      if (iter.hasNext) {
        preKV = iter.next()
        for (i <- 0 until preKV._2.size) {
          val nkc = relation.nonKeyColumns(i)
          if (preKV._2(i) != null) {
            val kv = new KeyValue(preKV._1, nkc.familyRaw, nkc.qualifierRaw, preKV._2(i))
            map.add(kv)
          }
        }
        while (iter.hasNext) {
          nowKV = iter.next()
          if (Bytes.equals(nowKV._1, preKV._1)) {
            for (i <- 0 until nowKV._2.size) {
              val nkc = relation.nonKeyColumns(i)
              if (preKV._2(i) != null) {
                val kv = new KeyValue(preKV._1, nkc.familyRaw, nkc.qualifierRaw, nowKV._2(i))
                map.add(kv)
              }
            }
          } else {
            ret ++= map.iterator().map((new ImmutableBytesWritable(preKV._1), _))
            preKV = nowKV
            map.clear()
            for (i <- 0 until preKV._2.size) {
              val nkc = relation.nonKeyColumns(i)
              if (preKV._2(i) != null) {
                val kv = new KeyValue(nowKV._1, nkc.familyRaw, nkc.qualifierRaw, nowKV._2(i))
                map.add(kv)
              }
            }
          }
        }
        ret ++= map.iterator().map((new ImmutableBytesWritable(preKV._1), _))
        map.clear()
        logDebug(s"after shuffle,sort by value,finish: ${System.currentTimeMillis()}")
        ret.iterator
      } else {
        logDebug(s"after shuffle,finish sort by value, finish: ${System.currentTimeMillis()}")
        Iterator.empty
      }
    }
    logDebug(s"save as hfile, begin: ${System.currentTimeMillis()}")
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    job.getConfiguration.set("mapred.output.dir", tmpPath)
    bulkLoadRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    logDebug(s"save as hfile, finish: ${System.currentTimeMillis()}")
  }

  override def run(sqlContext: SQLContext) = {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    val hbContext = sqlContext.asInstanceOf[HBaseSQLContext]

    val conf = hbContext.configuration
    val job = Job.getInstance(conf)

    val hadoopReader = if (isLocal) {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(path)).getCanonicalPath
      new HadoopReader(hbContext.sparkContext, pathString, delimiter)(relation)
    } else {
      new HadoopReader(hbContext.sparkContext, path, delimiter)(relation)
    }

    // tmp path for storing HFile
    val tmpPath = Util.getTempFilePath(conf, relation.tableName)
    val splitKeys = relation.getRegionStartKeys.toArray
    logDebug(s"start BulkLoad on table ${relation.htable.getName}: ${System.currentTimeMillis()}")
    makeBulkLoadRDD(splitKeys, hadoopReader, job, tmpPath, relation)
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
    path: String,
    tableName: String,
    isLocal: Boolean,
    delimiter: Option[String]) extends RunnableCommand with SparkHadoopMapReduceUtil {

  private[hbase] def makeBulkLoadRDD(
       splitKeys: Array[HBaseRawType],
       hadoopReader: HadoopReader,
       wrappedConf: SerializableWritable[Configuration],
       tmpPath: String)(relation: HBaseRelation) = {
    val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    val partitioner = new HBasePartitioner(splitKeys)
    val ordering = Ordering[HBaseRawType]
    val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)
    shuffled.mapPartitionsWithIndex { (index, iter) =>
      // the rdd now already sort by key
      logDebug(s"after shuffle: ${System.currentTimeMillis()}")
      logDebug(s"save as hfile, begin: ${System.currentTimeMillis()}")

      var config = wrappedConf.value
      config.set("mapred.output.dir", tmpPath + index)

      val job = new Job(config)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[KeyValue])
      job.setOutputFormatClass(classOf[HFileOutputFormat2])

//      val context = TaskContext.get
      val outfmt = classOf[HFileOutputFormat2]
      val jobFormat = outfmt.newInstance
      if (SparkEnv.get.conf.getBoolean("spark.hadoop.validateOutputSpecs", defaultValue = true)) {
        // FileOutputFormat ignores the filesystem parameter
        jobFormat.checkOutputSpecs(job)
      }
      config = job.getConfiguration

      val formatter = new SimpleDateFormat("yyyyMMddHHmm")
      val jobtrackerID = formatter.format(new Date())
      val stageId = shuffled.id

      def writeShard = {
        // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
        // around by taking a mod. We expect that no task will be attempted 2 billion times.
//        val attemptNumber = (context.taskAttemptId % Int.MaxValue).toInt
        /* "reduce task" <split #> <attempt # = spark task #> */
        val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
        val hadoopContext = newTaskAttemptContext(config, attemptId)
        jobFormat match {
          case c: Configurable => c.setConf(config)
          case _ => ()
        }
        val committer = jobFormat.getOutputCommitter(hadoopContext)
        committer.setupJob(hadoopContext)
        val writer = jobFormat.getRecordWriter(hadoopContext).
          asInstanceOf[RecordWriter[ImmutableBytesWritable, KeyValue]]
        val bytesWritable = new ImmutableBytesWritable
        var recordsWritten = 0L
        try {
          var kv: (HBaseRawType, Array[HBaseRawType]) = null
          while (iter.hasNext) {
            kv = iter.next()
            for (i <- 0 until kv._2.size) {
              val nkc = relation.nonKeyColumns(i)
              if (kv._2(i) != null) {
                bytesWritable.set(kv._1)
                writer.write(bytesWritable, new KeyValue(kv._1, nkc.familyRaw,
                  nkc.qualifierRaw, kv._2(i)))
              }
            }
            recordsWritten += 1
            if (iter.hasNext) {
              // force flush because we cannot guarantee intra-row ordering
              writer.write(null, null)
            }
          }
        } finally {
          writer.close(hadoopContext)
        }
        committer.commitTask(hadoopContext)
        committer.commitJob(hadoopContext)
        val path = new Path(tmpPath + index)
        logDebug(s"save as hfile, finish: ${System.currentTimeMillis()}, " +
          s"written $recordsWritten records")
        // return the output path
        Seq(path.getFileSystem(config).makeQualified(path).toString).toIterator
      }
      writeShard
    }
  }

  override def run(sqlContext: SQLContext) = {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    val hbContext = sqlContext.asInstanceOf[HBaseSQLContext]
    val conf = hbContext.configuration

    val hadoopReader = if (isLocal) {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(path)).getCanonicalPath
      new HadoopReader(hbContext.sparkContext, pathString, delimiter)(relation)
    } else {
      new HadoopReader(hbContext.sparkContext, path, delimiter)(relation)
    }

    // tmp path for storing HFile
    val tmpPath = Util.getTempFilePath(conf, relation.tableName)
    val splitKeys = relation.getRegionStartKeys.toArray
    val wrappedConf = new SerializableWritable(conf)
    makeBulkLoadRDD(
      splitKeys,
      hadoopReader,
      wrappedConf,
      tmpPath)(relation).foreachPartition { iter =>
      logDebug(s"start BulkLoad : ${System.currentTimeMillis()}")
      val conf = wrappedConf.value
      val load = new LoadIncrementalHFiles(conf)
      val htable = relation.htable
      if (iter.hasNext) {
        load.doBulkLoad(new Path(iter.next()), htable)
      }
      logDebug(s"finished BulkLoad : ${System.currentTimeMillis()}")

    }
    logDebug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")
    Seq.empty[Row]
  }

  override def output = Nil
}
