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

package org.apache.spark.sql.parquet

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat, FileOutputCommitter}

import parquet.hadoop.{ParquetRecordReader, ParquetInputFormat, ParquetOutputFormat}
import parquet.hadoop.api.ReadSupport
import parquet.hadoop.util.ContextUtil
import parquet.io.InvalidRecordException
import parquet.schema.MessageType

import org.apache.spark.{Logging, SerializableWritable, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Row}
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}

/**
 * Parquet table scan operator. Imports the file that backs the given
 * [[org.apache.spark.sql.parquet.ParquetRelation]] as a ``RDD[Row]``.
 */
case class ParquetTableScan(
    // note: output cannot be transient, see
    // https://issues.apache.org/jira/browse/SPARK-1367
    output: Seq[Attribute],
    relation: ParquetRelation,
    columnPruningPred: Seq[Expression])(
    @transient val sqlContext: SQLContext)
  extends LeafNode {

  override def execute(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(
      job,
      classOf[org.apache.spark.sql.parquet.RowReadSupport])
    val conf: Configuration = ContextUtil.getConfiguration(job)
    val fileList = FileSystemHelper.listFiles(relation.path, conf)
    // add all paths in the directory but skip "hidden" ones such
    // as "_SUCCESS" and "_metadata"
    for (path <- fileList if !path.getName.startsWith("_")) {
      NewFileInputFormat.addInputPath(job, path)
    }

    // Store both requested and original schema in `Configuration`
    conf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(output))
    conf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(relation.output))

    // Store record filtering predicate in `Configuration`
    // Note 1: the input format ignores all predicates that cannot be expressed
    // as simple column predicate filters in Parquet. Here we just record
    // the whole pruning predicate.
    // Note 2: you can disable filter predicate pushdown by setting
    // "spark.sql.hints.parquetFilterPushdown" to false inside SparkConf.
    if (columnPruningPred.length > 0 &&
      sc.conf.getBoolean(ParquetFilters.PARQUET_FILTER_PUSHDOWN_ENABLED, true)) {
      ParquetFilters.serializeFilterExpressions(columnPruningPred, conf)
    }

    sc.newAPIHadoopRDD(
      conf,
      classOf[org.apache.spark.sql.parquet.FilteringParquetRowInputFormat],
      classOf[Void],
      classOf[Row])
      .map(_._2)
      .filter(_ != null) // Parquet's record filters may produce null values
  }

  override def otherCopyArgs = sqlContext :: Nil

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): ParquetTableScan = {
    val success = validateProjection(prunedAttributes)
    if (success) {
      ParquetTableScan(prunedAttributes, relation, columnPruningPred)(sqlContext)
    } else {
      sys.error("Warning: Could not validate Parquet schema projection in pruneColumns")
      this
    }
  }

  /**
   * Evaluates a candidate projection by checking whether the candidate is a subtype
   * of the original type.
   *
   * @param projection The candidate projection.
   * @return True if the projection is valid, false otherwise.
   */
  private def validateProjection(projection: Seq[Attribute]): Boolean = {
    val original: MessageType = relation.parquetSchema
    val candidate: MessageType = ParquetTypesConverter.convertFromAttributes(projection)
    try {
      original.checkContains(candidate)
      true
    } catch {
      case e: InvalidRecordException => {
        false
      }
    }
  }
}

/**
 * Operator that acts as a sink for queries on RDDs and can be used to
 * store the output inside a directory of Parquet files. This operator
 * is similar to Hive's INSERT INTO TABLE operation in the sense that
 * one can choose to either overwrite or append to a directory. Note
 * that consecutive insertions to the same table must have compatible
 * (source) schemas.
 *
 * WARNING: EXPERIMENTAL! InsertIntoParquetTable with overwrite=false may
 * cause data corruption in the case that multiple users try to append to
 * the same table simultaneously. Inserting into a table that was
 * previously generated by other means (e.g., by creating an HDFS
 * directory and importing Parquet files generated by other tools) may
 * cause unpredicted behaviour and therefore results in a RuntimeException
 * (only detected via filename pattern so will not catch all cases).
 */
case class InsertIntoParquetTable(
    relation: ParquetRelation,
    child: SparkPlan,
    overwrite: Boolean = false)(
    @transient val sqlContext: SQLContext)
  extends UnaryNode with SparkHadoopMapReduceUtil {

  /**
   * Inserts all rows into the Parquet file.
   */
  override def execute() = {
    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execution will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().

    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)

    val writeSupport =
      if (child.output.map(_.dataType).forall(_.isPrimitive)) {
        logger.debug("Initializing MutableRowWriteSupport")
        classOf[org.apache.spark.sql.parquet.MutableRowWriteSupport]
      } else {
        classOf[org.apache.spark.sql.parquet.RowWriteSupport]
      }

    ParquetOutputFormat.setWriteSupportClass(job, writeSupport)

    val conf = ContextUtil.getConfiguration(job)
    RowWriteSupport.setSchema(relation.output, conf)

    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)

    if (overwrite) {
      try {
        fs.delete(fspath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${fspath.toString} prior"
              + s" to InsertIntoParquetTable:\n${e.toString}")
      }
    }
    saveAsHadoopFile(childRdd, relation.path.toString, conf)

    // We return the child RDD to allow chaining (alternatively, one could return nothing).
    childRdd
  }

  override def output = child.output

  override def otherCopyArgs = sqlContext :: Nil

  /**
   * Stores the given Row RDD as a Hadoop file.
   *
   * Note: We cannot use ``saveAsNewAPIHadoopFile`` from [[org.apache.spark.rdd.PairRDDFunctions]]
   * together with [[org.apache.spark.util.MutablePair]] because ``PairRDDFunctions`` uses
   * ``Tuple2`` and not ``Product2``. Also, we want to allow appending files to an existing
   * directory and need to determine which was the largest written file index before starting to
   * write.
   *
   * @param rdd The [[org.apache.spark.rdd.RDD]] to writer
   * @param path The directory to write to.
   * @param conf A [[org.apache.hadoop.conf.Configuration]].
   */
  private def saveAsHadoopFile(
      rdd: RDD[Row],
      path: String,
      conf: Configuration) {
    val job = new Job(conf)
    val keyType = classOf[Void]
    job.setOutputKeyClass(keyType)
    job.setOutputValueClass(classOf[Row])
    NewFileOutputFormat.setOutputPath(job, new Path(path))
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()

    val taskIdOffset =
      if (overwrite) {
        1
      } else {
        FileSystemHelper
          .findMaxTaskId(NewFileOutputFormat.getOutputPath(job).toString, job.getConfiguration) + 1
      }

    def writeShard(context: TaskContext, iter: Iterator[Row]): Int = {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        attemptNumber)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = new AppendingParquetOutputFormat(taskIdOffset)
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext)
      while (iter.hasNext) {
        val row = iter.next()
        writer.write(null, row)
      }
      writer.close(hadoopContext)
      committer.commitTask(hadoopContext)
      return 1
    }
    val jobFormat = new AppendingParquetOutputFormat(taskIdOffset)
    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    sqlContext.sparkContext.runJob(rdd, writeShard _)
    jobCommitter.commitJob(jobTaskContext)
  }
}

/**
 * TODO: this will be able to append to directories it created itself, not necessarily
 * to imported ones.
 */
private[parquet] class AppendingParquetOutputFormat(offset: Int)
  extends parquet.hadoop.ParquetOutputFormat[Row] {
  // override to accept existing directories as valid output directory
  override def checkOutputSpecs(job: JobContext): Unit = {}

  // override to choose output filename so not overwrite existing ones
  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val taskId: TaskID = context.getTaskAttemptID.getTaskID
    val partition: Int = taskId.getId
    val filename = s"part-r-${partition + offset}.parquet"
    val committer: FileOutputCommitter =
      getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
    new Path(committer.getWorkPath, filename)
  }
}

/**
 * We extend ParquetInputFormat in order to have more control over which
 * RecordFilter we want to use.
 */
private[parquet] class FilteringParquetRowInputFormat
  extends parquet.hadoop.ParquetInputFormat[Row] with Logging {
  override def createRecordReader(
      inputSplit: InputSplit,
      taskAttemptContext: TaskAttemptContext): RecordReader[Void, Row] = {
    val readSupport: ReadSupport[Row] = new RowReadSupport()

    val filterExpressions =
      ParquetFilters.deserializeFilterExpressions(ContextUtil.getConfiguration(taskAttemptContext))
    if (filterExpressions.length > 0) {
      logInfo(s"Pushing down predicates for RecordFilter: ${filterExpressions.mkString(", ")}")
      new ParquetRecordReader[Row](
        readSupport,
        ParquetFilters.createRecordFilter(filterExpressions))
    } else {
      new ParquetRecordReader[Row](readSupport)
    }
  }
}

private[parquet] object FileSystemHelper {
  def listFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.exists(path) || !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: path $path does not exist or is not a directory")
    }
    fs.listStatus(path).map(_.getPath)
  }

    /**
     * Finds the maximum taskid in the output file names at the given path.
     */
  def findMaxTaskId(pathStr: String, conf: Configuration): Int = {
    val files = FileSystemHelper.listFiles(pathStr, conf)
    // filename pattern is part-r-<int>.parquet
    val nameP = new scala.util.matching.Regex("""part-r-(\d{1,}).parquet""", "taskid")
    val hiddenFileP = new scala.util.matching.Regex("_.*")
    files.map(_.getName).map {
      case nameP(taskid) => taskid.toInt
      case hiddenFileP() => 0
      case other: String => {
        sys.error("ERROR: attempting to append to set of Parquet files and found file" +
          s"that does not match name pattern: $other")
        0
      }
      case _ => 0
    }.reduceLeft((a, b) => if (a < b) b else a)
  }
}
