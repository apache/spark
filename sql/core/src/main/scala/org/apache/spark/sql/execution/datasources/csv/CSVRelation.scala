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

package org.apache.spark.sql.execution.datasources.csv

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.execution.datasources.{CompressionCodecs, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

object CSVRelation extends Logging {

  def univocityTokenizer(
      file: RDD[String],
      header: Seq[String],
      firstLine: String,
      params: CSVOptions): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    file.mapPartitions { iter =>
      new BulkCsvReader(
        if (params.headerFlag) iter.filterNot(_ == firstLine) else iter,
        params,
        headers = header)
    }
  }

  def csvParser(
      schema: StructType,
      requiredColumns: Array[String],
      params: CSVOptions): Array[String] => Option[InternalRow] = {
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val safeRequiredFields = if (params.dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
    schemaFields.zipWithIndex.filter {
      case (field, _) => safeRequiredFields.contains(field)
    }.foreach {
      case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
    }
    val requiredSize = requiredFields.length
    val row = new GenericMutableRow(requiredSize)

    (tokens: Array[String]) => {
      if (params.dropMalformed && schemaFields.length != tokens.length) {
        logWarning(s"Dropping malformed line: ${tokens.mkString(params.delimiter.toString)}")
        None
      } else if (params.failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
          s"${tokens.mkString(params.delimiter.toString)}")
      } else {
        val indexSafeTokens = if (params.permissive && schemaFields.length > tokens.length) {
          tokens ++ new Array[String](schemaFields.length - tokens.length)
        } else if (params.permissive && schemaFields.length < tokens.length) {
          tokens.take(schemaFields.length)
        } else {
          tokens
        }
        try {
          var index: Int = 0
          var subIndex: Int = 0
          while (subIndex < safeRequiredIndices.length) {
            index = safeRequiredIndices(subIndex)
            val field = schemaFields(index)
            // It anyway needs to try to parse since it decides if this row is malformed
            // or not after trying to cast in `DROPMALFORMED` mode even if the casted
            // value is not stored in the row.
            val value = CSVTypeCast.castTo(
              indexSafeTokens(index),
              field.dataType,
              field.nullable,
              params)
            if (subIndex < requiredSize) {
              row(subIndex) = value
            }
            subIndex = subIndex + 1
          }
          Some(row)
        } catch {
          case NonFatal(e) if params.dropMalformed =>
            logWarning("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(params.delimiter.toString)}")
            None
        }
      }
    }
  }

  def parseCsv(
      tokenizedRDD: RDD[Array[String]],
      schema: StructType,
      requiredColumns: Array[String],
      options: CSVOptions): RDD[InternalRow] = {
    val parser = csvParser(schema, requiredColumns, options)
    tokenizedRDD.flatMap(parser(_).toSeq)
  }

  // Skips the header line of each file if the `header` option is set to true.
  def dropHeaderLine(
      file: PartitionedFile, lines: Iterator[String], csvOptions: CSVOptions): Unit = {
    // TODO What if the first partitioned file consists of only comments and empty lines?
    if (csvOptions.headerFlag && file.start == 0) {
      val nonEmptyLines = if (csvOptions.isCommentSet) {
        val commentPrefix = csvOptions.comment.toString
        lines.dropWhile { line =>
          line.trim.isEmpty || line.trim.startsWith(commentPrefix)
        }
      } else {
        lines.dropWhile(_.trim.isEmpty)
      }

      if (nonEmptyLines.hasNext) nonEmptyLines.drop(1)
    }
  }
}

private[sql] class CSVOutputWriterFactory(params: CSVOptions) extends OutputWriterFactory {
  override def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    if (bucketId.isDefined) sys.error("csv doesn't support bucketing")
    new CsvOutputWriter(path, dataSchema, context, params)
  }
}

private[sql] class CsvOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    params: CSVOptions) extends OutputWriter with Logging {

  // create the Generator without separator inserted between 2 records
  private[this] val text = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = context.getConfiguration
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
        val taskAttemptId = context.getTaskAttemptID
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId.csv$extension")
      }
    }.getRecordWriter(context)
  }

  private var firstRow: Boolean = params.headerFlag

  private val csvWriter = new LineCsvWriter(params, dataSchema.fieldNames.toSeq)

  private def rowToString(row: Seq[Any]): Seq[String] = row.map { field =>
    if (field != null) {
      field.toString
    } else {
      params.nullValue
    }
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    // TODO: Instead of converting and writing every row, we should use the univocity buffer
    val resultString = csvWriter.writeRow(rowToString(row.toSeq(dataSchema)), firstRow)
    if (firstRow) {
      firstRow = false
    }
    text.set(resultString)
    recordWriter.write(NullWritable.get(), text)
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}

/**
 * A factory for generating [[OutputWriter]]s for writing CSV files. This implemented is different
 * from the [[CSVOutputWriterFactory]] as this does not use any [[OutputCommitter]]. It simply
 * writes the data to the path used to generate the output writer. Callers of this factory
 * has to ensure which files are to be considered as committed.
 */
private[sql] class CSVStreamingOutputWriterFactory(
    sqlConf: SQLConf,
    dataSchema: StructType,
    hadoopConf: Configuration,
    options: Map[String, String]) extends OutputWriterFactory {

  private val csvOptions = new CSVOptions(options)

  private val serializableConf = {
    val conf = Job.getInstance(hadoopConf).getConfiguration
    csvOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }
    new SerializableConfiguration(conf)
  }

  /**
   * Returns a [[OutputWriter]] that writes data to the give path without using an
   * [[OutputCommitter]].
   */
  override private[sql] def newWriter(path: String): OutputWriter = new OutputWriter {

    private val hadoopTaskAttempId = new TaskAttemptID(new TaskID(new JobID, TaskType.MAP, 0), 0)
    private val hadoopAttemptContext =
      new TaskAttemptContextImpl(serializableConf.value, hadoopTaskAttempId)

    // Instance of RecordWriter that does not use OutputCommitter
    private val recordWriter = createNoCommitterRecordWriter(path, hadoopAttemptContext)

    // create the Generator without separator inserted between 2 records
    private[this] val text = new Text()

    private var firstRow: Boolean = csvOptions.headerFlag

    private val csvWriter = new LineCsvWriter(csvOptions, dataSchema.fieldNames.toSeq)

    private def rowToString(row: Seq[Any]): Seq[String] = row.map { field =>
      if (field != null) {
        field.toString
      } else {
        csvOptions.nullValue
      }
    }

    override def write(row: Row): Unit = {
      throw new UnsupportedOperationException("call writeInternal")
    }

    protected[sql] override def writeInternal(row: InternalRow): Unit = {
      val resultString = csvWriter.writeRow(rowToString(row.toSeq(dataSchema)), firstRow)
      if (firstRow) {
        firstRow = false
      }
      text.set(resultString)
      recordWriter.write(NullWritable.get(), text)
    }

    override def close(): Unit = recordWriter.close(hadoopAttemptContext)
  }

  /** Create a [[RecordWriter]] that writes the given path without using an [[OutputCommitter]]. */
  private def createNoCommitterRecordWriter(
      path: String,
      hadoopAttemptContext: TaskAttemptContext): RecordWriter[NullWritable, Text] = {
    // Custom TextOutputFormat that disable use of committer and writes to the given path
    val outputFormat = new TextOutputFormat[NullWritable, Text]() {
      override def getOutputCommitter(c: TaskAttemptContext): OutputCommitter = { null }
      override def getDefaultWorkFile(c: TaskAttemptContext, ext: String): Path = { new Path(path) }
    }
    outputFormat.getRecordWriter(hadoopAttemptContext)
  }

  /** Disable the use of the older API. */
  def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    throw new UnsupportedOperationException(
      "this version of newInstance is not supported for " +
        classOf[CSVStreamingOutputWriterFactory].getSimpleName)
  }
}
