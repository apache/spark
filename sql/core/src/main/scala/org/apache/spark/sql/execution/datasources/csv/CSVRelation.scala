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

import java.io.CharArrayWriter
import java.nio.charset.{Charset, StandardCharsets}

import com.univocity.parsers.csv.CsvWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{CompressionCodecs, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * Provides access to CSV data from pure SQL statements.
 */
class DefaultSource extends FileFormat with DataSourceRegister {

  override def shortName(): String = "csv"

  override def toString: String = "CSV"

  override def equals(other: Any): Boolean = other.isInstanceOf[DefaultSource]

  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val csvOptions = new CSVOptions(options)

    // TODO: Move filtering.
    val paths = files.filterNot(_.getPath.getName startsWith "_").map(_.getPath.toString)
    val rdd = createBaseRdd(sqlContext, csvOptions, paths)
    val schema = if (csvOptions.inferSchemaFlag) {
      InferSchema.infer(rdd, csvOptions)
    } else {
      // By default fields are assumed to be StringType
      val filteredRdd = rdd.mapPartitions(CSVUtils.filterCommentAndEmpty(_, csvOptions))
      val firstLine = filteredRdd.first()
      val firstRow = UnivocityParser.tokenizeLine(firstLine, csvOptions)
      val header = if (csvOptions.headerFlag) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
      }
      val schemaFields = header.map { fieldName =>
        StructField(fieldName.toString, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
    Some(schema)
  }

  override def prepareWrite(
      sqlContext: SQLContext,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val csvOptions = new CSVOptions(options)
    csvOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new CSVOutputWriterFactory(csvOptions)
  }

  override def buildReader(
      sqlContext: SQLContext,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String]): (PartitionedFile) => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)
    val headers = requiredSchema.fields.map(_.name)

    val conf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val broadcastedConf = sqlContext.sparkContext.broadcast(new SerializableConfiguration(conf))

    (file: PartitionedFile) => {
      val lines = {
        val conf = broadcastedConf.value.value
        new HadoopFileLinesReader(file, conf).map { line =>
          new String(line.getBytes, 0, line.getLength, csvOptions.charset)
        }
      }

      // Appends partition values
      val fullOutput = requiredSchema.toAttributes ++ partitionSchema.toAttributes
      val joinedRow = new JoinedRow()

      // TODO What if the first partitioned file consists of only comments and empty lines?
      val shouldDropHeader = csvOptions.headerFlag && file.start == 0
      val rows = UnivocityParser.parseCsv(
        lines,
        dataSchema,
        requiredSchema,
        headers,
        shouldDropHeader,
        csvOptions)

      val appendPartitionColumns = GenerateUnsafeProjection.generate(fullOutput, fullOutput)
      rows.map { row =>
        appendPartitionColumns(joinedRow(row, file.partitionValues))
      }
    }
  }

  private def createBaseRdd(
      sqlContext: SQLContext,
      options: CSVOptions,
      inputPaths: Seq[String]): RDD[String] = {
    val location = inputPaths.mkString(",")
    if (Charset.forName(options.charset) == StandardCharsets.UTF_8) {
      sqlContext.sparkContext.textFile(location)
    } else {
      val charset = options.charset
      sqlContext.sparkContext
        .hadoopFile[LongWritable, Text, TextInputFormat](location)
        .mapPartitions(_.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)))
    }
  }
}

private[sql] class CSVOutputWriterFactory(options: CSVOptions) extends OutputWriterFactory {
  override def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    if (bucketId.isDefined) sys.error("csv doesn't support bucketing")
    new CsvOutputWriter(path, dataSchema, context, options)
  }
}

private[sql] class CsvOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    options: CSVOptions) extends OutputWriter with Logging {

  // create the Generator without separator inserted between 2 records
  private[this] val result = new Text()

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

  private val writerSettings = UnivocityGenerator.getSettings(options)
  private val headers = dataSchema.fieldNames
  writerSettings.setHeaders(headers: _*)

  private[this] val writer = new CharArrayWriter()
  private[this] val csvWriter = new CsvWriter(writer, writerSettings)
  private[this] var writeHeader = options.headerFlag

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    // TODO: Instead of converting and writing every row, we should use the univocity buffer
    UnivocityGenerator(dataSchema, csvWriter, headers, writeHeader, options)(row)
    csvWriter.flush()

    result.set(writer.toString)
    writer.reset()

    writeHeader = false
    recordWriter.write(NullWritable.get(), result)
  }

  override def close(): Unit = {
    csvWriter.close()
    recordWriter.close(context)
  }
}
