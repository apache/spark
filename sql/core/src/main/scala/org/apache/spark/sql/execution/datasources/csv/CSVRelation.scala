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

import java.nio.charset.Charset

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{LineRecordReader, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

object CSVRelation extends Logging {

  def univocityTokenizer(
      file: RDD[String],
      header: Seq[String],
      firstLine: String,
      params: CSVOptions): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    file.mapPartitionsWithIndex({
      case (split, iter) => new BulkCsvReader(
        if (params.headerFlag) iter.filterNot(_ == firstLine) else iter,
        params,
        headers = header)
    }, true)
  }

  def parseCsv(
      tokenizedRDD: RDD[Array[String]],
      schema: StructType,
      requiredColumns: Array[String],
      inputs: Array[FileStatus],
      sqlContext: SQLContext,
      params: CSVOptions): RDD[Row] = {

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
    val rowArray = new Array[Any](safeRequiredIndices.length)
    val requiredSize = requiredFields.length
    tokenizedRDD.flatMap { tokens =>
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
            rowArray(subIndex) = CSVTypeCast.castTo(
              indexSafeTokens(index),
              field.dataType,
              field.nullable,
              params.nullValue)
            subIndex = subIndex + 1
          }
          Some(Row.fromSeq(rowArray.take(requiredSize)))
        } catch {
          case NonFatal(e) if params.dropMalformed =>
            logWarning("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(params.delimiter.toString)}")
            None
        }
      }
    }
  }
}

/**
 * Because `TextInputFormat` in Hadoop does not support non-ascii compatible encodings,
 * We need another `InputFormat` to handle the encodings. See SPARK-13108.
 */
private[csv] class EncodingTextInputFormat extends TextInputFormat {
  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    val conf: Configuration = {
      // Use reflection to get the Configuration. This is necessary because TaskAttemptContext is
      // a class in Hadoop 1.x and an interface in Hadoop 2.x.
      val method = context.getClass.getMethod("getConfiguration")
      method.invoke(context).asInstanceOf[Configuration]
    }
    val charset = Charset.forName(conf.get(EncodingTextInputFormat.ENCODING_KEY, "UTF-8"))
    val charsetName = charset.name
    val safeRecordDelimiterBytes = {
      val delimiter = "\n"
      val recordDelimiterBytes = delimiter.getBytes(charset)
      EncodingTextInputFormat.stripBOM(charsetName, recordDelimiterBytes)
    }

    new LineRecordReader(safeRecordDelimiterBytes) {
      var isFirst = true
      override def getCurrentValue: Text = {
        val value = super.getCurrentValue
        if (isFirst) {
          isFirst = false
          val safeBytes = EncodingTextInputFormat.stripBOM(charsetName, value.getBytes)
          new Text(safeBytes)
        } else {
          value
        }
      }
    }
  }
}

private[csv] object EncodingTextInputFormat {
  // configuration key for encoding type
  val ENCODING_KEY = "encodinginputformat.encoding"
  // BOM bytes for UTF-8, UTF-16 and UTF-32
  private val utf8BOM = Array(0xEF.toByte, 0xBB.toByte, 0xBF.toByte)
  private val utf16beBOM = Array(0xFE.toByte, 0xFF.toByte)
  private val utf16leBOM = Array(0xFF.toByte, 0xFE.toByte)
  private val utf32beBOM = Array(0x00.toByte, 0x00.toByte, 0xFE.toByte, 0xFF.toByte)
  private val utf32leBOM = Array(0xFF.toByte, 0xFE.toByte, 0x00.toByte, 0x00.toByte)

  def stripBOM(charsetName: String, bytes: Array[Byte]): Array[Byte] = {
    charsetName match {
      case "UTF-8" if bytes.startsWith(utf8BOM) =>
        bytes.slice(utf8BOM.length, bytes.length)
      case "UTF-16" | "UTF-16BE" if bytes.startsWith(utf16beBOM) =>
        bytes.slice(utf16beBOM.length, bytes.length)
      case "UTF-16LE" if bytes.startsWith(utf16leBOM) =>
        bytes.slice(utf16leBOM.length, bytes.length)
      case "UTF-32" | "UTF-32BE" if bytes.startsWith(utf32beBOM) =>
        bytes.slice(utf32beBOM.length, bytes.length)
      case "UTF-32LE" if bytes.startsWith(utf32leBOM) =>
        bytes.slice(utf32leBOM.length, bytes.length)
      case _ => bytes
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
    new CSVOutputWriter(path, dataSchema, context, params)
  }
}

private[sql] class CSVOutputWriter(
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
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
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
