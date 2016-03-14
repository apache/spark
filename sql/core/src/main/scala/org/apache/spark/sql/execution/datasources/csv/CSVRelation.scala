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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
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
