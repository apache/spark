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

import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.CompressionCodecs
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[sql] class CSVRelation(
    private val inputRDD: Option[RDD[String]],
    override val paths: Array[String] = Array.empty[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String])
    (@transient val sqlContext: SQLContext) extends HadoopFsRelation {

  override lazy val dataSchema: StructType = maybeDataSchema match {
    case Some(structType) => structType
    case None => inferSchema(paths)
  }

  private val options = new CSVOptions(parameters)

  @transient
  private var cachedRDD: Option[RDD[String]] = None

  private def readText(location: String): RDD[String] = {
    if (Charset.forName(options.charset) == Charset.forName("UTF-8")) {
      sqlContext.sparkContext.textFile(location)
    } else {
      val charset = options.charset
      sqlContext.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](location)
        .mapPartitions { _.map { pair =>
            new String(pair._2.getBytes, 0, pair._2.getLength, charset)
          }
        }
    }
  }

  private def baseRdd(inputPaths: Array[String]): RDD[String] = {
    inputRDD.getOrElse {
      cachedRDD.getOrElse {
        val rdd = readText(inputPaths.mkString(","))
        cachedRDD = Some(rdd)
        rdd
      }
    }
  }

  private def tokenRdd(header: Array[String], inputPaths: Array[String]): RDD[Array[String]] = {
    val rdd = baseRdd(inputPaths)
    // Make sure firstLine is materialized before sending to executors
    val firstLine = if (options.headerFlag) findFirstLine(rdd) else null
    CSVRelation.univocityTokenizer(rdd, header, firstLine, options)
  }

  /**
    * This supports to eliminate unneeded columns before producing an RDD
    * containing all of its tuples as Row objects. This reads all the tokens of each line
    * and then drop unneeded tokens without casting and type-checking by mapping
    * both the indices produced by `requiredColumns` and the ones of tokens.
    * TODO: Switch to using buildInternalScan
    */
  override def buildScan(requiredColumns: Array[String], inputs: Array[FileStatus]): RDD[Row] = {
    val pathsString = inputs.map(_.getPath.toUri.toString)
    val header = schema.fields.map(_.name)
    val tokenizedRdd = tokenRdd(header, pathsString)
    CSVRelation.parseCsv(tokenizedRdd, schema, requiredColumns, inputs, sqlContext, options)
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val conf = job.getConfiguration
    options.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new CSVOutputWriterFactory(options)
  }

  override def hashCode(): Int = Objects.hashCode(paths.toSet, dataSchema, schema, partitionColumns)

  override def equals(other: Any): Boolean = other match {
    case that: CSVRelation => {
      val equalPath = paths.toSet == that.paths.toSet
      val equalDataSchema = dataSchema == that.dataSchema
      val equalSchema = schema == that.schema
      val equalPartitionColums = partitionColumns == that.partitionColumns

      equalPath && equalDataSchema && equalSchema && equalPartitionColums
    }
    case _ => false
  }

  private def inferSchema(paths: Array[String]): StructType = {
    val rdd = baseRdd(paths)
    val firstLine = findFirstLine(rdd)
    val firstRow = new LineCsvReader(options).parseLine(firstLine)

    val header = if (options.headerFlag) {
      firstRow
    } else {
      firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
    }

    val parsedRdd = tokenRdd(header, paths)
    if (options.inferSchemaFlag) {
      CSVInferSchema.infer(parsedRdd, header, options.nullValue)
    } else {
      // By default fields are assumed to be StringType
      val schemaFields = header.map { fieldName =>
        StructField(fieldName.toString, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  /**
    * Returns the first line of the first non-empty file in path
    */
  private def findFirstLine(rdd: RDD[String]): String = {
    if (options.isCommentSet) {
      val comment = options.comment.toString
      rdd.filter { line =>
        line.trim.nonEmpty && !line.startsWith(comment)
      }.first()
    } else {
      rdd.filter { line =>
        line.trim.nonEmpty
      }.first()
    }
  }
}

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
      if (params.dropMalformed && schemaFields.length != tokens.size) {
        logWarning(s"Dropping malformed line: ${tokens.mkString(params.delimiter.toString)}")
        None
      } else if (params.failFast && schemaFields.length != tokens.size) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
          s"${tokens.mkString(params.delimiter.toString)}")
      } else {
        val indexSafeTokens = if (params.permissive && schemaFields.length > tokens.size) {
          tokens ++ new Array[String](schemaFields.length - tokens.size)
        } else if (params.permissive && schemaFields.length < tokens.size) {
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
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
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
