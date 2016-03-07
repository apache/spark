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

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.CompressionCodecs
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

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
    val rdd = baseRdd(sqlContext, csvOptions, paths)
    val firstLine = findFirstLine(csvOptions, rdd)
    val firstRow = new LineCsvReader(csvOptions).parseLine(firstLine)

    val header = if (csvOptions.headerFlag) {
      firstRow
    } else {
      firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
    }

    val parsedRdd = tokenRdd(sqlContext, csvOptions, header, paths)
    val schema = if (csvOptions.inferSchemaFlag) {
      CSVInferSchema.infer(parsedRdd, header, csvOptions.nullValue)
    } else {
      // By default fields are assumed to be StringType
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

  /**
   * This supports to eliminate unneeded columns before producing an RDD
   * containing all of its tuples as Row objects. This reads all the tokens of each line
   * and then drop unneeded tokens without casting and type-checking by mapping
   * both the indices produced by `requiredColumns` and the ones of tokens.
   */
  override def buildInternalScan(
      sqlContext: SQLContext,
      dataSchema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      bucketSet: Option[BitSet],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration],
      options: Map[String, String]): RDD[InternalRow] = {
    // TODO: Filter before calling buildInternalScan.
    val csvFiles = inputFiles.filterNot(_.getPath.getName startsWith "_")

    val csvOptions = new CSVOptions(options)
    val pathsString = csvFiles.map(_.getPath.toUri.toString)
    val header = dataSchema.fields.map(_.name)
    val tokenizedRdd = tokenRdd(sqlContext, csvOptions, header, pathsString)
    val external = CSVRelation.parseCsv(
      tokenizedRdd, dataSchema, requiredColumns, csvFiles, sqlContext, csvOptions)

    // TODO: Generate InternalRow in parseCsv
    val outputSchema = StructType(requiredColumns.map(c => dataSchema.find(_.name == c).get))
    val encoder = RowEncoder(outputSchema)
    external.map(encoder.toRow)
  }


  private def baseRdd(
      sqlContext: SQLContext,
      options: CSVOptions,
      inputPaths: Seq[String]): RDD[String] = {
    readText(sqlContext, options, inputPaths.mkString(","))
  }

  private def tokenRdd(
      sqlContext: SQLContext,
      options: CSVOptions,
      header: Array[String],
      inputPaths: Seq[String]): RDD[Array[String]] = {
    val rdd = baseRdd(sqlContext, options, inputPaths)
    // Make sure firstLine is materialized before sending to executors
    val firstLine = if (options.headerFlag) findFirstLine(options, rdd) else null
    CSVRelation.univocityTokenizer(rdd, header, firstLine, options)
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private def findFirstLine(options: CSVOptions, rdd: RDD[String]): String = {
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

  private def readText(
      sqlContext: SQLContext,
      options: CSVOptions,
      location: String): RDD[String] = {
    if (Charset.forName(options.charset) == Charset.forName("UTF-8")) {
      sqlContext.sparkContext.textFile(location)
    } else {
      val charset = options.charset
      sqlContext.sparkContext
        .hadoopFile[LongWritable, Text, TextInputFormat](location)
        .mapPartitions(_.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)))
    }
  }
}
