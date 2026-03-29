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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.csv.CSVExprUtils
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.functions._

object CSVUtils {
  /**
   * Filter ignorable rows for CSV dataset (lines empty and starting with `comment`).
   * This is currently being used in CSV schema inference.
   */
  def filterCommentAndEmpty(lines: Dataset[String], options: CSVOptions): Dataset[String] = {
    // Note that this was separately made by SPARK-18362. Logically, this should be the same
    // with the one below, `filterCommentAndEmpty` but execution path is different. One of them
    // might have to be removed in the near future if possible.
    import lines.sparkSession.implicits._
    val aliased = lines.toDF("value")
    val nonEmptyLines = aliased.filter(length(trim($"value")) > 0)
    if (options.isCommentSet) {
      nonEmptyLines.filter(!$"value".startsWith(options.comment.toString)).as[String]
    } else {
      nonEmptyLines.as[String]
    }
  }

  /**
   * Skip the given first line so that only data can remain in a dataset.
   * This is similar with `dropHeaderLine` below and currently being used in CSV schema inference.
   */
  def filterHeaderLine(
       iter: Iterator[String],
       firstLine: String,
       options: CSVOptions): Iterator[String] = {
    // Note that unlike actual CSV reading path, it simply filters the given first line. Therefore,
    // this skips the line same with the header if exists. One of them might have to be removed
    // in the near future if possible.
    if (options.headerFlag) {
      iter.filterNot(_ == firstLine)
    } else {
      iter
    }
  }

  /**
   * Generates a header from the given row which is null-safe and duplicate-safe.
   */
  def makeSafeHeader(
      row: Array[String],
      caseSensitive: Boolean,
      options: CSVOptions): Array[String] = {
    if (options.headerFlag) {
      val duplicates = {
        val headerNames = row.filter(_ != null)
          // scalastyle:off caselocale
          .map(name => if (caseSensitive) name else name.toLowerCase)
        // scalastyle:on caselocale
        headerNames.diff(headerNames.distinct).distinct
      }

      row.zipWithIndex.map { case (value, index) =>
        if (value == null || value.isEmpty || value == options.nullValue) {
          // When there are empty strings or the values set in `nullValue`, put the
          // index as the suffix.
          s"_c$index"
          // scalastyle:off caselocale
        } else if (!caseSensitive && duplicates.contains(value.toLowerCase)) {
          // scalastyle:on caselocale
          // When there are case-insensitive duplicates, put the index as the suffix.
          s"$value$index"
        } else if (duplicates.contains(value)) {
          // When there are duplicates, put the index as the suffix.
          s"$value$index"
        } else {
          value
        }
      }
    } else {
      row.zipWithIndex.map { case (_, index) =>
        // Uses default column names, "_c#" where # is its position of fields
        // when header option is disabled.
        s"_c$index"
      }
    }
  }

  /**
   * Sample CSV dataset as configured by `samplingRatio`.
   */
  def sample(csv: Dataset[String], options: CSVOptions): Dataset[String] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      csv
    } else {
      csv.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  /**
   * Sample CSV RDD as configured by `samplingRatio`.
   */
  def sample(csv: RDD[Array[String]], options: CSVOptions): RDD[Array[String]] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      csv
    } else {
      csv.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  def filterCommentAndEmpty(iter: Iterator[String], options: CSVOptions): Iterator[String] =
    CSVExprUtils.filterCommentAndEmpty(iter, options)

  def readHeaderLine(filePath: Path, options: CSVOptions, conf: Configuration): Option[String] = {
    val inputStream = CodecStreams.createInputStream(conf, filePath)
    try {
      val lines = new Iterator[String] {
        private val in = options.lineSeparatorInRead match {
          case Some(sep) => new LineReader(inputStream, sep)
          case _ => new LineReader(inputStream)
        }
        private val text = new Text()
        private var finished = false
        private var hasValue = false

        override def hasNext: Boolean = {
          if (!finished && !hasValue) {
            val bytesRead = in.readLine(text)
            finished = bytesRead == 0
            hasValue = !finished
          }
          !finished
        }

        override def next(): String = {
          if (!hasValue) {
            throw QueryExecutionErrors.endOfStreamError()
          }
          hasValue = false
          new String(text.getBytes, 0, text.getLength, options.charset)
        }
      }
      val filteredLines = CSVUtils.filterCommentAndEmpty(lines, options)
      filteredLines.buffered.headOption
    } finally {
      inputStream.close()
    }
  }
}
