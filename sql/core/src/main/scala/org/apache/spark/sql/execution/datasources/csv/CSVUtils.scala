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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CSVUtils {
  /**
   * Filter ignorable rows for CSV dataset (lines empty and starting with `comment`).
   * This is currently being used in CSV schema inference.
   */
  def filterCommentAndEmpty(lines: Dataset[String], options: CSVOptions): Dataset[String] = {
    // Note that this was separately made by SPARK-18362. Logically, this should be the same
    // with the one below, `filterCommentAndEmpty` but execution path is different. One of them
    // might have to be removed in the near future if possible.
    import lines.sqlContext.implicits._
    val nonEmptyLines = lines.filter(length(trim($"value")) > 0)
    if (options.isCommentSet) {
      nonEmptyLines.filter(!$"value".startsWith(options.comment.toString))
    } else {
      nonEmptyLines
    }
  }

  /**
   * Filter ignorable rows for CSV iterator (lines empty and starting with `comment`).
   * This is currently being used in CSV reading path and CSV schema inference.
   */
  def filterCommentAndEmpty(iter: Iterator[String], options: CSVOptions): Iterator[String] = {
    iter.filter { line =>
      line.trim.nonEmpty && !line.startsWith(options.comment.toString)
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

  def skipComments(iter: Iterator[String], options: CSVOptions): Iterator[String] = {
    if (options.isCommentSet) {
      val commentPrefix = options.comment.toString
      iter.dropWhile { line =>
        line.trim.isEmpty || line.trim.startsWith(commentPrefix)
      }
    } else {
      iter.dropWhile(_.trim.isEmpty)
    }
  }

  /**
   * Extracts header and moves iterator forward so that only data remains in it
   */
  def extractHeader(iter: Iterator[String], options: CSVOptions): Option[String] = {
    val nonEmptyLines = skipComments(iter, options)
    if (nonEmptyLines.hasNext) {
      Some(nonEmptyLines.next())
    } else {
      None
    }
  }
  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   */
  @throws[IllegalArgumentException]
  def toChar(str: String): Char = {
    (str: Seq[Char]) match {
      case Seq() => throw new IllegalArgumentException("Delimiter cannot be empty string")
      case Seq('\\') => throw new IllegalArgumentException("Single backslash is prohibited." +
        " It has special meaning as beginning of an escape sequence." +
        " To get the backslash character, pass a string with two backslashes as the delimiter.")
      case Seq(c) => c
      case Seq('\\', 't') => '\t'
      case Seq('\\', 'r') => '\r'
      case Seq('\\', 'b') => '\b'
      case Seq('\\', 'f') => '\f'
      // In case user changes quote char and uses \" as delimiter in options
      case Seq('\\', '\"') => '\"'
      case Seq('\\', '\'') => '\''
      case Seq('\\', '\\') => '\\'
      case _ if str == """\u0000""" => '\u0000'
      case Seq('\\', _) =>
        throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
      case _ =>
        throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
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
}
