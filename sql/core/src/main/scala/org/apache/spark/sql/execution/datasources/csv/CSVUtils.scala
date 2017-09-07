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

  /**
   * Drop header line so that only data can remain.
   * This is similar with `filterHeaderLine` above and currently being used in CSV reading path.
   */
  def dropHeaderLine(iter: Iterator[String], options: CSVOptions): Iterator[String] = {
    val nonEmptyLines = if (options.isCommentSet) {
      val commentPrefix = options.comment.toString
      iter.dropWhile { line =>
        line.trim.isEmpty || line.trim.startsWith(commentPrefix)
      }
    } else {
      iter.dropWhile(_.trim.isEmpty)
    }

    if (nonEmptyLines.hasNext) nonEmptyLines.drop(1)
    iter
  }

  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   */
  @throws[IllegalArgumentException]
  def toChar(str: String): Char = {
    if (str.charAt(0) == '\\') {
      str.charAt(1)
      match {
        case 't' => '\t'
        case 'r' => '\r'
        case 'b' => '\b'
        case 'f' => '\f'
        case '\"' => '\"' // In case user changes quote char and uses \" as delimiter in options
        case '\'' => '\''
        case 'u' if str == """\u0000""" => '\u0000'
        case _ =>
          throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
      }
    } else if (str.length == 1) {
      str.charAt(0)
    } else {
      throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
    }
  }

  /**
   * Verify if the schema is supported in CSV datasource.
   */
  def verifySchema(schema: StructType): Unit = {
    def verifyType(dataType: DataType): Unit = dataType match {
      case ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | BooleanType | _: DecimalType | TimestampType |
           DateType | StringType =>

      case udt: UserDefinedType[_] => verifyType(udt.sqlType)

      case _ =>
        throw new UnsupportedOperationException(
          s"CSV data source does not support ${dataType.simpleString} data type.")
    }

    schema.foreach(field => verifyType(field.dataType))
  }

}
