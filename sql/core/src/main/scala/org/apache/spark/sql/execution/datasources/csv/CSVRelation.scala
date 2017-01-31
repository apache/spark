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

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.PartitionedFile

object CSVRelation extends Logging {

  def univocityTokenizer(
      file: Dataset[String],
      firstLine: String,
      params: CSVOptions): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    val commentPrefix = params.comment.toString
    file.rdd.mapPartitions { iter =>
      val parser = new CsvParser(params.asParserSettings)
      val filteredIter = iter.filter { line =>
        line.trim.nonEmpty && !line.startsWith(commentPrefix)
      }
      if (params.headerFlag) {
        filteredIter.filterNot(_ == firstLine).map { item =>
          parser.parseLine(item)
        }
      } else {
        filteredIter.map { item =>
          parser.parseLine(item)
        }
      }
    }
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
