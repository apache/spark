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

private[csv] object CSVUtils {
  /**
   * Filter ignorable rows for CSV (lines empty and starting with `comment`)
   */
  def filterCommentAndEmpty(iter: Iterator[String], options: CSVOptions): Iterator[String] = {
    if (options.isCommentSet) {
      val commentPrefix = options.comment.toString
      iter.filter { line =>
        line.trim.nonEmpty && !line.startsWith(commentPrefix)
      }
    } else {
      iter.filter { line =>
        line.trim.nonEmpty
      }
    }
  }

  /**
   * Drop header line so that only data can remain.
   */
  def dropHeaderLine(iter: Iterator[String], options: CSVOptions): Unit = {
    if (options.headerFlag) {
      val nonEmptyLines = if (options.isCommentSet) {
        val commentPrefix = options.comment.toString
        iter.dropWhile { line =>
          line.trim.isEmpty || line.trim.startsWith(commentPrefix)
        }
      } else {
        iter.dropWhile(_.trim.isEmpty)
      }

      if (nonEmptyLines.hasNext) nonEmptyLines.drop(1)
    }
  }
}
