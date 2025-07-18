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
package org.apache.spark.util

import java.util.HexFormat
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.util.ArrayImplicits._

private[spark] trait SparkStringUtils {
  private final lazy val SPACE_DELIMITED_UPPERCASE_HEX =
    HexFormat.of().withDelimiter(" ").withUpperCase()

  /**
   * Returns a pretty string of the byte array which prints each byte as a hex digit and add
   * spaces between them. For example, [1A C0].
   */
  def getHexString(bytes: Array[Byte]): String = {
    s"[${SPACE_DELIMITED_UPPERCASE_HEX.formatHex(bytes)}]"
  }

  def sideBySide(left: String, right: String): Seq[String] = {
    sideBySide(left.split("\n").toImmutableArraySeq, right.split("\n").toImmutableArraySeq)
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map { case (l, r) =>
      (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    import org.apache.spark.util.ArrayImplicits._
    str.split(",").map(_.trim()).filter(_.nonEmpty).toImmutableArraySeq
  }
}

private[spark] object SparkStringUtils extends SparkStringUtils with Logging {

  /** Whether we have warned about plan string truncation yet. */
  private val truncationWarningPrinted = new AtomicBoolean(false)

  /**
   * Format a sequence with semantics similar to calling .mkString(). Any elements beyond
   * `maxFields` will be dropped and replaced by a "... N more fields" placeholder.
   *
   * @return
   *   the trimmed and formatted string.
   */
  def truncatedString[T](
      seq: Seq[T],
      start: String,
      sep: String,
      end: String,
      maxFields: Int,
      customToString: Option[T => String] = None): String = {
    if (seq.length > maxFields) {
      if (truncationWarningPrinted.compareAndSet(false, true)) {
        logWarning(
          "Truncated the string representation of a plan since it was too large. This " +
            s"behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.")
      }
      val numFields = math.max(0, maxFields)
      val restNum = seq.length - numFields
      val ending = (if (numFields == 0) "" else sep) +
        (if (restNum == 0) "" else s"... $restNum more fields") + end
      if (customToString.isDefined) {
        seq.take(numFields).map(customToString.get).mkString(start, sep, ending)
      } else {
        seq.take(numFields).mkString(start, sep, ending)
      }
    } else {
      if (customToString.isDefined) {
        seq.map(customToString.get).mkString(start, sep, end)
      } else {
        seq.mkString(start, sep, end)
      }
    }
  }

  /** Shorthand for calling truncatedString() without start or end strings. */
  def truncatedString[T](seq: Seq[T], sep: String, maxFields: Int): String = {
    truncatedString(seq, "", sep, "", maxFields)
  }
}
