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

package org.apache.spark.sql.execution.datasources.text

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs}

/**
 * Options for the Text data source.
 */
private[text] class TextOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  import TextOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * Compression codec to use.
   */
  val compressionCodec = parameters.get(COMPRESSION).map(CompressionCodecs.getCodecClassName)

  /**
   * wholetext - If true, read a file as a single row and not split by "\n".
   */
  val wholeText = parameters.getOrElse(WHOLETEXT, "false").toBoolean

  val encoding: Option[String] = parameters.get(ENCODING)

  /**
   * A string between two consecutive text lines.
   * Note: the option 'lineSep' uses a different default value in read and write.
   */
  val lineSeparator: Option[String] = parameters.get(LINE_SEPARATOR).map { lineSep =>
    require(lineSep.nonEmpty, s"'$LINE_SEPARATOR' cannot be an empty string.")

    lineSep
  }

  /**
   * A sequence of bytes between two consecutive text lines in read.
   * Format of the `lineSep` option is:
   *   selector (1 char) + separator spec (any length) | sequence of chars
   *
   * Currently the following selectors are supported:
   * - 'x' + sequence of bytes in hexadecimal format. For example: "x0a 0d".
   *   Hex pairs can be separated by any chars different from 0-9,A-F,a-f
   * - '\' - reserved for a sequence of control chars like "\r\n"
   *         and unicode escape like "\u000D\u000A"
   * - 'r' and '/' - reserved for future use
   */
  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.collect {
    case hexs if hexs.startsWith("x") =>
      hexs.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray
        .map(Integer.parseInt(_, 16).toByte)
    case reserved if reserved.startsWith("r") || reserved.startsWith("/") =>
      throw new NotImplementedError(s"The $reserved selector has not supported yet")
    case lineSep =>
      lineSep.getBytes(encoding.map(Charset.forName(_)).getOrElse(StandardCharsets.UTF_8))
  }
  val lineSeparatorInWrite: Array[Byte] =
    lineSeparatorInRead.getOrElse("\n".getBytes(StandardCharsets.UTF_8))
}

private[datasources] object TextOptions {
  val COMPRESSION = "compression"
  val WHOLETEXT = "wholetext"
  val ENCODING = "encoding"
  val LINE_SEPARATOR = "lineSep"
}
