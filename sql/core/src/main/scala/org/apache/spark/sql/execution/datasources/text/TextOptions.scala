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

import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs}

/**
 * Options for the Text data source.
 */
class TextOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends FileSourceOptions(parameters) {

  import TextOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  private def getString(paramName: TextOptions.Value): Option[String] = {
    parameters.get(paramName.toString)
  }

  /**
   * Compression codec to use.
   */
  val compressionCodec = getString(COMPRESSION).map(CompressionCodecs.getCodecClassName)

  /**
   * wholetext - If true, read a file as a single row and not split by "\n".
   */
  val wholeText = getString(WHOLETEXT).getOrElse("false").toBoolean

  val encoding: Option[String] = getString(ENCODING)

  val lineSeparator: Option[String] = getString(LINE_SEPARATOR).map { lineSep =>
    require(lineSep.nonEmpty, s"'$LINE_SEPARATOR' cannot be an empty string.")

    lineSep
  }

  // Note that the option 'lineSep' uses a different default value in read and write.
  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.map { lineSep =>
    lineSep.getBytes(encoding.map(Charset.forName(_)).getOrElse(StandardCharsets.UTF_8))
  }
  val lineSeparatorInWrite: Array[Byte] =
    lineSeparatorInRead.getOrElse("\n".getBytes(StandardCharsets.UTF_8))
}

object TextOptions extends Enumeration {
  val COMPRESSION = Value("compression")
  val WHOLETEXT = Value("wholetext")
  val ENCODING = Value("encoding")
  val LINE_SEPARATOR = Value("lineSep")
}
