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
 package org.apache.spark.sql.catalyst.util
 import java.nio.charset.{Charset, CharsetDecoder, CharsetEncoder, CodingErrorAction, IllegalCharsetNameException, UnsupportedCharsetException}
 import java.util.Locale

 import org.apache.spark.sql.errors.QueryExecutionErrors
 import org.apache.spark.sql.internal.SQLConf

private[sql] object CharsetProvider {

  final lazy val VALID_CHARSETS =
    Set("us-ascii", "iso-8859-1", "utf-8", "utf-16be", "utf-16le", "utf-16", "utf-32")

  def forName(
      charset: String,
      legacyCharsets: Boolean = SQLConf.get.legacyJavaCharsets,
      caller: String = ""): Charset = {
    val lowercasedCharset = charset.toLowerCase(Locale.ROOT)
    if (legacyCharsets || VALID_CHARSETS.contains(lowercasedCharset)) {
      try {
        Charset.forName(lowercasedCharset)
      } catch {
        case _: IllegalCharsetNameException |
             _: UnsupportedCharsetException |
             _: IllegalArgumentException =>
          throw QueryExecutionErrors.invalidCharsetError(caller, charset)
      }
    } else {
      throw QueryExecutionErrors.invalidCharsetError(caller, charset)
    }
  }

  def newEncoder(charset: String,
      legacyCharsets: Boolean,
      legacyErrorAction: Boolean,
      caller: String = "encode"): CharsetEncoder = {
    val codingErrorAction = if (legacyErrorAction) {
      CodingErrorAction.REPLACE
    } else {
      CodingErrorAction.REPORT
    }

    forName(charset, legacyCharsets, caller)
      .newEncoder()
      .onMalformedInput(codingErrorAction)
      .onUnmappableCharacter(codingErrorAction)
  }

  def newDecoder(charset: String,
      legacyCharsets: Boolean = SQLConf.get.legacyJavaCharsets,
      legacyErrorAction: Boolean = SQLConf.get.legacyCodingErrorAction,
      caller: String = "decode"): CharsetDecoder = {
    val codingErrorAction = if (legacyErrorAction) {
      CodingErrorAction.REPLACE
    } else {
      CodingErrorAction.REPORT
    }

    forName(charset, legacyCharsets, caller)
      .newDecoder()
      .onMalformedInput(codingErrorAction)
      .onUnmappableCharacter(codingErrorAction)
  }

}
