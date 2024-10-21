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
package org.apache.spark.sql.catalyst.expressions.url

import java.net.{URI, URISyntaxException}
import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.types.UTF8String

case class ParseUrlEvaluator(
    children: Seq[Expression],
    failOnError: Boolean) {

  import ParseUrlEvaluator._

  // If the url is a constant, cache the URL object so that we don't need to convert url
  // from UTF8String to String to URL for every row.
  @transient private lazy val cachedUrl = children.head match {
    case Literal(url: UTF8String, _) if url ne null => getUrl(url, failOnError)
    case _ => null
  }

  // If the key is a constant, cache the Pattern object so that we don't need to convert key
  // from UTF8String to String to StringBuilder to String to Pattern for every row.
  @transient private lazy val cachedPattern = children(2) match {
    case Literal(key: UTF8String, _) if key ne null => getPattern(key)
    case _ => null
  }

  // If the partToExtract is a constant, cache the Extract part function so that we don't need
  // to check the partToExtract for every row.
  @transient private lazy val cachedExtractPartFunc = children(1) match {
    case Literal(part: UTF8String, _) => getExtractPartFunc(part)
    case _ => null
  }

  private def extractValueFromQuery(query: UTF8String, pattern: Pattern): UTF8String = {
    val m = pattern.matcher(query.toString)
    if (m.find()) {
      UTF8String.fromString(m.group(2))
    } else {
      null
    }
  }

  private def extractFromUrl(url: URI, partToExtract: UTF8String): UTF8String = {
    if (cachedExtractPartFunc ne null) {
      UTF8String.fromString(cachedExtractPartFunc.apply(url))
    } else {
      UTF8String.fromString(getExtractPartFunc(partToExtract).apply(url))
    }
  }

  private def parseUrlWithoutKey(url: UTF8String, partToExtract: UTF8String): UTF8String = {
    if (cachedUrl ne null) {
      extractFromUrl(cachedUrl, partToExtract)
    } else {
      val currentUrl = getUrl(url, failOnError)
      if (currentUrl ne null) {
        extractFromUrl(currentUrl, partToExtract)
      } else {
        null
      }
    }
  }

  private def getPattern(key: UTF8String): Pattern = {
    Pattern.compile(REGEXPREFIX + key.toString + REGEXSUBFIX)
  }

  private def getUrl(url: UTF8String, failOnError: Boolean): URI = {
    try {
      new URI(url.toString)
    } catch {
      case e: URISyntaxException if failOnError =>
        throw QueryExecutionErrors.invalidUrlError(url, e)
      case _: URISyntaxException => null
    }
  }

  private def getExtractPartFunc(partToExtract: UTF8String): URI => String = {

    // partToExtract match {
    //   case HOST => _.toURL().getHost
    //   case PATH => _.toURL().getPath
    //   case QUERY => _.toURL().getQuery
    //   case REF => _.toURL().getRef
    //   case PROTOCOL => _.toURL().getProtocol
    //   case FILE => _.toURL().getFile
    //   case AUTHORITY => _.toURL().getAuthority
    //   case USERINFO => _.toURL().getUserInfo
    //   case _ => (url: URI) => null
    // }

    partToExtract match {
      case HOST => _.getHost
      case PATH => _.getRawPath
      case QUERY => _.getRawQuery
      case REF => _.getRawFragment
      case PROTOCOL => _.getScheme
      case FILE =>
        (url: URI) =>
          if (url.getRawQuery ne null) {
            url.getRawPath + "?" + url.getRawQuery
          } else {
            url.getRawPath
          }
      case AUTHORITY => _.getRawAuthority
      case USERINFO => _.getRawUserInfo
      case _ => (url: URI) => null
    }
  }

  final def evaluate(url: UTF8String, path: UTF8String): Any = {
    if (url == null || path == null) return null
    parseUrlWithoutKey(url, path)
  }

  final def evaluate(url: UTF8String, path: UTF8String, key: UTF8String): Any = {
    if (url == null || path == null || key == null) return null
    if (path != QUERY) {
      return null
    }

    val query = parseUrlWithoutKey(url, path)
    if (query eq null) {
      return null
    }

    if (cachedPattern ne null) {
      extractValueFromQuery(query, cachedPattern)
    } else {
      extractValueFromQuery(query, getPattern(key))
    }
  }
}

object ParseUrlEvaluator {
  private val HOST = UTF8String.fromString("HOST")
  private val PATH = UTF8String.fromString("PATH")
  private val QUERY = UTF8String.fromString("QUERY")
  private val REF = UTF8String.fromString("REF")
  private val PROTOCOL = UTF8String.fromString("PROTOCOL")
  private val FILE = UTF8String.fromString("FILE")
  private val AUTHORITY = UTF8String.fromString("AUTHORITY")
  private val USERINFO = UTF8String.fromString("USERINFO")
  private val REGEXPREFIX = "(&|^)"
  private val REGEXSUBFIX = "=([^&]*)"
}
