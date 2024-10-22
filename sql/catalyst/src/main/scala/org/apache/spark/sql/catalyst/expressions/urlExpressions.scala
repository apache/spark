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

package org.apache.spark.sql.catalyst.expressions

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.url.ParseUrlEvaluator
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, ObjectType}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Translates a string into 'application/x-www-form-urlencoded' format using a specific encoding scheme.
  """,
  arguments = """
    Arguments:
      str - a string expression to be translated
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('https://spark.apache.org');
       https%3A%2F%2Fspark.apache.org
  """,
  since = "3.4.0",
  group = "url_funcs")
// scalastyle:on line.size.limit
case class UrlEncode(child: Expression)
  extends RuntimeReplaceable with UnaryLike[Expression] with ImplicitCastInputTypes {

  override lazy val replacement: Expression =
    StaticInvoke(
      UrlCodec.getClass,
      SQLConf.get.defaultStringType,
      "encode",
      Seq(child),
      Seq(StringTypeWithCollation))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeWithCollation)

  override def prettyName: String = "url_encode"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding scheme.
  """,
  arguments = """
    Arguments:
      * str - a string expression to decode
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('https%3A%2F%2Fspark.apache.org');
       https://spark.apache.org
  """,
  since = "3.4.0",
  group = "url_funcs")
// scalastyle:on line.size.limit
case class UrlDecode(child: Expression, failOnError: Boolean = true)
  extends RuntimeReplaceable with UnaryLike[Expression] with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, true)

  override lazy val replacement: Expression =
    StaticInvoke(
      UrlCodec.getClass,
      SQLConf.get.defaultStringType,
      "decode",
      Seq(child, Literal(failOnError)),
      Seq(StringTypeWithCollation, BooleanType))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeWithCollation)

  override def prettyName: String = "url_decode"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str) - This is a special version of `url_decode` that performs the same operation, but returns a NULL value instead of raising an error if the decoding cannot be performed.
  """,
  arguments = """
    Arguments:
      * str - a string expression to decode
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('https%3A%2F%2Fspark.apache.org');
       https://spark.apache.org
  """,
  since = "4.0.0",
  group = "url_funcs")
// scalastyle:on line.size.limit
case class TryUrlDecode(expr: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(expr: Expression) = this(expr, UrlDecode(expr, false))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }

  override def parameters: Seq[Expression] = Seq(expr)

  override def prettyName: String = "try_url_decode"
}

object UrlCodec {
  def encode(src: UTF8String): UTF8String = {
    UTF8String.fromString(URLEncoder.encode(src.toString, StandardCharsets.UTF_8))
  }

  def decode(src: UTF8String, failOnError: Boolean): UTF8String = {
    try {
      UTF8String.fromString(URLDecoder.decode(src.toString, StandardCharsets.UTF_8))
    } catch {
      case e: IllegalArgumentException if failOnError =>
        throw QueryExecutionErrors.illegalUrlError(src, e)
      case _: IllegalArgumentException => null
    }
  }
}

/**
 * Extracts a part from a URL
 */
@ExpressionDescription(
  usage = "_FUNC_(url, partToExtract[, key]) - Extracts a part from a URL.",
  examples = """
    Examples:
      > SELECT _FUNC_('http://spark.apache.org/path?query=1', 'HOST');
       spark.apache.org
      > SELECT _FUNC_('http://spark.apache.org/path?query=1', 'QUERY');
       query=1
      > SELECT _FUNC_('http://spark.apache.org/path?query=1', 'QUERY', 'query');
       1
  """,
  since = "2.0.0",
  group = "url_funcs")
case class ParseUrl(children: Seq[Expression], failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends Expression with ExpectsInputTypes with RuntimeReplaceable {
  def this(children: Seq[Expression]) = this(children, SQLConf.get.ansiEnabled)

  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] =
    Seq.fill(children.size)(StringTypeWithCollation)
  override def dataType: DataType = SQLConf.get.defaultStringType
  override def prettyName: String = "parse_url"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size > 3 || children.size < 2) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("[2, 3]"), children.length
      )
    } else {
      super[ExpectsInputTypes].checkInputDataTypes()
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ParseUrl =
    copy(children = newChildren)

  // If the url is a constant, cache the URL object so that we don't need to convert url
  // from UTF8String to String to URL for every row.
  @transient private lazy val cachedUrl = children.head match {
    case Literal(url: UTF8String, _) if url ne null =>
      () => ParseUrlEvaluator.getUrl(url, failOnError)
    case _ => null
  }

  // If the partToExtract is a constant, cache the Extract part function so that we don't need
  // to check the partToExtract for every row.
  @transient private lazy val cachedExtractPartFunc = children(1) match {
    case Literal(part: UTF8String, _) => ParseUrlEvaluator.getExtractPartFunc(part)
    case _ => null
  }

  // If the key is a constant, cache the Pattern object so that we don't need to convert key
  // from UTF8String to String to StringBuilder to String to Pattern for every row.
  @transient private lazy val cachedPattern = () => children(2) match {
    case Literal(key: UTF8String, _) if key ne null => ParseUrlEvaluator.getPattern(key)
    case _ => null
  }

  @transient
  private lazy val evaluator: ParseUrlEvaluator = ParseUrlEvaluator(
    cachedUrl, cachedExtractPartFunc, cachedPattern, failOnError)

  override def replacement: Expression = Invoke(
    Literal.create(evaluator, ObjectType(classOf[ParseUrlEvaluator])),
    "evaluate",
    dataType,
    children,
    children.map(_.dataType))
}
