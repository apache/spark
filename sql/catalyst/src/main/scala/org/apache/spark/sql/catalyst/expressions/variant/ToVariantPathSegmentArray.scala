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

package org.apache.spark.sql.catalyst.expressions.variant

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RuntimeReplaceable, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, ObjectType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class ToVariantPathSegmentArray(path: Expression, funcName: String)
  extends UnaryExpression
  with RuntimeReplaceable {

  override def foldable: Boolean = path.foldable
  override def nullIntolerant: Boolean = true
  override def child: Expression = path
  override def prettyName: String = "to_variant_path_segment_array"

  override def dataType: DataType = ObjectType(classOf[Array[VariantPathSegment]])

  override def replacement: Expression =
    StaticInvoke(
      VariantPathParser.getClass,
      dataType,
      "parse",
      Seq(path, Literal(UTF8String.fromString(funcName), StringType)),
      Seq(path.dataType, StringType)
    )

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(path = newChild)
}

object VariantPathParser extends RegexParsers {
  private def root: Parser[Char] = '$'

  // Parse index segment like `[123]`.
  private def index: Parser[VariantPathSegment] =
    for {
      index <- '[' ~> "\\d+".r <~ ']'
    } yield {
      ArrayExtraction(index.toInt)
    }

  // Parse key segment like `.name`, `['name']`, or `["name"]`.
  private def key: Parser[VariantPathSegment] =
    for {
      key <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\'\\?]+".r <~ "']" |
        "[\"" ~> "[^\\\"\\?]+".r <~ "\"]"
    } yield {
      ObjectExtraction(key)
    }

  private val parser: Parser[List[VariantPathSegment]] = phrase(root ~> rep(key | index))

  def parse(str: UTF8String, funcName: UTF8String): Array[VariantPathSegment] = {
    if (str == null) return null
    parseAll(parser, str.toString) match {
      case Success(result, _) => result.toArray
      case _ => throw QueryExecutionErrors.invalidVariantGetPath(str.toString, funcName.toString)
    }
  }
}
