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
import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, StringType}
import org.apache.spark.unsafe.types.UTF8String


// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    """
    _FUNC_(str) - Translates a string into Unicode encoding. The Unicode Standard is a text encoding standard maintained by the Unicode Consortium designed to support the use of text written in all of the world's major writing systems.
  """,
  arguments =
    """
    Arguments:
      str - a string expression to be translated
  """,
  examples =
    """
    Examples:
      > SELECT _FUNC_('apache');
       \u0061\u0070\u0061\u0063\u0068\u0065
  """,
  since = "3.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class UnicodeEncode(child: Expression)
  extends RuntimeReplaceable with UnaryLike[Expression] with ImplicitCastInputTypes {

  override def replacement: Expression =
    StaticInvoke(
      UnicodeCodec.getClass,
      StringType,
      "encode",
      Seq(child, Literal("UTF-8")),
      Seq(StringType, StringType))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "unicode_encode"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    """
    _FUNC_(str) - Decodes a `str` in Unicode encoding format.  The Unicode Standard is a text encoding standard maintained by the Unicode Consortium designed to support the use of text written in all of the world's major writing systems.
  """,
  arguments =
    """
    Arguments:
      * str - a string expression to decode
  """,
  examples =
    """
    Examples:
      > SELECT _FUNC_('\u0061\u0070\u0061\u0063\u0068\u0065');
       apache
  """,
  since = "3.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class UnicodeDecode(child: Expression)
  extends RuntimeReplaceable with UnaryLike[Expression] with ImplicitCastInputTypes {

  override def replacement: Expression =
    StaticInvoke(
      UnicodeCodec.getClass,
      StringType,
      "decode",
      Seq(child, Literal("UTF-8")),
      Seq(StringType, StringType))

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "unicode_decode"
}

object UnicodeCodec {
  def encode(str: String): UTF8String = {
    val utfBytes = str.toCharArray()
    val unicodeBytes: StringBuilder = new StringBuilder();
    for (utfByte <- utfBytes) {
      var hexB = Integer.toHexString(utfByte);
      if (hexB.length() <= 2) {
        hexB = "00" + hexB;
      }
      unicodeBytes.append("\\u").append(hexB);
    }
    UTF8String.fromString(unicodeBytes.toString())

  }

  def decode(str: String): String = {
    val pattern: Pattern = Pattern.compile("(\\\\u(\\w{4}))");
    val matcher: Matcher = pattern.matcher(str);
    var ch: Char = 0
    var src: String = str
    while (matcher.find()) {
      ch = Integer.parseInt(matcher.group(2), 16).toChar
      src = str.replace(matcher.group(1), ch + "");
    }
    str;
  }

}

