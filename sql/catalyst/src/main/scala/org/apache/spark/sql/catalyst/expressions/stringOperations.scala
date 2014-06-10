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

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.BooleanType


trait StringRegexExpression {
  self: BinaryExpression =>

  type EvaluatedType = Any

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

  def nullable: Boolean = true
  def dataType: DataType = BooleanType

  // try cache the pattern for Literal
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value)
    case _ => null
  }

  protected def compile(str: String): Pattern = if(str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  protected def pattern(str: String) = if(cache == null) compile(str) else cache

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == null) {
      null
    } else {
      val r = right.eval(input)
      if(r == null) {
        null
      } else {
        val regex = pattern(r.asInstanceOf[String])
        if(regex == null) {
          null
        } else {
          matches(regex, l.asInstanceOf[String])
        }
      }
    }
  }
}

trait CaseConversionExpression {
  self: UnaryExpression =>

  type EvaluatedType = Any

  def convert(v: String): String

  override def foldable: Boolean = child.foldable
  def nullable: Boolean = child.nullable
  def dataType: DataType = StringType

  override def eval(input: Row): Any = {
    val evaluated = child.eval(input)
    if (evaluated == null) {
      null
    } else {
      convert(evaluated.toString)
    }
  }
}

/**
 * Simple RegEx pattern matching function
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  def symbol = "LIKE"

  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  override def escape(v: String) = {
    val sb = new StringBuilder()
    var i = 0;
    while (i < v.length) {
      // Make a special case for "\\_" and "\\%"
      val n = v.charAt(i);
      if (n == '\\' && i + 1 < v.length && (v.charAt(i + 1) == '_' || v.charAt(i + 1) == '%')) {
        sb.append(v.charAt(i + 1))
        i += 1
      } else {
        if (n == '_') {
          sb.append(".");
        } else if (n == '%') {
          sb.append(".*");
        } else {
          sb.append(Pattern.quote(Character.toString(n)));
        }
      }

      i += 1
    }

    sb.toString()
  }

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()
}

case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  def symbol = "RLIKE"
  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
}

/**
 * A function that converts the characters of a string to uppercase.
 */
case class Upper(child: Expression) extends UnaryExpression with CaseConversionExpression {
  
  override def convert(v: String): String = v.toUpperCase()

  override def toString() = s"Upper($child)"
}

/**
 * A function that converts the characters of a string to lowercase.
 */
case class Lower(child: Expression) extends UnaryExpression with CaseConversionExpression {
  
  override def convert(v: String): String = v.toLowerCase()

  override def toString() = s"Lower($child)"
}
