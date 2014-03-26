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

package org.apache.spark.sql
package catalyst
package expressions

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.BooleanType
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.errors.`package`.TreeNodeException


/**
 * Thrown when an invalid RegEx string is found.
 */
class InvalidRegExException[TreeType <: TreeNode[_]](tree: TreeType, reason: String) extends
  errors.TreeNodeException(tree, s"$reason", null)

trait StringRegexExpression {
  self: BinaryExpression =>

  type EvaluatedType = Any
  
  def escape(v: String): String
  def nullable: Boolean = true
  def dataType: DataType = BooleanType
  
  // try cache the pattern for Literal 
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value)
    case _ => null
  }
  
  protected def compile(str: Any): Pattern = str match {
    // TODO or let it be null if couldn't compile the regex?
    case x: String if(x != null) => Pattern.compile(escape(x))
    case x: String => null
    case _ => throw new InvalidRegExException(this, "$str can not be compiled to regex pattern")
  }
  
  protected def pattern(str: String) = if(cache == null) compile(str) else cache
  
  protected def filter: PartialFunction[(Row, (String, String)), Any] = {
    case (row, (null, r)) => { false }
    case (row, (l, null)) => { false }
    case (row, (l, r)) => { 
      val regex = pattern(r)
      if(regex == null) {
        null
      } else {
        regex.matcher(l).matches
      }
    }
  }

  override def apply(input: Row): Any = {
    val l = left.apply(input)
    if(l == null) {
      null
    } else {
      val r = right.apply(input)
      if(r == null) {
        null
      } else {
        filter.lift(input, (l.asInstanceOf[String], r.asInstanceOf[String])).get
      }
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
}

case class RLike(left: Expression, right: Expression) 
  extends BinaryExpression with StringRegexExpression {
  
  def symbol = "RLIKE"

  override def escape(v: String) = v
}
