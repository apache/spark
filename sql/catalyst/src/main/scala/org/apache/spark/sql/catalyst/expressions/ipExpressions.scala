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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utility methods for IPv4 address processing.
 * All methods use pure integer arithmetic - no JNI, no java.net.InetAddress.
 */
object Ipv4Utils {

  /**
   * Convert IPv4 string to Long.
   * Returns None if the input is not a valid IPv4 address.
   *
   * Validation rules (Flink-compatible):
   * - Accepts 1-4 dot-separated parts (short-form IP support)
   * - Each part must be an integer 0-255
   * - Leading zeros are parsed as decimal (not octal), matching MySQL/Flink behavior
   * - No non-digit characters (except '.')
   * - No empty segments, no leading/trailing dots, no whitespace
   * - No negative numbers
   *
   * Short-form assembly (Flink-compatible):
   * - 1 part:  n        -> 0.0.0.n  (value = n)
   * - 2 parts: a.b      -> a.0.0.b  (value = (a<<24) | b)
   * - 3 parts: a.b.c    -> a.b.0.c  (value = (a<<24) | (b<<16) | c)
   * - 4 parts: a.b.c.d  -> direct mapping (value = (a<<24) | (b<<16) | (c<<8) | d)
   */
  def ipv4ToLong(ipv4: String): Option[Long] = {
    if (ipv4 == null || ipv4.isEmpty) return None
    // Check for any whitespace characters - reject immediately
    var i = 0
    while (i < ipv4.length) {
      val c = ipv4.charAt(i)
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') return None
      if (c != '.' && (c < '0' || c > '9')) return None
      i += 1
    }
    // Use split with -1 limit to preserve trailing empty strings.
    // This ensures "192.168.1." -> ["192","168","1",""] (4 parts, last empty)
    val parts = ipv4.split("\\.", -1)
    // Check for leading dot (split gives empty first element)
    if (parts.nonEmpty && parts(0).isEmpty) return None
    val numParts = parts.length
    if (numParts < 1 || numParts > 4) return None
    val segments = new Array[Long](numParts)
    i = 0
    while (i < numParts) {
      val part = parts(i)
      // Empty segment check (e.g., "192..1" or trailing "192.168.1.")
      if (part.isEmpty) return None
      try {
        val value = part.toLong
        // Check for negative or out-of-range
        if (value < 0 || value > 255) return None
        // Leading zeros are accepted and parsed as decimal,
        // matching MySQL and Flink behavior (not octal like POSIX inet_aton).
        segments(i) = value
      } catch {
        case _: NumberFormatException => return None
      }
      i += 1
    }
    val result = numParts match {
      case 1 => segments(0)
      case 2 => (segments(0) << 24) | segments(1)
      case 3 => (segments(0) << 24) | (segments(1) << 16) | segments(2)
      case 4 =>
        (segments(0) << 24) | (segments(1) << 16) | (segments(2) << 8) | segments(3)
    }
    Some(result)
  }

  /**
   * Convert Long to IPv4 dotted-decimal string.
   * Returns None if value is out of range (<0 or >0xFFFFFFFF).
   */
  def longToIpv4(value: Long): Option[String] = {
    if (value < 0 || value > 0xFFFFFFFFL) return None
    val a = (value >> 24) & 0xFF
    val b = (value >> 16) & 0xFF
    val c = (value >> 8) & 0xFF
    val d = value & 0xFF
    Some(s"$a.$b.$c.$d")
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Converts an IPv4 address string to a 32-bit integer.
    Returns null if the input is invalid (non-ANSI mode).
  """,
  arguments = """
    Arguments:
      * str - a string expression containing an IPv4 address in dotted-decimal notation
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('192.168.1.1');
       3232235777
      > SELECT _FUNC_('0.0.0.0');
       0
      > SELECT _FUNC_('255.255.255.255');
       4294967295
      > SELECT _FUNC_('127.1');
       2130706433
  """,
  note = """
    In ANSI mode, an invalid IPv4 address string raises an error.
    Use try_inet_aton to return NULL for invalid inputs instead.
  """,
  since = "4.3.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class InetAton(child: Expression, failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, SQLConf.get.ansiEnabled)

  override def prettyName: String = "inet_aton"
  override def nullIntolerant: Boolean = true
  override def nullable: Boolean = true
  // Override foldable to false: failOnError defaults to SQLConf.get.ansiEnabled,
  // so the expression is context-dependent. ConstantFolding must not evaluate it
  // during planning (e.g. doc generation), as it would throw for invalid inputs
  // when failOnError is true. This trades a minor constant-folding opportunity
  // (valid literal inputs won't be pre-evaluated) for safety.
  override def foldable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))
  override def dataType: DataType = LongType

  override protected def nullSafeEval(input: Any): Any = {
    val result = Ipv4Utils.ipv4ToLong(input.asInstanceOf[UTF8String].toString)
    if (result.isDefined) result.get
    else if (failOnError) {
      throw QueryExecutionErrors.invalidIpv4AddressError(input.asInstanceOf[UTF8String])
    } else null
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val utils = Ipv4Utils.getClass.getName.stripSuffix("$")
    val errors = QueryExecutionErrors.getClass.getName.stripSuffix("$")
    val resultVar = ctx.freshName("result")
    nullSafeCodeGen(ctx, ev, input => {
      val failExpr = if (failOnError) "true" else "false"
      s"""
        |scala.Option $resultVar = $utils.ipv4ToLong($input.toString());
        |if ($resultVar.isDefined()) {
        |  ${ev.value} = ((java.lang.Long) $resultVar.get()).longValue();
        |} else if ($failExpr) {
        |  throw $errors.invalidIpv4AddressError($input);
        |} else {
        |  ${ev.isNull} = true;
        |}
      """.stripMargin
    })
  }

  override protected def withNewChildInternal(newChild: Expression): InetAton =
    copy(child = newChild, failOnError = failOnError)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(value) - Converts a 32-bit integer to an IPv4 address string in dotted-decimal notation.
    Returns null if the value is out of valid IPv4 range (<0 or >4294967295) in non-ANSI mode.
  """,
  arguments = """
    Arguments:
      * value - a long expression representing a 32-bit IPv4 integer
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(3232235777);
       192.168.1.1
      > SELECT _FUNC_(0);
       0.0.0.0
  """,
  note = """
    In ANSI mode, a value outside the valid IPv4 range (0 to 4294967295) raises an error.
  """,
  since = "4.3.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class InetNtoa(child: Expression, failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression
  with ImplicitCastInputTypes
  with DefaultStringProducingExpression {

  def this(child: Expression) = this(child, SQLConf.get.ansiEnabled)

  override def prettyName: String = "inet_ntoa"
  override def nullIntolerant: Boolean = true
  override def nullable: Boolean = true
  // Override foldable to false: same reason as InetAton.
  override def foldable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override protected def nullSafeEval(input: Any): Any = {
    val result = Ipv4Utils.longToIpv4(input.asInstanceOf[Long])
    if (result.isDefined) UTF8String.fromString(result.get)
    else if (failOnError) {
      throw QueryExecutionErrors.invalidIpv4LongError(input.asInstanceOf[Long])
    } else null
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val utils = Ipv4Utils.getClass.getName.stripSuffix("$")
    val errors = QueryExecutionErrors.getClass.getName.stripSuffix("$")
    val resultVar = ctx.freshName("result")
    nullSafeCodeGen(ctx, ev, input => {
      val failExpr = if (failOnError) "true" else "false"
      s"""
        |scala.Option $resultVar = $utils.longToIpv4($input);
        |if ($resultVar.isDefined()) {
        |  ${ev.value} = UTF8String.fromString((String) $resultVar.get());
        |} else if ($failExpr) {
        |  throw $errors.invalidIpv4LongError($input);
        |} else {
        |  ${ev.isNull} = true;
        |}
      """.stripMargin
    })
  }

  override protected def withNewChildInternal(newChild: Expression): InetNtoa =
    copy(child = newChild, failOnError = failOnError)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str) - This is a special version of `inet_aton` that performs the same operation, but
    returns NULL instead of raising an error if the input is not a valid IPv4 address.
  """,
  arguments = """
    Arguments:
      * str - a string expression containing a potential IPv4 address
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('192.168.1.1');
       3232235777
      > SELECT _FUNC_('invalid');
       NULL
  """,
  since = "4.3.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class TryInetAton(expr: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(expr: Expression) = this(expr, InetAton(expr, false))

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(replacement = newChild)

  override def parameters: Seq[Expression] = Seq(expr)

  override def prettyName: String = "try_inet_aton"
}
