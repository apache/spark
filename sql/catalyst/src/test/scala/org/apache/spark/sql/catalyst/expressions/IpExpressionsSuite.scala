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

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class IpExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  // ===========================================================================
  // Ipv4Utils Tests
  // ===========================================================================

  test("Ipv4Utils.ipv4ToLong - valid 4-segment IPv4 addresses") {
    assert(Ipv4Utils.ipv4ToLong("0.0.0.0").contains(0L))
    assert(Ipv4Utils.ipv4ToLong("255.255.255.255").contains(4294967295L))
    assert(Ipv4Utils.ipv4ToLong("192.168.1.1").contains(3232235777L))
    assert(Ipv4Utils.ipv4ToLong("127.0.0.1").contains(2130706433L))
    assert(Ipv4Utils.ipv4ToLong("10.0.0.1").contains(167772161L))
  }

  test("Ipv4Utils.ipv4ToLong - valid short-form IPv4 (Flink-compatible)") {
    // 3 segments: a.b.c -> a.b.0.c
    assert(Ipv4Utils.ipv4ToLong("192.168.1").contains(3232235521L))
    // 2 segments: a.b -> a.0.0.b
    assert(Ipv4Utils.ipv4ToLong("192.168").contains(3221225640L))
    // 1 segment: a -> 0.0.0.a
    assert(Ipv4Utils.ipv4ToLong("192").contains(192L))
    assert(Ipv4Utils.ipv4ToLong("0").contains(0L))
    assert(Ipv4Utils.ipv4ToLong("255").contains(255L))
  }

  test("Ipv4Utils.ipv4ToLong - invalid inputs") {
    // Null and empty
    assert(Ipv4Utils.ipv4ToLong(null).isEmpty)
    assert(Ipv4Utils.ipv4ToLong("").isEmpty)
    // Out of range
    assert(Ipv4Utils.ipv4ToLong("256.1.1.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("1.256.1.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("1.1.256.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("1.1.1.256").isEmpty)
    // Negative
    assert(Ipv4Utils.ipv4ToLong("-1.2.3.4").isEmpty)
    // Too many segments
    assert(Ipv4Utils.ipv4ToLong("1.2.3.4.5").isEmpty)
    // Non-numeric
    assert(Ipv4Utils.ipv4ToLong("abc.def.ghi.jkl").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("192.168.1.1x").isEmpty)
    // Empty segments
    assert(Ipv4Utils.ipv4ToLong("192..1.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong(".192.168.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("192.168.1.").isEmpty)
    // Whitespace
    assert(Ipv4Utils.ipv4ToLong(" 127.0.0.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("127.0.0.1 ").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("127. 0.0.1").isEmpty)
    assert(Ipv4Utils.ipv4ToLong("127.0.0.1\t").isEmpty)
    // Leading zeros - accepted, parsed as decimal (MySQL/Flink compatible)
    assert(Ipv4Utils.ipv4ToLong("010.000.000.001").contains(167772161L))
    // Single "0" is fine
    assert(Ipv4Utils.ipv4ToLong("0.0.0.0").contains(0L))
  }

  test("Ipv4Utils.longToIpv4 - valid values") {
    assert(Ipv4Utils.longToIpv4(0L).contains("0.0.0.0"))
    assert(Ipv4Utils.longToIpv4(4294967295L).contains("255.255.255.255"))
    assert(Ipv4Utils.longToIpv4(3232235777L).contains("192.168.1.1"))
    assert(Ipv4Utils.longToIpv4(2130706433L).contains("127.0.0.1"))
  }

  test("Ipv4Utils.longToIpv4 - out of range") {
    assert(Ipv4Utils.longToIpv4(-1L).isEmpty)
    assert(Ipv4Utils.longToIpv4(4294967296L).isEmpty)
    assert(Ipv4Utils.longToIpv4(Long.MaxValue).isEmpty)
    assert(Ipv4Utils.longToIpv4(Long.MinValue).isEmpty)
  }

  test("Ipv4Utils round-trip") {
    val ips = Seq("1.2.3.4", "192.168.1.1", "10.0.0.1", "255.255.255.255", "0.0.0.0")
    ips.foreach { ip =>
      val longVal = Ipv4Utils.ipv4ToLong(ip).get
      val back = Ipv4Utils.longToIpv4(longVal).get
      assert(back === ip, s"round-trip failed for $ip: got $back")
    }
  }

  // ===========================================================================
  // InetAton Tests
  // ===========================================================================

  test("InetAton - valid 4-segment IPv4 addresses") {
    checkEvaluation(InetAton(Literal("192.168.1.1")), 3232235777L)
    checkEvaluation(InetAton(Literal("0.0.0.0")), 0L)
    checkEvaluation(InetAton(Literal("255.255.255.255")), 4294967295L)
    checkEvaluation(InetAton(Literal("127.0.0.1")), 2130706433L)
    checkEvaluation(InetAton(Literal("10.0.0.1")), 167772161L)
  }

  test("InetAton - valid short-form IPv4 (Flink-compatible)") {
    checkEvaluation(InetAton(Literal("192.168.1")), 3232235521L)
    checkEvaluation(InetAton(Literal("192.168")), 3221225640L)
    checkEvaluation(InetAton(Literal("192")), 192L)
    checkEvaluation(InetAton(Literal("127.1")), 2130706433L)
  }

  test("InetAton - invalid input returns null in non-ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(InetAton(Literal("invalid")), null)
      checkEvaluation(InetAton(Literal("")), null)
      checkEvaluation(InetAton(Literal("256.1.1.1")), null)
      checkEvaluation(InetAton(Literal("-1.2.3.4")), null)
      checkEvaluation(InetAton(Literal("1.2.3.4.5")), null)
      checkEvaluation(InetAton(Literal("192..1.1")), null)
      checkEvaluation(InetAton(Literal(" 127.0.0.1")), null)
    }
  }

  test("InetAton - leading zeros parsed as decimal") {
    // "010.000.000.001" = "10.0.0.1" = 167772161
    checkEvaluation(InetAton(Literal("010.000.000.001")), 167772161L)
    checkEvaluation(InetAton(Literal("010.0.0.1")), 167772161L)
  }

  test("InetAton - invalid input throws in ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkErrorInExpression[SparkIllegalArgumentException](
        InetAton(Literal("invalid")),
        "INVALID_IPV4_ADDRESS",
        Map("address" -> "'invalid'"))
    }
  }

  test("InetAton - null input returns null") {
    checkEvaluation(InetAton(Literal.create(null, StringType)), null)
  }

  test("InetAton - round-trip with InetNtoa") {
    checkEvaluation(
      InetNtoa(InetAton(Literal("1.2.3.4"))),
      UTF8String.fromString("1.2.3.4"))
    checkEvaluation(
      InetNtoa(InetAton(Literal("10.0.0.1"))),
      UTF8String.fromString("10.0.0.1"))
  }

  // ===========================================================================
  // InetNtoa Tests
  // ===========================================================================

  test("InetNtoa - valid long values") {
    checkEvaluation(InetNtoa(Literal(0L)), UTF8String.fromString("0.0.0.0"))
    checkEvaluation(InetNtoa(Literal(4294967295L)),
      UTF8String.fromString("255.255.255.255"))
    checkEvaluation(InetNtoa(Literal(3232235777L)),
      UTF8String.fromString("192.168.1.1"))
    checkEvaluation(InetNtoa(Literal(2130706433L)),
      UTF8String.fromString("127.0.0.1"))
  }

  test("InetNtoa - out of range returns null in non-ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(InetNtoa(Literal(-1L)), null)
      checkEvaluation(InetNtoa(Literal(4294967296L)), null)
    }
  }

  test("InetNtoa - out of range throws in ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkErrorInExpression[SparkIllegalArgumentException](
        InetNtoa(Literal(-1L)),
        "INVALID_IPV4_LONG",
        Map("value" -> "-1"))
    }
  }

  test("InetNtoa - null input returns null") {
    checkEvaluation(InetNtoa(Literal.create(null, LongType)), null)
  }

  // ===========================================================================
  // TryInetAton Tests
  // ===========================================================================

  test("TryInetAton - valid input works like InetAton") {
    checkEvaluation(new TryInetAton(Literal("192.168.1.1")), 3232235777L)
    checkEvaluation(new TryInetAton(Literal("0.0.0.0")), 0L)
    checkEvaluation(new TryInetAton(Literal("255.255.255.255")), 4294967295L)
  }

  test("TryInetAton - invalid input returns null even in ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkEvaluation(new TryInetAton(Literal("invalid")), null)
      checkEvaluation(new TryInetAton(Literal("")), null)
      checkEvaluation(new TryInetAton(Literal("256.1.1.1")), null)
      checkEvaluation(new TryInetAton(Literal("192..1.1")), null)
    }
  }

  test("TryInetAton - null input returns null") {
    checkEvaluation(new TryInetAton(Literal.create(null, StringType)), null)
  }

  // ===========================================================================
  // Foldable tests - prevent ConstantFolding from crashing during doc gen
  // ===========================================================================

  test("InetAton - not foldable (prevents ConstantFolding crash)") {
    assert(!InetAton(Literal("invalid")).foldable)
    assert(!InetAton(Literal("192.168.1.1")).foldable)
    // failOnError=false should also not be foldable (it's still context-dependent)
    assert(!InetAton(Literal("invalid"), false).foldable)
  }

  test("InetNtoa - not foldable (prevents ConstantFolding crash)") {
    assert(!InetNtoa(Literal(0L)).foldable)
    assert(!InetNtoa(Literal(3232235777L)).foldable)
    assert(!InetNtoa(Literal(-1L), false).foldable)
  }

  test("TryInetAton - not foldable (wraps InetAton which is not foldable)") {
    // TryInetAton wraps InetAton (failOnError=false), but its own foldable is inherited
    // and its children include the InetAton which is now not foldable
    assert(!new TryInetAton(Literal("invalid")).foldable)
  }

  // ===========================================================================
  // Type check tests
  // ===========================================================================

  test("InetAton - input type mismatch") {
    val result = InetAton(Literal(1)).checkInputDataTypes()
    assert(result.isInstanceOf[DataTypeMismatch])
    val mismatch = result.asInstanceOf[DataTypeMismatch]
    assert(mismatch.errorSubClass === "UNEXPECTED_INPUT_TYPE")
    assert(mismatch.messageParameters("inputType") === "\"INT\"")
  }

  test("InetNtoa - input type mismatch") {
    val result = InetNtoa(Literal("abc")).checkInputDataTypes()
    assert(result.isInstanceOf[DataTypeMismatch])
    val mismatch = result.asInstanceOf[DataTypeMismatch]
    assert(mismatch.errorSubClass === "UNEXPECTED_INPUT_TYPE")
    assert(mismatch.messageParameters("inputType") === "\"STRING\"")
  }
}
