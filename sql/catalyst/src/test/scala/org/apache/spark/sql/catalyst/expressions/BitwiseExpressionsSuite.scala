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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._


class BitwiseExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  import IntegralLiteralTestUtils._

  test("BitwiseNOT") {
    def check(input: Any, expected: Any): Unit = {
      val expr = BitwiseNot(Literal(input))
      assert(expr.dataType === Literal(input).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, (~1.toByte).toByte)
    check(1000.toShort, (~1000.toShort).toShort)
    check(1000000, ~1000000)
    check(123456789123L, ~123456789123L)

    checkEvaluation(BitwiseNot(Literal.create(null, IntegerType)), null)
    checkEvaluation(BitwiseNot(positiveShortLit), (~positiveShort).toShort)
    checkEvaluation(BitwiseNot(negativeShortLit), (~negativeShort).toShort)
    checkEvaluation(BitwiseNot(positiveIntLit), ~positiveInt)
    checkEvaluation(BitwiseNot(negativeIntLit), ~negativeInt)
    checkEvaluation(BitwiseNot(positiveLongLit), ~positiveLong)
    checkEvaluation(BitwiseNot(negativeLongLit), ~negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseNot, dt)
    }
  }

  test("BitwiseAnd") {
    def check(input1: Any, input2: Any, expected: Any): Unit = {
      val expr = BitwiseAnd(Literal(input1), Literal(input2))
      assert(expr.dataType === Literal(input1).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, 2.toByte, (1.toByte & 2.toByte).toByte)
    check(1000.toShort, 2.toShort, (1000.toShort & 2.toShort).toShort)
    check(1000000, 4, 1000000 & 4)
    check(123456789123L, 5L, 123456789123L & 5L)

    val nullLit = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseAnd(nullLit, Literal(1)), null)
    checkEvaluation(BitwiseAnd(Literal(1), nullLit), null)
    checkEvaluation(BitwiseAnd(nullLit, nullLit), null)
    checkEvaluation(BitwiseAnd(positiveShortLit, negativeShortLit),
      (positiveShort & negativeShort).toShort)
    checkEvaluation(BitwiseAnd(positiveIntLit, negativeIntLit), positiveInt & negativeInt)
    checkEvaluation(BitwiseAnd(positiveLongLit, negativeLongLit), positiveLong & negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseAnd, dt, dt)
    }
  }

  test("BitwiseOr") {
    def check(input1: Any, input2: Any, expected: Any): Unit = {
      val expr = BitwiseOr(Literal(input1), Literal(input2))
      assert(expr.dataType === Literal(input1).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, 2.toByte, (1.toByte | 2.toByte).toByte)
    check(1000.toShort, 2.toShort, (1000.toShort | 2.toShort).toShort)
    check(1000000, 4, 1000000 | 4)
    check(123456789123L, 5L, 123456789123L | 5L)

    val nullLit = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseOr(nullLit, Literal(1)), null)
    checkEvaluation(BitwiseOr(Literal(1), nullLit), null)
    checkEvaluation(BitwiseOr(nullLit, nullLit), null)
    checkEvaluation(BitwiseOr(positiveShortLit, negativeShortLit),
      (positiveShort | negativeShort).toShort)
    checkEvaluation(BitwiseOr(positiveIntLit, negativeIntLit), positiveInt | negativeInt)
    checkEvaluation(BitwiseOr(positiveLongLit, negativeLongLit), positiveLong | negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseOr, dt, dt)
    }
  }

  test("BitwiseXor") {
    def check(input1: Any, input2: Any, expected: Any): Unit = {
      val expr = BitwiseXor(Literal(input1), Literal(input2))
      assert(expr.dataType === Literal(input1).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, 2.toByte, (1.toByte ^ 2.toByte).toByte)
    check(1000.toShort, 2.toShort, (1000.toShort ^ 2.toShort).toShort)
    check(1000000, 4, 1000000 ^ 4)
    check(123456789123L, 5L, 123456789123L ^ 5L)

    val nullLit = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseXor(nullLit, Literal(1)), null)
    checkEvaluation(BitwiseXor(Literal(1), nullLit), null)
    checkEvaluation(BitwiseXor(nullLit, nullLit), null)
    checkEvaluation(BitwiseXor(positiveShortLit, negativeShortLit),
      (positiveShort ^ negativeShort).toShort)
    checkEvaluation(BitwiseXor(positiveIntLit, negativeIntLit), positiveInt ^ negativeInt)
    checkEvaluation(BitwiseXor(positiveLongLit, negativeLongLit), positiveLong ^ negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseXor, dt, dt)
    }
  }

  test("BitGet") {
    val nullLongLiteral = Literal.create(null, LongType)
    val nullIntLiteral = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseGet(nullLongLiteral, Literal(1)), null)
    checkEvaluation(BitwiseGet(Literal(11L), nullIntLiteral), null)
    checkEvaluation(BitwiseGet(nullLongLiteral, nullIntLiteral), null)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(3)), 1.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(2)), 0.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(1)), 1.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(0)), 1.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(63)), 0.toByte)

    val row1 = create_row(11L, -1)
    val row2 = create_row(11L, 64)
    val row3 = create_row(11, 32)
    val row4 = create_row(11.toShort, 16)
    val row5 = create_row(11.toByte, 16)

    val tl = $"t".long.at(0)
    val ti = $"t".int.at(0)
    val ts = $"t".short.at(0)
    val tb = $"t".byte.at(0)
    val p = $"p".int.at(1)

    val expr = BitwiseGet(tl, p)
    checkExceptionInExpression[IllegalArgumentException](
      expr, row1, "Invalid bit position: -1 is less than zero")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row2, "Invalid bit position: 64 exceeds the bit upper limit")
    checkExceptionInExpression[IllegalArgumentException](
      BitwiseGet(ti, p), row3, "Invalid bit position: 32 exceeds the bit upper limit")
    checkExceptionInExpression[IllegalArgumentException](
      BitwiseGet(ts, p), row4, "Invalid bit position: 16 exceeds the bit upper limit")
    checkExceptionInExpression[IllegalArgumentException](
      BitwiseGet(tb, p), row5, "Invalid bit position: 16 exceeds the bit upper limit")

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegenAllowingException(BitwiseGet, dt, IntegerType)
    }
  }

  test("BitSet") {
    val inputs = Array(
      Literal(0) -> 1,
      Literal(0) -> 0,
      Literal(0) -> 8,
      Literal(7) -> 15,
      Literal(7) -> 7,
      Literal(15) -> 15,
      Literal(15) -> 7,
      Literal.create(null, IntegerType) -> null,
      Literal(123) -> null)
    checkEvaluation(BitwiseSet(inputs(0)._1, Literal(0), Literal(1)), inputs(0)._2)
    checkEvaluation(BitwiseSet(inputs(1)._1, Literal(0), Literal(0)), inputs(1)._2)
    checkEvaluation(BitwiseSet(inputs(2)._1, Literal(3), Literal(1)), inputs(2)._2)
    checkEvaluation(BitwiseSet(inputs(3)._1, Literal(3), Literal(1)), inputs(3)._2)
    checkEvaluation(BitwiseSet(inputs(4)._1, Literal(3), Literal(0)), inputs(4)._2)
    checkEvaluation(BitwiseSet(inputs(5)._1, Literal(3), Literal(1)), inputs(5)._2)
    checkEvaluation(BitwiseSet(inputs(6)._1, Literal(3), Literal(0)), inputs(6)._2)
    // Invalid bit position , -ve position not supported
    assert(BitwiseSet(inputs(3)._1, Literal(-1), Literal(1)).checkInputDataTypes().isFailure)
    // Invalid bit position , higher than datatype bit size  not supported
    assert(BitwiseSet(inputs(3)._1, Literal(80), Literal(1)).checkInputDataTypes().isFailure)
    // Invalid replacement bit value
    assert(BitwiseSet(inputs(3)._1, Literal(2), Literal(5)).checkInputDataTypes().isFailure)
    assert(BitwiseSet(inputs(3)._1, Literal(2), Literal(-1)).checkInputDataTypes().isFailure)
    val nullLongLiteral = Literal.create(null, LongType)
    // Null input value
    checkEvaluation(BitwiseSet(inputs(7)._1, Literal(1), Literal(1)), inputs(7)._2)
    // -ve position not supported
    assert(BitwiseSet(inputs(8)._1, Literal(-1), Literal(1)).checkInputDataTypes().isFailure)
    // Null position value not supported
    assert(BitwiseSet(inputs(8)._1, nullLongLiteral, Literal(1)).checkInputDataTypes().isFailure)
    // Null bit value not supported
    assert(BitwiseSet(inputs(8)._1, Literal(3), nullLongLiteral).checkInputDataTypes().isFailure)
    // test all 64 bit set positions in Long datatype
    Seq(9223372036854775806L, 9223372036854775805L, 9223372036854775803L, 9223372036854775799L,
      9223372036854775791L, 9223372036854775775L, 9223372036854775743L, 9223372036854775679L,
      9223372036854775551L, 9223372036854775295L, 9223372036854774783L, 9223372036854773759L,
      9223372036854771711L, 9223372036854767615L, 9223372036854759423L, 9223372036854743039L,
      9223372036854710271L, 9223372036854644735L, 9223372036854513663L, 9223372036854251519L,
      9223372036853727231L, 9223372036852678655L, 9223372036850581503L, 9223372036846387199L,
      9223372036837998591L, 9223372036821221375L, 9223372036787666943L, 9223372036720558079L,
      9223372036586340351L, 9223372036317904895L, 9223372035781033983L, 9223372034707292159L,
      9223372032559808511L, 9223372028264841215L, 9223372019674906623L, 9223372002495037439L,
      9223371968135299071L, 9223371899415822335L, 9223371761976868863L, 9223371487098961919L,
      9223370937343148031L, 9223369837831520255L, 9223367638808264703L, 9223363240761753599L,
      9223354444668731391L, 9223336852482686975L, 9223301668110598143L, 9223231299366420479L,
      9223090561878065151L, 9222809086901354495L, 9222246136947933183L, 9221120237041090559L,
      9218868437227405311L, 9214364837600034815L, 9205357638345293823L, 9187343239835811839L,
      9151314442816847871L, 9079256848778919935L, 8935141660703064063L, 8646911284551352319L,
      8070450532247928831L, 6917529027641081855L, 4611686018427387903L,
      9223372036854775807L).zipWithIndex.foreach { case (result: Long, position: Int) =>
      checkEvaluation(
        BitwiseSet(Literal(9223372036854775807L), Literal(position), Literal(0)),
        result)
    }
    // test all 32 bit set positions in Integer datatype
    Seq(2147483646, 2147483645, 2147483643, 2147483639, 2147483631, 2147483615, 2147483583,
      2147483519, 2147483391, 2147483135, 2147482623, 2147481599, 2147479551, 2147475455,
      2147467263, 2147450879, 2147418111, 2147352575, 2147221503, 2146959359, 2146435071,
      2145386495, 2143289343, 2139095039, 2130706431, 2113929215, 2080374783, 2013265919,
      1879048191, 1610612735, 1073741823, 2147483647).zipWithIndex.foreach {
      case (result: Int, position: Int) =>
        checkEvaluation(BitwiseSet(Literal(2147483647), Literal(position), Literal(0)), result)
    }
    // test all 16 bit set positions in Short datatype
    Seq(32766, 32765, 32763, 32759, 32751, 32735, 32703, 32639, 32511, 32255, 31743, 30719, 28671,
      24575, 16383, 32767).zipWithIndex.foreach { case (result: Int, position: Int) =>
      checkEvaluation(BitwiseSet(Literal(32767), Literal(position), Literal(0)), result)
    }
    // test all 8 bit set positions in Byte datatype
    Seq(126, 125, 123, 119, 111, 95, 63, 127).zipWithIndex.foreach {
      case (result: Int, position: Int) =>
        checkEvaluation(BitwiseSet(Literal(127), Literal(position), Literal(0)), result)
    }
  }
}
