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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ConditionalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("if") {
    val testcases = Seq[(java.lang.Boolean, Integer, Integer, Integer)](
      (true, 1, 2, 1),
      (false, 1, 2, 2),
      (null, 1, 2, 2),
      (true, null, 2, null),
      (false, 1, null, null),
      (null, null, 2, 2),
      (null, 1, null, null)
    )

    // dataType must match T.
    def testIf(convert: (Integer => Any), dataType: DataType): Unit = {
      for ((predicate, trueValue, falseValue, expected) <- testcases) {
        val trueValueConverted = if (trueValue == null) null else convert(trueValue)
        val falseValueConverted = if (falseValue == null) null else convert(falseValue)
        val expectedConverted = if (expected == null) null else convert(expected)

        checkEvaluation(
          If(Literal.create(predicate, BooleanType),
            Literal.create(trueValueConverted, dataType),
            Literal.create(falseValueConverted, dataType)),
          expectedConverted)
      }
    }

    testIf(_ == 1, BooleanType)
    testIf(_.toShort, ShortType)
    testIf(identity, IntegerType)
    testIf(_.toLong, LongType)

    testIf(_.toFloat, FloatType)
    testIf(_.toDouble, DoubleType)
    testIf(Decimal(_), DecimalType.USER_DEFAULT)

    testIf(identity, DateType)
    testIf(_.toLong, TimestampType)

    testIf(_.toString, StringType)

    DataTypeTestUtils.propertyCheckSupported.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(If, BooleanType, dt, dt)
    }
  }

  test("case when") {
    val row = create_row(null, false, true, "a", "b", "c")
    val c1 = $"a".boolean.at(0)
    val c2 = $"a".boolean.at(1)
    val c3 = $"a".boolean.at(2)
    val c4 = $"a".string.at(3)
    val c5 = $"a".string.at(4)
    val c6 = $"a".string.at(5)

    checkEvaluation(CaseWhen(Seq((c1, c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((c2, c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((c3, c4)), c6), "a", row)
    checkEvaluation(CaseWhen(Seq((Literal.create(null, BooleanType), c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((Literal.create(false, BooleanType), c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((Literal.create(true, BooleanType), c4)), c6), "a", row)

    checkEvaluation(CaseWhen(Seq((c3, c4), (c2, c5)), c6), "a", row)
    checkEvaluation(CaseWhen(Seq((c2, c4), (c3, c5)), c6), "b", row)
    checkEvaluation(CaseWhen(Seq((c1, c4), (c2, c5)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((c1, c4), (c2, c5))), null, row)

    assert(CaseWhen(Seq((c2, c4)), c6).nullable)
    assert(CaseWhen(Seq((c2, c4), (c3, c5)), c6).nullable)
    assert(CaseWhen(Seq((c2, c4), (c3, c5))).nullable)

    val c4_notNull = $"a".boolean.notNull.at(3)
    val c5_notNull = $"a".boolean.notNull.at(4)
    val c6_notNull = $"a".boolean.notNull.at(5)

    assert(CaseWhen(Seq((c2, c4_notNull)), c6_notNull).nullable === false)
    assert(CaseWhen(Seq((c2, c4)), c6_notNull).nullable)
    assert(CaseWhen(Seq((c2, c4_notNull))).nullable)
    assert(CaseWhen(Seq((c2, c4_notNull)), c6).nullable)

    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5_notNull)), c6_notNull).nullable === false)
    assert(CaseWhen(Seq((c2, c4), (c3, c5_notNull)), c6_notNull).nullable)
    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5)), c6_notNull).nullable)
    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5_notNull)), c6).nullable)

    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5_notNull))).nullable)
    assert(CaseWhen(Seq((c2, c4), (c3, c5_notNull))).nullable)
    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5))).nullable)
  }

  test("if/case when - null flags of non-primitive types") {
    val arrayWithNulls = Literal.create(Seq("a", null, "b"), ArrayType(StringType, true))
    val arrayWithoutNulls = Literal.create(Seq("c", "d"), ArrayType(StringType, false))
    val structWithNulls = Literal.create(
      create_row(null, null),
      StructType(Seq(StructField("a", IntegerType, true), StructField("b", StringType, true))))
    val structWithoutNulls = Literal.create(
      create_row(1, "a"),
      StructType(Seq(StructField("a", IntegerType, false), StructField("b", StringType, false))))
    val mapWithNulls = Literal.create(Map(1 -> null), MapType(IntegerType, StringType, true))
    val mapWithoutNulls = Literal.create(Map(1 -> "a"), MapType(IntegerType, StringType, false))

    val arrayIf1 = If(Literal.FalseLiteral, arrayWithNulls, arrayWithoutNulls)
    val arrayIf2 = If(Literal.FalseLiteral, arrayWithoutNulls, arrayWithNulls)
    val arrayIf3 = If(Literal.TrueLiteral, arrayWithNulls, arrayWithoutNulls)
    val arrayIf4 = If(Literal.TrueLiteral, arrayWithoutNulls, arrayWithNulls)
    val structIf1 = If(Literal.FalseLiteral, structWithNulls, structWithoutNulls)
    val structIf2 = If(Literal.FalseLiteral, structWithoutNulls, structWithNulls)
    val structIf3 = If(Literal.TrueLiteral, structWithNulls, structWithoutNulls)
    val structIf4 = If(Literal.TrueLiteral, structWithoutNulls, structWithNulls)
    val mapIf1 = If(Literal.FalseLiteral, mapWithNulls, mapWithoutNulls)
    val mapIf2 = If(Literal.FalseLiteral, mapWithoutNulls, mapWithNulls)
    val mapIf3 = If(Literal.TrueLiteral, mapWithNulls, mapWithoutNulls)
    val mapIf4 = If(Literal.TrueLiteral, mapWithoutNulls, mapWithNulls)

    val arrayCaseWhen1 = CaseWhen(Seq((Literal.FalseLiteral, arrayWithNulls)), arrayWithoutNulls)
    val arrayCaseWhen2 = CaseWhen(Seq((Literal.FalseLiteral, arrayWithoutNulls)), arrayWithNulls)
    val arrayCaseWhen3 = CaseWhen(Seq((Literal.TrueLiteral, arrayWithNulls)), arrayWithoutNulls)
    val arrayCaseWhen4 = CaseWhen(Seq((Literal.TrueLiteral, arrayWithoutNulls)), arrayWithNulls)
    val structCaseWhen1 = CaseWhen(Seq((Literal.FalseLiteral, structWithNulls)), structWithoutNulls)
    val structCaseWhen2 = CaseWhen(Seq((Literal.FalseLiteral, structWithoutNulls)), structWithNulls)
    val structCaseWhen3 = CaseWhen(Seq((Literal.TrueLiteral, structWithNulls)), structWithoutNulls)
    val structCaseWhen4 = CaseWhen(Seq((Literal.TrueLiteral, structWithoutNulls)), structWithNulls)
    val mapCaseWhen1 = CaseWhen(Seq((Literal.FalseLiteral, mapWithNulls)), mapWithoutNulls)
    val mapCaseWhen2 = CaseWhen(Seq((Literal.FalseLiteral, mapWithoutNulls)), mapWithNulls)
    val mapCaseWhen3 = CaseWhen(Seq((Literal.TrueLiteral, mapWithNulls)), mapWithoutNulls)
    val mapCaseWhen4 = CaseWhen(Seq((Literal.TrueLiteral, mapWithoutNulls)), mapWithNulls)

    def checkResult(expectedType: DataType, expectedValue: Any, result: Expression): Unit = {
      assert(expectedType == result.dataType)
      checkEvaluation(result, expectedValue)
    }

    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayIf1)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayIf2)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayIf3)
    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayIf4)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structIf1)
    checkResult(structWithNulls.dataType, structWithNulls.value, structIf2)
    checkResult(structWithNulls.dataType, structWithNulls.value, structIf3)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structIf4)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapIf1)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapIf2)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapIf3)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapIf4)

    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayCaseWhen1)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayCaseWhen2)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayCaseWhen3)
    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayCaseWhen4)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structCaseWhen1)
    checkResult(structWithNulls.dataType, structWithNulls.value, structCaseWhen2)
    checkResult(structWithNulls.dataType, structWithNulls.value, structCaseWhen3)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structCaseWhen4)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapCaseWhen1)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapCaseWhen2)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapCaseWhen3)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapCaseWhen4)
  }

  test("case key when") {
    val row = create_row(null, 1, 2, "a", "b", "c")
    val c1 = $"a".int.at(0)
    val c2 = $"a".int.at(1)
    val c3 = $"a".int.at(2)
    val c4 = $"a".string.at(3)
    val c5 = $"a".string.at(4)
    val c6 = $"a".string.at(5)

    val literalNull = Literal.create(null, IntegerType)
    val literalInt = Literal(1)
    val literalString = Literal("a")

    checkEvaluation(CaseKeyWhen(c1, Seq(c2, c4, c5)), "b", row)
    checkEvaluation(CaseKeyWhen(c1, Seq(c2, c4, literalNull, c5, c6)), "c", row)
    checkEvaluation(CaseKeyWhen(c2, Seq(literalInt, c4, c5)), "a", row)
    checkEvaluation(CaseKeyWhen(c2, Seq(c1, c4, c5)), "b", row)
    checkEvaluation(CaseKeyWhen(c4, Seq(literalString, c2, c3)), 1, row)
    checkEvaluation(CaseKeyWhen(c4, Seq(c6, c3, c5, c2, Literal(3))), 3, row)

    checkEvaluation(CaseKeyWhen(literalInt, Seq(c2, c4, c5)), "a", row)
    checkEvaluation(CaseKeyWhen(literalString, Seq(c5, c2, c4, c3)), 2, row)
    checkEvaluation(CaseKeyWhen(c6, Seq(c5, c2, c4, c3)), null, row)
    checkEvaluation(CaseKeyWhen(literalNull, Seq(c2, c5, c1, c6)), null, row)
  }

  test("case key when - internal pattern matching expects a List while apply takes a Seq") {
    val indexedSeq = IndexedSeq(Literal(1), Literal(42), Literal(42), Literal(1))
    val caseKeyWhen = CaseKeyWhen(Literal(12), indexedSeq)
    assert(caseKeyWhen.branches ==
      IndexedSeq((Literal(12) === Literal(1), Literal(42)),
        (Literal(12) === Literal(42), Literal(1))))
  }

  test("SPARK-22705: case when should use less global variables") {
    val ctx = new CodegenContext()
    CaseWhen(Seq((Literal.create(false, BooleanType), Literal(1)),
      (Literal.create(false, BooleanType), Literal(2))), Literal(-1)).genCode(ctx)
    assert(ctx.inlinedMutableStates.size == 1)
  }

  test("SPARK-27551: informative error message of mismatched types for case when") {
    val caseVal1 = Literal.create(
      create_row(1),
      StructType(Seq(StructField("x", IntegerType, false))))
    val caseVal2 = Literal.create(
      create_row(1),
      StructType(Seq(StructField("y", IntegerType, false))))
    val elseVal = Literal.create(
      create_row(1),
      StructType(Seq(StructField("z", IntegerType, false))))

    val checkResult1 = CaseWhen(Seq((Literal.FalseLiteral, caseVal1),
      (Literal.FalseLiteral, caseVal2))).checkInputDataTypes()
    assert(checkResult1 == DataTypeMismatch(
      errorSubClass = "DATA_DIFF_TYPES",
      messageParameters = Map(
        "functionName" -> "`casewhen`",
        "dataType" -> "[\"STRUCT<x: INT NOT NULL>\", \"STRUCT<y: INT NOT NULL>\"]")))

    val checkResult2 = CaseWhen(Seq((Literal.FalseLiteral, caseVal1),
      (Literal.FalseLiteral, caseVal2)), Some(elseVal)).checkInputDataTypes()
    assert(checkResult2 == DataTypeMismatch(
      errorSubClass = "DATA_DIFF_TYPES",
      messageParameters = Map(
        "functionName" -> "`casewhen`",
        "dataType" -> ("[\"STRUCT<x: INT NOT NULL>\", " +
          "\"STRUCT<y: INT NOT NULL>\", \"STRUCT<z: INT NOT NULL>\"]"))))
  }

  test("SPARK-27917 test semantic equals of CaseWhen") {
    val attrRef = AttributeReference("ACCESS_CHECK", StringType)()
    val aliasAttrRef = attrRef.withName("access_check")
    // Test for Equality
    var caseWhenObj1 = CaseWhen(Seq((attrRef, Literal("A"))))
    var caseWhenObj2 = CaseWhen(Seq((aliasAttrRef, Literal("A"))))
    assert(caseWhenObj1.semanticEquals(caseWhenObj2))
    assert(caseWhenObj2.semanticEquals(caseWhenObj1))
    // Test for inEquality
    caseWhenObj2 = CaseWhen(Seq((attrRef, Literal("a"))))
    assert(!caseWhenObj1.semanticEquals(caseWhenObj2))
    assert(!caseWhenObj2.semanticEquals(caseWhenObj1))
    // Test with elseValue with Equality
    caseWhenObj1 = CaseWhen(Seq((attrRef, Literal("A"))), attrRef.withName("ELSEVALUE"))
    caseWhenObj2 = CaseWhen(Seq((aliasAttrRef, Literal("A"))), aliasAttrRef.withName("elsevalue"))
    assert(caseWhenObj1.semanticEquals(caseWhenObj2))
    assert(caseWhenObj2.semanticEquals(caseWhenObj1))
    caseWhenObj1 = CaseWhen(Seq((attrRef, Literal("A"))), Literal("ELSEVALUE"))
    caseWhenObj2 = CaseWhen(Seq((aliasAttrRef, Literal("A"))), Literal("elsevalue"))
    // Test with elseValue with inEquality
    assert(!caseWhenObj1.semanticEquals(caseWhenObj2))
    assert(!caseWhenObj2.semanticEquals(caseWhenObj1))
  }

  test("SPARK-49396 accurate nullability check") {
    val trueBranch = (TrueLiteral, Literal(5))
    val normalBranch = (NonFoldableLiteral(true), Literal(10))

    val nullLiteral = Literal.create(null, BooleanType)
    val noElseValue = CaseWhen(normalBranch :: trueBranch :: Nil, None)
    assert(!noElseValue.nullable)
    val withElseValue = CaseWhen(normalBranch :: trueBranch :: Nil, Some(Literal(1)))
    assert(!withElseValue.nullable)
    val withNullableElseValue = CaseWhen(normalBranch :: trueBranch :: Nil, Some(nullLiteral))
    assert(!withNullableElseValue.nullable)
    val firstTrueNonNullableSecondTrueNullable = CaseWhen(trueBranch ::
      (TrueLiteral, nullLiteral) :: Nil, None)
    assert(!firstTrueNonNullableSecondTrueNullable.nullable)
    val firstTrueNullableSecondTrueNonNullable = CaseWhen((TrueLiteral, nullLiteral) ::
      trueBranch :: Nil, None)
    assert(firstTrueNullableSecondTrueNonNullable.nullable)
    val hasNullInNotTrueBranch = CaseWhen(trueBranch :: (FalseLiteral, nullLiteral) :: Nil, None)
    assert(!hasNullInNotTrueBranch.nullable)
    val noTrueBranch = CaseWhen(normalBranch :: Nil, Literal(1))
    assert(!noTrueBranch.nullable)
  }

  // A lookup-shaped branch: `key = <literal keyValue> THEN <string label>`.
  private def eqBranch(key: Expression, keyValue: Any, label: Any): (Expression, Expression) =
    (EqualTo(key, Literal.create(keyValue, key.dataType)), Literal(label))

  test("CaseWhen lookup: hash probe matches the if/else-if chain (string keys)") {
    val n = CaseWhen.LookupThreshold + 2
    val key = BoundReference(0, StringType, nullable = true)
    val branches = (0 until n).map(i => eqBranch(key, s"k$i", s"v$i"))
    val withElse = CaseWhen(branches, Some(Literal("else")))
    val noElse = CaseWhen(branches, None)

    // hits at the start, middle and end (different bucket positions)
    Seq(0, n / 2, n - 1).foreach { i =>
      checkEvaluation(withElse, s"v$i", create_row(s"k$i"))
      checkEvaluation(noElse, s"v$i", create_row(s"k$i"))
    }
    checkEvaluation(withElse, "else", create_row("absent"))  // miss -> else
    checkEvaluation(noElse, null, create_row("absent"))      // miss, no else -> null
    checkEvaluation(withElse, "else", create_row(null))      // null key -> else
    checkEvaluation(noElse, null, create_row(null))          // null key, no else -> null

    // a constant NULL branch value yields NULL on a hit (not a miss/else)
    val withNullValue = CaseWhen(
      branches.updated(0, (EqualTo(key, Literal("k0")), Literal.create(null, StringType))),
      Some(Literal("else")))
    checkEvaluation(withNullValue, null, create_row("k0"))
    checkEvaluation(withNullValue, "v1", create_row("k1"))

    // duplicate keys: first branch wins
    val dup = CaseWhen(
      ((EqualTo(key, Literal("k0")), Literal("first")): (Expression, Expression)) +:
        ((EqualTo(key, Literal("k0")), Literal("second")): (Expression, Expression)) +:
        branches.drop(1),
      Some(Literal("else")))
    checkEvaluation(dup, "first", create_row("k0"))

    // literal on the left-hand side of the equality
    val swapped = CaseWhen(
      (0 until n).map { i =>
        (EqualTo(Literal(s"k$i"), key), Literal(s"v$i")): (Expression, Expression)
      },
      Some(Literal("else")))
    checkEvaluation(swapped, "v1", create_row("k1"))
    checkEvaluation(swapped, "else", create_row("absent"))
  }

  test("CaseWhen lookup: generates a hash probe under codegen, chain when disabled") {
    val key = BoundReference(0, StringType, nullable = true)
    def mkCase: CaseWhen = CaseWhen(
      (0 until CaseWhen.LookupThreshold + 2).map(i => eqBranch(key, s"k$i", s"v$i")),
      Some(Literal("else")))
    withSQLConf(SQLConf.CASE_WHEN_LOOKUP_ENABLED.key -> "true") {
      val code = mkCase.genCode(new CodegenContext()).code.toString
      assert(code.contains("caseWhenBuckets"), "expected the hash-probe reference array")
      assert(!code.contains("caseWhenResultState"), "should not use the if/else-if chain")
    }
    withSQLConf(SQLConf.CASE_WHEN_LOOKUP_ENABLED.key -> "false") {
      val code = mkCase.genCode(new CodegenContext()).code.toString
      assert(!code.contains("caseWhenBuckets"))
      assert(code.contains("caseWhenResultState"), "expected the if/else-if chain")
    }
  }

  test("CaseWhen lookup: falls back to the if/else-if chain when not lookup-shaped") {
    val n = CaseWhen.LookupThreshold + 2
    def usesChain(cw: CaseWhen): Boolean = {
      val code = cw.genCode(new CodegenContext()).code.toString
      code.contains("caseWhenResultState") && !code.contains("caseWhenBuckets")
    }
    val strKey = BoundReference(0, StringType, nullable = true)
    def elseVal = Some(Literal("else"))

    // Ineligible key types (only binary-collation strings use the probe):
    // integer/long -- cheap compares, the chain wins (measured); float -- NaN/-0.0 equality;
    // non-binary-collation string -- collation-aware equality a hash bucket cannot honor.
    val intKey = BoundReference(0, IntegerType, nullable = true)
    assert(usesChain(CaseWhen((0 until n).map(i => eqBranch(intKey, i, s"v$i")), elseVal)))
    val longKey = BoundReference(0, LongType, nullable = true)
    assert(usesChain(CaseWhen((0 until n).map(i => eqBranch(longKey, i.toLong, s"v$i")), elseVal)))
    val floatKey = BoundReference(0, FloatType, nullable = true)
    assert(usesChain(
      CaseWhen((0 until n).map(i => eqBranch(floatKey, i.toFloat, s"v$i")), elseVal)))
    val lcaseKey = BoundReference(0, StringType("UTF8_LCASE"), nullable = true)
    assert(usesChain(CaseWhen((0 until n).map(i => eqBranch(lcaseKey, s"k$i", s"v$i")), elseVal)))

    // Ineligible shapes on an otherwise-eligible string key:
    // non-foldable branch value (cannot precompute the value table)
    val strCol = BoundReference(1, StringType, nullable = true)
    assert(usesChain(CaseWhen(
      (0 until n).map(i => (EqualTo(strKey, Literal(s"k$i")), strCol): (Expression, Expression)),
      elseVal)))

    // one non-equality branch among otherwise lookup-shaped branches
    val mixed = (0 until n).map(i => eqBranch(strKey, s"k$i", s"v$i")) :+
      ((GreaterThan(strKey, Literal("zzz")), Literal("big")): (Expression, Expression))
    assert(usesChain(CaseWhen(mixed, elseVal)))

    // non-deterministic key (Uuid is a non-deterministic StringType expression; a seed is needed
    // for it to codegen, but it stays non-deterministic so the probe must bail to the chain)
    val uuidKey = Uuid(Some(0L))
    assert(usesChain(CaseWhen((0 until n).map(i => eqBranch(uuidKey, s"k$i", s"v$i")), elseVal)))

    // fewer distinct keys than the fixed threshold
    assert(usesChain(CaseWhen(
      (0 until CaseWhen.LookupThreshold - 1).map(i => eqBranch(strKey, s"k$i", s"v$i")), elseVal)))
  }
}
