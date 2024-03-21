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

import java.util.TimeZone

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.MULTI_COMMUTATIVE_OP_OPT_THRESHOLD
import org.apache.spark.sql.types.{BooleanType, Decimal, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

class CanonicalizeSuite extends SparkFunSuite {

  test("SPARK-24276: IN expression with different order are semantically equal") {
    val range = Range(1, 1, 1, 1)
    val idAttr = range.output.head

    val in1 = In(idAttr, Seq(Literal(1), Literal(2)))
    val in2 = In(idAttr, Seq(Literal(2), Literal(1)))
    val in3 = In(idAttr, Seq(Literal(1), Literal(2), Literal(3)))

    assert(in1.canonicalized.semanticHash() == in2.canonicalized.semanticHash())
    assert(in1.canonicalized.semanticHash() != in3.canonicalized.semanticHash())

    assert(range.where(in1).sameResult(range.where(in2)))
    assert(!range.where(in1).sameResult(range.where(in3)))

    val arrays1 = In(idAttr, Seq(CreateArray(Seq(Literal(1), Literal(2))),
      CreateArray(Seq(Literal(2), Literal(1)))))
    val arrays2 = In(idAttr, Seq(CreateArray(Seq(Literal(2), Literal(1))),
      CreateArray(Seq(Literal(1), Literal(2)))))
    val arrays3 = In(idAttr, Seq(CreateArray(Seq(Literal(1), Literal(2))),
      CreateArray(Seq(Literal(3), Literal(1)))))

    assert(arrays1.canonicalized.semanticHash() == arrays2.canonicalized.semanticHash())
    assert(arrays1.canonicalized.semanticHash() != arrays3.canonicalized.semanticHash())

    assert(range.where(arrays1).sameResult(range.where(arrays2)))
    assert(!range.where(arrays1).sameResult(range.where(arrays3)))
  }

  test("SPARK-26402: accessing nested fields with different cases in case insensitive mode") {
    val expId = NamedExpression.newExprId
    val qualifier = Seq.empty[String]
    val structType = StructType(
      StructField("a", StructType(StructField("b", IntegerType, false) :: Nil), false) :: Nil)

    // GetStructField with different names are semantically equal
    val fieldA1 = GetStructField(
      AttributeReference("data1", structType, false)(expId, qualifier),
      0, Some("a1"))
    val fieldA2 = GetStructField(
      AttributeReference("data2", structType, false)(expId, qualifier),
      0, Some("a2"))
    assert(fieldA1.semanticEquals(fieldA2))

    val fieldB1 = GetStructField(
      GetStructField(
        AttributeReference("data1", structType, false)(expId, qualifier),
        0, Some("a1")),
      0, Some("b1"))
    val fieldB2 = GetStructField(
      GetStructField(
        AttributeReference("data2", structType, false)(expId, qualifier),
        0, Some("a2")),
      0, Some("b2"))
    assert(fieldB1.semanticEquals(fieldB2))
  }

  test("SPARK-30847: Take productPrefix into account in MurmurHash3.productHash") {
    val range = Range(1, 1, 1, 1)
    val addExpr = Add(range.output.head, Literal(1))
    val subExpr = Subtract(range.output.head, Literal(1))
    assert(addExpr.canonicalized.hashCode() != subExpr.canonicalized.hashCode())
  }

  test("SPARK-31515: Canonicalize Cast should consider the value of needTimeZone") {
    val literal = Literal(1)
    val cast = Cast(literal, LongType)
    val castWithTimeZoneId = Cast(literal, LongType, Some(TimeZone.getDefault.getID))
    assert(castWithTimeZoneId.semanticEquals(cast))
  }

  test("SPARK-43336: Canonicalize Cast between Timestamp and TimestampNTZ should consider " +
    "timezone") {
    val timestampLiteral = Literal.create(1L, TimestampType)
    val timestampNTZLiteral = Literal.create(1L, TimestampNTZType)
    Seq(
      Cast(timestampLiteral, TimestampNTZType),
      Cast(timestampNTZLiteral, TimestampType)
    ).foreach { cast =>
      assert(!cast.semanticEquals(cast.withTimeZone(SQLConf.get.sessionLocalTimeZone)))
    }
  }

  test("SPARK-32927: Bitwise operations are commutative") {
    Seq(BitwiseOr(_, _), BitwiseAnd(_, _), BitwiseXor(_, _)).foreach { f =>
      val e1 = f($"a", f($"b", $"c"))
      val e2 = f(f($"a", $"b"), $"c")
      val e3 = f($"a", f($"b", $"a"))

      assert(e1.canonicalized == e2.canonicalized)
      assert(e1.canonicalized != e3.canonicalized)
    }
  }

  test("SPARK-32927: Bitwise operations are commutative for non-deterministic expressions") {
    Seq(BitwiseOr(_, _), BitwiseAnd(_, _), BitwiseXor(_, _)).foreach { f =>
      val e1 = f($"a", f(rand(42), $"c"))
      val e2 = f(f($"a", rand(42)), $"c")
      val e3 = f($"a", f(rand(42), $"a"))

      assert(e1.canonicalized == e2.canonicalized)
      assert(e1.canonicalized != e3.canonicalized)
    }
  }

  test("SPARK-32927: Bitwise operations are commutative for literal expressions") {
    Seq(BitwiseOr(_, _), BitwiseAnd(_, _), BitwiseXor(_, _)).foreach { f =>
      val e1 = f($"a", f(42, $"c"))
      val e2 = f(f($"a", 42), $"c")
      val e3 = f($"a", f(42, $"a"))

      assert(e1.canonicalized == e2.canonicalized)
      assert(e1.canonicalized != e3.canonicalized)
    }
  }

  test("SPARK-32927: Bitwise operations are commutative in a complex case") {
    Seq(BitwiseOr(_, _), BitwiseAnd(_, _), BitwiseXor(_, _)).foreach { f1 =>
      Seq(BitwiseOr(_, _), BitwiseAnd(_, _), BitwiseXor(_, _)).foreach { f2 =>
        val e1 = f2(f1($"a", f1($"b", $"c")), $"a")
        val e2 = f2(f1(f1($"a", $"b"), $"c"), $"a")
        val e3 = f2(f1($"a", f1($"b", $"a")), $"a")

        assert(e1.canonicalized == e2.canonicalized)
        assert(e1.canonicalized != e3.canonicalized)
      }
    }
  }

  test("SPARK-33421: Support Greatest and Least in Expression Canonicalize") {
    Seq(Least(_), Greatest(_)).foreach { f =>
      // test deterministic expr
      val expr1 = f(Seq(Literal(1), Literal(2), Literal(3)))
      val expr2 = f(Seq(Literal(3), Literal(1), Literal(2)))
      val expr3 = f(Seq(Literal(1), Literal(1), Literal(1)))
      assert(expr1.canonicalized == expr2.canonicalized)
      assert(expr1.canonicalized != expr3.canonicalized)
      assert(expr2.canonicalized != expr3.canonicalized)

      // test non-deterministic expr
      val randExpr1 = f(Seq(Literal(1), rand(1)))
      val randExpr2 = f(Seq(rand(1), Literal(1)))
      val randExpr3 = f(Seq(Literal(1), rand(2)))
      assert(randExpr1.canonicalized == randExpr2.canonicalized)
      assert(randExpr1.canonicalized != randExpr3.canonicalized)
      assert(randExpr2.canonicalized != randExpr3.canonicalized)

      // test nested expr
      val nestedExpr1 = f(Seq(Literal(1), f(Seq(Literal(2), Literal(3)))))
      val nestedExpr2 = f(Seq(f(Seq(Literal(2), Literal(3))), Literal(1)))
      val nestedExpr3 = f(Seq(f(Seq(Literal(1), Literal(1))), Literal(1)))
      assert(nestedExpr1.canonicalized == nestedExpr2.canonicalized)
      assert(nestedExpr1.canonicalized != nestedExpr3.canonicalized)
      assert(nestedExpr2.canonicalized != nestedExpr3.canonicalized)
    }
  }

  test("SPARK-38030: Canonicalization should not remove nullability of AttributeReference" +
    " dataType") {
    val structType = StructType(Seq(StructField("name", StringType, nullable = false)))
    val attr = AttributeReference("col", structType)()
    // AttributeReference dataType should not be converted to nullable
    assert(attr.canonicalized.dataType === structType)

    val cast = Cast(attr, structType)
    assert(cast.resolved)
    // canonicalization should not converted resolved cast to unresolved
    assert(cast.canonicalized.resolved)
  }

  test("SPARK-40362: Commutative operator under BinaryComparison") {
    Seq(EqualTo, EqualNullSafe, GreaterThan, LessThan, GreaterThanOrEqual, LessThanOrEqual)
      .foreach { bc =>
        assert(bc(Multiply($"a", $"b"), Literal(10)).semanticEquals(
          bc(Multiply($"b", $"a"), Literal(10))))
      }
  }

  test("SPARK-40903: Only reorder decimal Add when the result data type is not changed") {
    val d = Decimal(1.2)
    val literal1 = Literal.create(d, DecimalType(2, 1))
    val literal2 = Literal.create(d, DecimalType(2, 1))
    val literal3 = Literal.create(d, DecimalType(3, 2))
    assert(Add(literal1, literal2).semanticEquals(Add(literal2, literal1)))
    assert(Add(Add(literal1, literal2), literal3).semanticEquals(
      Add(Add(literal3, literal2), literal1)))

    val literal4 = Literal.create(d, DecimalType(12, 5))
    val literal5 = Literal.create(d, DecimalType(12, 6))
    assert(!Add(Add(literal4, literal5), literal1).semanticEquals(
      Add(Add(literal1, literal5), literal4)))
  }

  test("SPARK-42162: Commutative expression canonicalization should work" +
    " with the MultiCommutativeOp memory optimization") {
    val default = SQLConf.get.getConf(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD)
    SQLConf.get.setConfString(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD.key, "3")

    // Add
    val d = Decimal(1.2)
    val literal1 = Literal.create(d, DecimalType(2, 1))
    val literal2 = Literal.create(d, DecimalType(2, 1))
    val literal3 = Literal.create(d, DecimalType(3, 2))
    assert(Add(literal1, Add(literal2, literal3))
      .semanticEquals(Add(Add(literal1, literal2), literal3)))
    assert(Add(literal1, Add(literal2, literal3)).canonicalized.isInstanceOf[MultiCommutativeOp])

    // Multiply
    assert(Multiply(literal1, Multiply(literal2, literal3))
      .semanticEquals(Multiply(Multiply(literal1, literal2), literal3)))
    assert(Multiply(literal1, Multiply(literal2, literal3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // And
    val literalBool1 = Literal.create(true, BooleanType)
    val literalBool2 = Literal.create(true, BooleanType)
    val literalBool3 = Literal.create(true, BooleanType)
    assert(And(literalBool1, And(literalBool2, literalBool3))
      .semanticEquals(And(And(literalBool1, literalBool2), literalBool3)))
    assert(And(literalBool1, And(literalBool2, literalBool3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // Or
    assert(Or(literalBool1, Or(literalBool2, literalBool3))
      .semanticEquals(Or(Or(literalBool1, literalBool2), literalBool3)))
    assert(Or(literalBool1, Or(literalBool2, literalBool3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // BitwiseAnd
    val literalBit1 = Literal(1)
    val literalBit2 = Literal(2)
    val literalBit3 = Literal(3)
    assert(BitwiseAnd(literalBit1, BitwiseAnd(literalBit2, literalBit3))
      .semanticEquals(BitwiseAnd(BitwiseAnd(literalBit1, literalBit2), literalBit3)))
    assert(BitwiseAnd(literalBit1, BitwiseAnd(literalBit2, literalBit3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // BitwiseOr
    assert(BitwiseOr(literalBit1, BitwiseOr(literalBit2, literalBit3))
      .semanticEquals(BitwiseOr(BitwiseOr(literalBit1, literalBit2), literalBit3)))
    assert(BitwiseOr(literalBit1, BitwiseOr(literalBit2, literalBit3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // BitwiseXor
    assert(BitwiseXor(literalBit1, BitwiseXor(literalBit2, literalBit3))
      .semanticEquals(BitwiseXor(BitwiseXor(literalBit1, literalBit2), literalBit3)))
    assert(BitwiseXor(literalBit1, BitwiseXor(literalBit2, literalBit3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    SQLConf.get.setConfString(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD.key, default.toString)
  }

  test("SPARK-42162: Commutative expression canonicalization should not use" +
    " MultiCommutativeOp memory optimization when threshold is not met") {
    val default = SQLConf.get.getConf(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD)
    SQLConf.get.setConfString(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD.key, "100")

    // Add
    val d = Decimal(1.2)
    val literal1 = Literal.create(d, DecimalType(2, 1))
    val literal2 = Literal.create(d, DecimalType(2, 1))
    val literal3 = Literal.create(d, DecimalType(3, 2))
    assert(Add(literal1, Add(literal2, literal3))
      .semanticEquals(Add(Add(literal1, literal2), literal3)))
    assert(!Add(literal1, Add(literal2, literal3)).canonicalized.isInstanceOf[MultiCommutativeOp])

    // Multiply
    assert(Multiply(literal1, Multiply(literal2, literal3))
      .semanticEquals(Multiply(Multiply(literal1, literal2), literal3)))
    assert(!Multiply(literal1, Multiply(literal2, literal3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // And
    val literalBool1 = Literal.create(true, BooleanType)
    val literalBool2 = Literal.create(true, BooleanType)
    val literalBool3 = Literal.create(true, BooleanType)
    assert(And(literalBool1, And(literalBool2, literalBool3))
      .semanticEquals(And(And(literalBool1, literalBool2), literalBool3)))
    assert(!And(literalBool1, And(literalBool2, literalBool3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // Or
    assert(Or(literalBool1, Or(literalBool2, literalBool3))
      .semanticEquals(Or(Or(literalBool1, literalBool2), literalBool3)))
    assert(!Or(literalBool1, Or(literalBool2, literalBool3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // BitwiseAnd
    val literalBit1 = Literal(1)
    val literalBit2 = Literal(2)
    val literalBit3 = Literal(3)
    assert(BitwiseAnd(literalBit1, BitwiseAnd(literalBit2, literalBit3))
      .semanticEquals(BitwiseAnd(BitwiseAnd(literalBit1, literalBit2), literalBit3)))
    assert(!BitwiseAnd(literalBit1, BitwiseAnd(literalBit2, literalBit3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // BitwiseOr
    assert(BitwiseOr(literalBit1, BitwiseOr(literalBit2, literalBit3))
      .semanticEquals(BitwiseOr(BitwiseOr(literalBit1, literalBit2), literalBit3)))
    assert(!BitwiseOr(literalBit1, BitwiseOr(literalBit2, literalBit3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    // BitwiseXor
    assert(BitwiseXor(literalBit1, BitwiseXor(literalBit2, literalBit3))
      .semanticEquals(BitwiseXor(BitwiseXor(literalBit1, literalBit2), literalBit3)))
    assert(!BitwiseXor(literalBit1, BitwiseXor(literalBit2, literalBit3))
      .canonicalized.isInstanceOf[MultiCommutativeOp])

    SQLConf.get.setConfString(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD.key, default.toString)
  }

  test("toJSON works properly with MultiCommutativeOp") {
    val default = SQLConf.get.getConf(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD)
    SQLConf.get.setConfString(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD.key, "1")

    val d = Decimal(1.2)
    val literal1 = Literal.create(d, DecimalType(2, 1))
    val literal2 = Literal.create(d, DecimalType(2, 1))
    val literal3 = Literal.create(d, DecimalType(3, 2))
    val op = Add(literal1, Add(literal2, literal3))
    assert(op.canonicalized.toJSON.nonEmpty)
    SQLConf.get.setConfString(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD.key, default.toString)
  }

  test("canonicalization of With expressions with one common expression") {
    val expr = Divide(Literal.create(1, IntegerType), AttributeReference("a", IntegerType)())
    val common1 = IsNull(With(expr.copy()) { case Seq(expr) =>
      If(EqualTo(expr, Literal.create(0.0, DoubleType)), Literal.create(0.0, DoubleType), expr)
    })
    val common2 = IsNull(With(expr.copy()) { case Seq(expr) =>
      If(EqualTo(expr, Literal.create(0.0, DoubleType)), Literal.create(0.0, DoubleType), expr)
    })
    // Check that canonicalization is consistent across multiple executions.
    assert(common1.canonicalized == common2.canonicalized)
    // Check that CommonExpressionDef starts ID'ing at 1 and that its child is canonicalized.
    assert(common1.canonicalized.exists {
      case d: CommonExpressionDef => d.id.id == 1 && d.child == expr.canonicalized
      case _ => false
    })
    // Check that CommonExpressionRef ID corresponds to the def.
    assert(common1.canonicalized.exists {
      case r: CommonExpressionRef => r.id.id == 1
      case _ => false
    })
  }

  test("canonicalization of With expressions with multiple common expressions") {
    val expr1 = Divide(Literal.create(1, IntegerType), AttributeReference("a", IntegerType)())
    val expr2 = Multiply(Literal.create(2, IntegerType), AttributeReference("a", IntegerType)())
    val common1 = With(expr1.copy(), expr2.copy()) { case Seq(expr1, expr2) =>
      If(EqualTo(expr1, expr2), expr1, expr2)
    }
    val common2 = With(expr1.copy(), expr2.copy()) { case Seq(expr1, expr2) =>
      If(EqualTo(expr1, expr2), expr1, expr2)
    }
    // Check that canonicalization is consistent across multiple executions.
    assert(common1.canonicalized == common2.canonicalized)
    // Check that CommonExpressionDef starts ID'ing at 1 and that its child is canonicalized.
    assert(common1.canonicalized.exists {
      case d: CommonExpressionDef => d.id.id == 1 && d.child == expr1.canonicalized
      case _ => false
    })
    assert(common1.canonicalized.exists {
      case d: CommonExpressionDef => d.id.id == 2 && d.child == expr2.canonicalized
      case _ => false
    })
    // Check that CommonExpressionRef ID corresponds to the def.
    assert(common1.canonicalized.exists {
      case r: CommonExpressionRef => r.id.id == 1
      case _ => false
    })
    assert(common1.canonicalized.exists {
      case r: CommonExpressionRef => r.id.id == 2
      case _ => false
    })
  }

  test("canonicalization of With expressions with nested common expressions") {
    val expr1 = AttributeReference("a", BooleanType)()
    val expr2 = AttributeReference("b", BooleanType)()

    val common1 = With(expr1) { case Seq(expr1) =>
      Or(With(expr2) { case Seq(expr2) =>
        And(EqualTo(expr1, expr2), EqualTo(expr1, expr2))
      }, expr1)
    }
    val common2 = With(expr1) { case Seq(expr1) =>
      Or(With(expr2) { case Seq(expr2) =>
        And(EqualTo(expr1, expr2), EqualTo(expr1, expr2))
      }, expr1)
    }
    // Check that canonicalization is consistent across multiple executions.
    assert(common1.canonicalized == common2.canonicalized)
    // Check that CommonExpressionDef starts ID'ing at 1 and that its child is canonicalized.
    assert(common1.canonicalized.exists {
      case d: CommonExpressionDef => d.id.id == 1 && d.child == expr2.canonicalized
      case _ => false
    })
    assert(common1.canonicalized.exists {
      case d: CommonExpressionDef => d.id.id == 2 && d.child == expr1.canonicalized
      case _ => false
    })
    // Check that CommonExpressionRef ID corresponds to the def.
    assert(common1.canonicalized.exists {
      case r: CommonExpressionRef => r.id.id == 1
      case _ => false
    })
    assert(common1.canonicalized.exists {
      case r: CommonExpressionRef => r.id.id == 2
      case _ => false
    })

    val common3 = With(expr1.newInstance()) { case Seq(expr1) =>
      Or(With(expr2.newInstance()) { case Seq(expr2) =>
        And(EqualTo(expr1, expr2), EqualTo(expr1, expr2))
      }, expr1)
    }
    val common4 = With(expr1.newInstance()) { case Seq(expr1) =>
      Or(With(expr2.newInstance()) { case Seq(expr2) =>
        And(EqualTo(expr2, expr1), EqualTo(expr2, expr1))
      }, expr1)
    }
    // Check that canonicalization for two different expressions with similar structures is
    // different.
    assert(common3.canonicalized != common4.canonicalized)
  }
}
