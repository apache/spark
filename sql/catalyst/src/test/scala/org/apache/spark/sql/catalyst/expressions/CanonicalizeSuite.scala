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

import junit.framework.TestCase.assertEquals

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Range}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

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

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for add operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int)
    val y = tr1.where('a.attr + 'c.attr > 10).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val addExpr = (fullCond match {
      case GreaterThan(x: Add, _) => x
      case LessThan(_, x: Add) => x
    }).clone().asInstanceOf[Add]
    val canonicalizedFullCond = fullCond.canonicalized
    val newAddExpr = Add(addExpr.right, addExpr.left)
    val builtCondnCanonicalized = GreaterThan(newAddExpr, Literal(10)).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for multiply operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int)
    val y = tr1.where('a.attr * 'c.attr > 10).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val multiplyExpr = (fullCond match {
      case GreaterThan(x: Multiply, _) => x
      case LessThan(_, x: Multiply) => x
    }).clone().asInstanceOf[Multiply]
    val canonicalizedFullCond = fullCond.canonicalized
    val newMultiplyExpr = Multiply(multiplyExpr.right, multiplyExpr.left)
    val builtCondnCanonicalized = GreaterThan(newMultiplyExpr, Literal(10)).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for OR operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val attribA = tr1.output.find(_.name == "a").get
    val y = tr1.where(('a.attr + 'c.attr > 10 || 'a.attr * 'c.attr < 35) ===
      'a.attr + 1 > 6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val orExpr = (fullCond match {
      case EqualTo(x: Or, _) => x
      case EqualTo(_, x: Or) => x
    }).clone().asInstanceOf[Or]
    val canonicalizedFullCond = fullCond.canonicalized
    val newOrExpr = Or(orExpr.right, orExpr.left)
    val builtCondnCanonicalized = EqualTo(newOrExpr, attribA + 1 > 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for And operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val attribA = tr1.output.find(_.name == "a").get
    val y = tr1.where(('a.attr + 'c.attr + 8 > 10 && 'a.attr * 'c.attr < 35) ===
      'a.attr + 1 > 6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val andExpr = (fullCond match {
      case EqualTo(x: And, _) => x
      case EqualTo(_, x: And) => x
    }).clone().asInstanceOf[And]
    val canonicalizedFullCond = fullCond.canonicalized
    val newAndExpr = And(andExpr.right, andExpr.left)
    val builtCondnCanonicalized = EqualTo(newAndExpr, attribA + 1 > 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for bitwise OR operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val y = tr1.where(('c.attr + 5 | 'a.attr) ===
     6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val orExpr = (fullCond match {
      case EqualTo(x: BitwiseOr, _) => x
      case EqualTo(_, x: BitwiseOr) => x
    }).clone().asInstanceOf[BitwiseOr]
    val canonicalizedFullCond = fullCond.canonicalized
    val newOrExpr = BitwiseOr(orExpr.right, orExpr.left)
    val builtCondnCanonicalized = EqualTo(newOrExpr, 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for bitwise And operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val y = tr1.where(('a.attr & ('c.attr * 90)  ) ===
      6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val andExpr = (fullCond match {
      case EqualTo(x: BitwiseAnd, _) => x
      case EqualTo(_, x: BitwiseAnd) => x
    }).clone().asInstanceOf[BitwiseAnd]
    val canonicalizedFullCond = fullCond.canonicalized
    val newAndExpr = BitwiseAnd(andExpr.right, andExpr.left)
    val builtCondnCanonicalized = EqualTo(newAndExpr, 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for Bitwise Xor operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val y = tr1.where((('a.attr * 100) ^ ('a.attr * 210)) ===
      6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val xorExpr = (fullCond match {
      case EqualTo(x: BitwiseXor, _) => x
      case EqualTo(_, x: BitwiseXor) => x
    }).clone().asInstanceOf[BitwiseXor]
    val canonicalizedFullCond = fullCond.canonicalized
    val newXorExpr = BitwiseXor(xorExpr.right, xorExpr.left)
    val builtCondnCanonicalized = EqualTo(newXorExpr, 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for Greatest operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val y = tr1.where(greatest('a.attr * 100, 'a.attr * 210) ===
      6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val greatestExpr = (fullCond match {
      case EqualTo(x: Greatest, _) => x
      case EqualTo(_, x: Greatest) => x
    }).clone().asInstanceOf[Greatest]
    val canonicalizedFullCond = fullCond.canonicalized
    val newGreatestExpr = greatest(greatestExpr.children(1), greatestExpr.children.head)
    val builtCondnCanonicalized = EqualTo(newGreatestExpr, 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 commutative operators as subexpression breaks canonicalization" +
    " for Least operator") {
    val tr1 = LocalRelation('c.int, 'b.string, 'a.int).analyze

    val y = tr1.where(least('a.attr * 200, 'a.attr * 110) ===
      6).analyze
    val fullCond = y.asInstanceOf[Filter].condition.clone()
    val leastExpr = (fullCond match {
      case EqualTo(x: Least, _) => x
      case EqualTo(_, x: Least) => x
    }).clone().asInstanceOf[Least]
    val canonicalizedFullCond = fullCond.canonicalized
    val newLeastExpr = least(leastExpr.children(1), leastExpr.children.head)
    val builtCondnCanonicalized = EqualTo(newLeastExpr, 6).canonicalized
    assertEquals(canonicalizedFullCond, builtCondnCanonicalized)
  }

  test("SPARK-40362 validate fix using a deep nested tree of commutative & non" +
    "commutative expressions") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int).analyze

    val y = tr1.where(
      CaseWhen(
        Seq(
          ('a.attr + 'b.attr > Literal(300), Literal(1)),
          ('a.attr * 'b.attr > Literal(400), Literal(2)),
          (('a.attr * 'b.attr - 700 > Literal(100)) || ('d.attr * 'e.attr < Literal(1000)),
            Literal(7)),
          (
            CaseWhen(
              Seq(
                (abs('b.attr * 'd.attr) - 100 > Literal(50), Literal(5)),
                (abs('e.attr * 'a.attr) < Literal(30), Literal(11)),
                ((abs('c.attr * 'a.attr) < Literal(30)) && ('a.attr * 'd.attr < Literal(1000)),
                  Literal(17)),
              ), Option(Literal(99))) > Literal(-1000), Literal(9)
          )
        ), Option(Literal(-2000))) > Literal(-160000)).analyze

    val fullCond = y.asInstanceOf[Filter].condition.clone()

    val canonicalizeCond1 = fullCond.canonicalized

    // get a new expression where for the commutative expressions, reverse the
    // operands
    val fullConditionWithOpsReversed = fullCond.transformDown {
      case a@Add(l, r, _) => a.copy(left = r, right = l)
      case m@Multiply(l, r, _) => m.copy(left = r, right = l)
      case a@And(l, r) => a.copy(left = r, right = l)
      case o@Or(l, r) => o.copy(left = r, right = l)
    }

    val canonicalizedCond2 = fullConditionWithOpsReversed.canonicalized

    assertEquals(canonicalizeCond1, canonicalizedCond2)
  }

  test("benchmark1") {
    val t1 = System.currentTimeMillis
    val col = Literal(true)
    var and = And(col, col)
    var i = 0
    while (i < 2000) {
      and = And(and, col)
      i += 1
    }
    and.canonicalized
    val t2 = System.currentTimeMillis
    println("time = " + (t2 - t1))
  }

  test("benchmark2") {
    val t1 = System.currentTimeMillis()
    val col1 = AttributeReference("a", IntegerType)()
    val col2 = AttributeReference("b", IntegerType)()
    var and: Expression = And(col1 + col2 < 100, col1 * col2 > -100)
    var i = 0
    while (i < 2000) {
      and = if (i % 2 == 0) {
        And(and, col1 * col2 > -i * 10)
      } else {
        Or(and, col1 * col2 > -i * 10)
      }
      i += 1
    }
    and.canonicalized
    val t2 = System.currentTimeMillis()
    println("time taken ms= " + (t2 - t1))
  }
}
