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

package org.apache.spark.sql.catalyst.plans

import java.util.TimeZone

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType}

class ConstraintPropagationSuite extends PlanTest {

  private def resolveColumn(tr: LocalRelation, columnName: String): Expression =
    resolveColumn(tr.analyze, columnName)

  private def resolveColumn(plan: LogicalPlan, columnName: String): Expression =
    plan.resolveQuoted(columnName, caseInsensitiveResolution).get

  private def verifyConstraints(found: ExpressionSet, expected: ExpressionSet): Unit = {
    val missing = expected -- found
    val extra = found -- expected
    if (missing.nonEmpty || extra.nonEmpty) {
      fail(
        s"""
           |== FAIL: Constraints do not match ===
           |Found: ${found.mkString(",")}
           |Expected: ${expected.mkString(",")}
           |== Result ==
           |Missing: ${if (missing.isEmpty) "N/A" else missing.mkString(",")}
           |Found but not expected: ${if (extra.isEmpty) "N/A" else extra.mkString(",")}
         """.stripMargin)
    }
  }

  private def castWithTimeZone(expr: Expression, dataType: DataType) = {
    Cast(expr, dataType, Option(TimeZone.getDefault().getID))
  }

  test("propagating constraints in filters") {
    val tr = LocalRelation($"a".int, $"b".string, $"c".int)

    assert(tr.analyze.constraints.isEmpty)

    assert(tr.where($"a".attr > 10).select($"c".attr, $"b".attr).analyze.constraints.isEmpty)

    verifyConstraints(tr
      .where($"a".attr > 10)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") > 10,
        IsNotNull(resolveColumn(tr, "a")))))

    verifyConstraints(tr
      .where($"a".attr > 10)
      .select($"c".attr, $"a".attr)
      .where($"c".attr =!= 100)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") > 10,
        resolveColumn(tr, "c") =!= 100,
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "c")))))
  }

  test("propagating constraints in aggregate") {
    val tr = LocalRelation($"a".int, $"b".string, $"c".int)

    assert(tr.analyze.constraints.isEmpty)

    val aliasedRelation = tr.where($"c".attr > 10 && $"a".attr < 5)
      .groupBy($"a", $"c", $"b")($"a", $"c".as("c1"), count($"a").as("a3"))
      .select($"c1", $"a", $"a3").analyze

    // SPARK-16644: aggregate expression count(a) should not appear in the constraints.
    verifyConstraints(aliasedRelation.analyze.constraints,
      ExpressionSet(Seq(resolveColumn(aliasedRelation.analyze, "c1") > 10,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "c1")),
        resolveColumn(aliasedRelation.analyze, "a") < 5,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "a")),
        IsNotNull(resolveColumn(aliasedRelation.analyze, "a3")))))
  }

  test("propagating constraints in expand") {
    val tr = LocalRelation($"a".int, $"b".int, $"c".int)

    assert(tr.analyze.constraints.isEmpty)

    // We add IsNotNull constraints for 'a, 'b and 'c into LocalRelation
    // by creating notNullRelation.
    val notNullRelation = tr.where($"c".attr > 10 && $"a".attr < 5 && $"b".attr > 2)
    verifyConstraints(notNullRelation.analyze.constraints,
      ExpressionSet(Seq(resolveColumn(notNullRelation.analyze, "c") > 10,
        IsNotNull(resolveColumn(notNullRelation.analyze, "c")),
        resolveColumn(notNullRelation.analyze, "a") < 5,
        IsNotNull(resolveColumn(notNullRelation.analyze, "a")),
        resolveColumn(notNullRelation.analyze, "b") > 2,
        IsNotNull(resolveColumn(notNullRelation.analyze, "b")))))

    val expand = Expand(
          Seq(
            Seq($"c", Literal.create(null, StringType), 1),
            Seq($"c", $"a", 2)),
          Seq($"c", $"a", $"gid".int),
          Project(Seq($"a", $"c"),
            notNullRelation))
    verifyConstraints(expand.analyze.constraints,
      ExpressionSet(Seq.empty[Expression]))
  }

  test("propagating constraints in aliases") {
    val tr = LocalRelation($"a".int, $"b".string, $"c".int)

    assert(tr.where($"c".attr > 10).select($"a".as("x"), $"b".as("y"))
      .analyze.constraints.isEmpty)

    val aliasedRelation = tr.where($"a".attr > 10).select($"a".as("x"), $"b",
      $"b".as("y"), $"a".as("z"))

    verifyConstraints(aliasedRelation.analyze.constraints,
      ExpressionSet(Seq(resolveColumn(aliasedRelation.analyze, "x") > 10,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "x")),
        resolveColumn(aliasedRelation.analyze, "b") <=> resolveColumn(aliasedRelation.analyze, "y"),
        resolveColumn(aliasedRelation.analyze, "z") <=> resolveColumn(aliasedRelation.analyze, "x"),
        resolveColumn(aliasedRelation.analyze, "z") > 10,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "z")))))

    val multiAlias = tr.where($"a" === $"c" + 10).select($"a".as("x"), $"c".as("y"))
    verifyConstraints(multiAlias.analyze.constraints,
      ExpressionSet(Seq(IsNotNull(resolveColumn(multiAlias.analyze, "x")),
        IsNotNull(resolveColumn(multiAlias.analyze, "y")),
        resolveColumn(multiAlias.analyze, "x") === resolveColumn(multiAlias.analyze, "y") + 10))
    )
  }

  test("propagating constraints in union") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int)
    val tr2 = LocalRelation($"d".int, $"e".int, $"f".int)
    val tr3 = LocalRelation($"g".int, $"h".int, $"i".int)

    assert(tr1
      .where($"a".attr > 10)
      .union(tr2.where($"e".attr > 10)
      .union(tr3.where($"i".attr > 10)))
      .analyze.constraints.isEmpty)

    verifyConstraints(tr1
      .where($"a".attr > 10)
      .union(tr2.where($"d".attr > 10)
      .union(tr3.where($"g".attr > 10)))
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr1, "a") > 10,
        IsNotNull(resolveColumn(tr1, "a")))))

    val a = resolveColumn(tr1, "a")
    verifyConstraints(tr1
      .where($"a".attr > 10)
      .union(tr2.where($"d".attr > 11))
      .analyze.constraints,
      ExpressionSet(Seq(a > 10 || a > 11, IsNotNull(a))))

    val b = resolveColumn(tr1, "b")
    verifyConstraints(tr1
      .where($"a".attr > 10 && $"b".attr < 10)
      .union(tr2.where($"d".attr > 11 && $"e".attr < 11))
      .analyze.constraints,
      ExpressionSet(Seq(a > 10 || a > 11, b < 10 || b < 11, IsNotNull(a), IsNotNull(b))))
  }

  test("propagating constraints in intersect") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int)
    val tr2 = LocalRelation($"a".int, $"b".int, $"c".int)

    verifyConstraints(tr1
      .where($"a".attr > 10)
      .intersect(tr2.where($"b".attr < 100), isAll = false)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr1, "a") > 10,
        resolveColumn(tr1, "b") < 100,
        IsNotNull(resolveColumn(tr1, "a")),
        IsNotNull(resolveColumn(tr1, "b")))))
  }

  test("propagating constraints in except") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int)
    val tr2 = LocalRelation($"a".int, $"b".int, $"c".int)
    verifyConstraints(tr1
      .where($"a".attr > 10)
      .except(tr2.where($"b".attr < 100), isAll = false)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr1, "a") > 10,
        IsNotNull(resolveColumn(tr1, "a")))))
  }

  test("propagating constraints in inner join") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")
    verifyConstraints(tr1
      .where($"a".attr > 10)
      .join(tr2.where($"d".attr < 100), Inner, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr1.resolveQuoted("a", caseInsensitiveResolution).get > 10,
        tr2.resolveQuoted("d", caseInsensitiveResolution).get < 100,
        tr1.resolveQuoted("a", caseInsensitiveResolution).get ===
          tr2.resolveQuoted("a", caseInsensitiveResolution).get,
        tr2.resolveQuoted("a", caseInsensitiveResolution).get > 10,
        IsNotNull(tr2.resolveQuoted("a", caseInsensitiveResolution).get),
        IsNotNull(tr1.resolveQuoted("a", caseInsensitiveResolution).get),
        IsNotNull(tr2.resolveQuoted("d", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in left-semi join") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")
    verifyConstraints(tr1
      .where($"a".attr > 10)
      .join(tr2.where($"d".attr < 100), LeftSemi, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr1.resolveQuoted("a", caseInsensitiveResolution).get > 10,
        IsNotNull(tr1.resolveQuoted("a", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in left-outer join") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")
    verifyConstraints(tr1
      .where($"a".attr > 10)
      .join(tr2.where($"d".attr < 100), LeftOuter, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr1.resolveQuoted("a", caseInsensitiveResolution).get > 10,
        IsNotNull(tr1.resolveQuoted("a", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in right-outer join") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")
    verifyConstraints(tr1
      .where($"a".attr > 10)
      .join(tr2.where($"d".attr < 100), RightOuter, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr2.resolveQuoted("d", caseInsensitiveResolution).get < 100,
        IsNotNull(tr2.resolveQuoted("d", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in full-outer join") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")
    assert(tr1.where($"a".attr > 10)
      .join(tr2.where($"d".attr < 100), FullOuter, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints.isEmpty)
  }

  test("infer additional constraints in filters") {
    val tr = LocalRelation($"a".int, $"b".int, $"c".int)

    verifyConstraints(tr
      .where($"a".attr > 10 && $"a".attr === $"b".attr)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") > 10,
        resolveColumn(tr, "b") > 10,
        resolveColumn(tr, "a") === resolveColumn(tr, "b"),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")))))
  }

  test("infer constraints on cast") {
    val tr = LocalRelation($"a".int, $"b".long, $"c".int, $"d".long, $"e".int)
    verifyConstraints(
      tr.where($"a".attr === $"b".attr &&
        $"c".attr + 100 > $"d".attr &&
        IsNotNull(Cast(Cast(resolveColumn(tr, "e"), LongType), LongType))).analyze.constraints,
      ExpressionSet(Seq(
        castWithTimeZone(resolveColumn(tr, "a"), LongType) === resolveColumn(tr, "b"),
        castWithTimeZone(resolveColumn(tr, "c") + 100, LongType) > resolveColumn(tr, "d"),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")),
        IsNotNull(castWithTimeZone(castWithTimeZone(resolveColumn(tr, "e"), LongType), LongType)))))
  }

  test("infer isnotnull constraints from compound expressions") {
    val tr = LocalRelation($"a".int, $"b".long, $"c".int, $"d".long, $"e".int)
    verifyConstraints(
      tr.where($"a".attr + $"b".attr === $"c".attr &&
        IsNotNull(
          Cast(
            Cast(Cast(resolveColumn(tr, "e"), LongType), LongType), LongType))).analyze.constraints,
      ExpressionSet(Seq(
        castWithTimeZone(resolveColumn(tr, "a"), LongType) + resolveColumn(tr, "b") ===
          castWithTimeZone(resolveColumn(tr, "c"), LongType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "e")),
        IsNotNull(
          castWithTimeZone(castWithTimeZone(castWithTimeZone(
            resolveColumn(tr, "e"), LongType), LongType), LongType)))))

    verifyConstraints(
      tr.where(($"a".attr * $"b".attr + 100) === $"c".attr && $"d" / 10 === $"e")
        .analyze.constraints,
      ExpressionSet(Seq(
        castWithTimeZone(resolveColumn(tr, "a"), LongType) * resolveColumn(tr, "b") +
          castWithTimeZone(100, LongType) ===
            castWithTimeZone(resolveColumn(tr, "c"), LongType),
        castWithTimeZone(resolveColumn(tr, "d"), DoubleType) /
          castWithTimeZone(10, DoubleType) ===
            castWithTimeZone(resolveColumn(tr, "e"), DoubleType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")))))

    verifyConstraints(
      tr.where(($"a".attr * $"b".attr - 10) >= $"c".attr && $"d" / 10 < $"e").analyze.constraints,
      ExpressionSet(Seq(
        castWithTimeZone(resolveColumn(tr, "a"), LongType) * resolveColumn(tr, "b") -
          castWithTimeZone(10, LongType) >=
            castWithTimeZone(resolveColumn(tr, "c"), LongType),
        castWithTimeZone(resolveColumn(tr, "d"), DoubleType) /
          castWithTimeZone(10, DoubleType) <
            castWithTimeZone(resolveColumn(tr, "e"), DoubleType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")))))

    verifyConstraints(
      tr.where($"a".attr + $"b".attr - $"c".attr * $"d".attr > $"e".attr * 1000)
        .analyze.constraints,
      ExpressionSet(Seq(
        (castWithTimeZone(resolveColumn(tr, "a"), LongType) + resolveColumn(tr, "b")) -
          (castWithTimeZone(resolveColumn(tr, "c"), LongType) * resolveColumn(tr, "d")) >
            castWithTimeZone(resolveColumn(tr, "e") * 1000, LongType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")))))

    // The constraint IsNotNull(IsNotNull(expr)) doesn't guarantee expr is not null.
    verifyConstraints(
      tr.where($"a".attr === $"c".attr &&
        IsNotNull(IsNotNull(resolveColumn(tr, "b")))).analyze.constraints,
      ExpressionSet(Seq(
        resolveColumn(tr, "a") === resolveColumn(tr, "c"),
        IsNotNull(IsNotNull(resolveColumn(tr, "b"))),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "c")))))

    verifyConstraints(
      tr.where($"a".attr === 1 && IsNotNull(resolveColumn(tr, "b")) &&
        IsNotNull(resolveColumn(tr, "c"))).analyze.constraints,
      ExpressionSet(Seq(
        resolveColumn(tr, "a") === 1,
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")))))
  }

  test("infer IsNotNull constraints from non-nullable attributes") {
    val tr = LocalRelation($"a".int, AttributeReference("b", IntegerType, nullable = false)(),
      AttributeReference("c", StringType, nullable = false)())

    verifyConstraints(tr.analyze.constraints,
      ExpressionSet(Seq(IsNotNull(resolveColumn(tr, "b")), IsNotNull(resolveColumn(tr, "c")))))
  }

  test("not infer non-deterministic constraints") {
    val tr = LocalRelation($"a".int, $"b".string, $"c".int)

    verifyConstraints(tr
      .where($"a".attr === Rand(0))
      .analyze.constraints,
      ExpressionSet(Seq(IsNotNull(resolveColumn(tr, "a")))))

    verifyConstraints(tr
      .where($"a".attr === InputFileName())
      .where($"a".attr =!= $"c".attr)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") =!= resolveColumn(tr, "c"),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "c")))))
  }

  test("enable/disable constraint propagation") {
    val tr = LocalRelation($"a".int, $"b".string, $"c".int)
    val filterRelation = tr.where($"a".attr > 10)

    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "true") {
      assert(filterRelation.analyze.constraints.nonEmpty)
    }

    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      assert(filterRelation.analyze.constraints.isEmpty)
    }

    val aliasedRelation = tr.where($"c".attr > 10 && $"a".attr < 5)
      .groupBy($"a", $"c", $"b")($"a", $"c".as("c1"), count($"a").as("a3"))
      .select($"c1", $"a", $"a3")

    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "true") {
      assert(aliasedRelation.analyze.constraints.nonEmpty)
    }

    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      assert(aliasedRelation.analyze.constraints.isEmpty)
    }
  }

  test("infer range constraints by substituting attr=literal bindings") {
    // When a.pt = '20260610' (modeled as a.k = 5) and b.v >= a.k are both present,
    // inferAdditionalConstraints should emit b.v >= 5.
    val tr = LocalRelation($"k".int, $"v".int)
    val a = resolveColumn(tr, "k")
    val b = resolveColumn(tr, "v")

    val constraints = ExpressionSet(Seq(
      EqualTo(a, Literal(5)),
      GreaterThanOrEqual(b, a),
      LessThanOrEqual(b, Add(a, Literal(10)))))

    val helper = new ConstraintHelper {}
    val inferred = helper.inferConstraintsFromLiteralBindings(constraints)

    // b >= 5 must be inferred
    assert(inferred.exists {
      case GreaterThanOrEqual(attr: Attribute, Literal(v, IntegerType)) =>
        attr.semanticEquals(b) && v == 5
      case _ => false
    }, s"Expected b >= 5 in inferred constraints; got: $inferred")

    // b <= 15 must be inferred (a + 10 with a=5, i.e. Add(Literal(5), Literal(10)))
    assert(inferred.exists {
      case LessThanOrEqual(attr: Attribute, rhs) if attr.semanticEquals(b) =>
        // rhs is either Literal(15) or Add(Literal(5), Literal(10)) - both are foldable
        rhs.foldable && rhs.eval(null) == 15
      case _ => false
    }, s"Expected b <= 15 in inferred constraints; got: $inferred")
  }

  test("infer range constraints: literal on left side of EqualTo") {
    val tr = LocalRelation($"k".int, $"v".int)
    val a = resolveColumn(tr, "k")
    val b = resolveColumn(tr, "v")

    // Literal appears on the left: 5 = a.k
    val constraints = ExpressionSet(Seq(
      EqualTo(Literal(5), a),
      GreaterThanOrEqual(b, a)))

    val helper = new ConstraintHelper {}
    val inferred = helper.inferConstraintsFromLiteralBindings(constraints)

    assert(inferred.exists {
      case GreaterThanOrEqual(attr: Attribute, Literal(v, IntegerType)) =>
        attr.semanticEquals(b) && v == 5
      case _ => false
    }, s"Expected b >= 5 in inferred constraints; got: $inferred")
  }

  test("infer constraints by substituting attr=literal bindings into EqualTo expressions") {
    val tr = LocalRelation($"k".int, $"v".int)
    val a = resolveColumn(tr, "k")
    val b = resolveColumn(tr, "v")

    val constraints = ExpressionSet(Seq(
      EqualTo(a, Literal(5)),
      EqualTo(b, Add(a, Literal(1)))))

    val helper = new ConstraintHelper {}
    val inferred = helper.inferConstraintsFromLiteralBindings(constraints)

    // b = a + 1 with a = 5 should infer b = 5 + 1 (foldable to 6)
    assert(inferred.exists {
      case EqualTo(attr: Attribute, rhs) if attr.semanticEquals(b) =>
        rhs.foldable && rhs.eval(null) == 6
      case _ => false
    }, s"Expected b = 6 in inferred constraints; got: $inferred")
  }

  test("non-deterministic constraints inferred from literal substitution are filtered out") {
    val tr = LocalRelation($"k".int, $"v".int)
    val a = resolveColumn(tr, "k")
    val b = resolveColumn(tr, "v")

    // b >= a.k + Rand(); after substituting a.k=5 the result is non-deterministic.
    // inferAdditionalConstraints may emit it, but the plan-level constraints filter must drop it.
    val randExpr = Add(a, Cast(Rand(0L), IntegerType))
    val condition = EqualTo(a, Literal(5)) && GreaterThanOrEqual(b, randExpr)
    val plan = Filter(condition, tr.analyze)

    assert(!plan.analyze.constraints.exists {
      case GreaterThanOrEqual(attr: Attribute, _) => attr.semanticEquals(b)
      case _ => false
    }, s"Should not keep non-deterministic constraint in plan constraints; " +
      s"got: ${plan.analyze.constraints}")
  }

  test("do not infer constraints by substituting non-binary-stable collated attributes") {
    // a is UTF8_LCASE (case-insensitive). a = 'hello' only tells us a is case-insensitively
    // equal to 'hello'; it could still be 'HELLO'. Substituting a with 'hello' into a
    // UTF8_BINARY comparison like b >= a would infer b >= 'hello', which is wrong.
    val a = AttributeReference("a", StringType(CollationFactory.UTF8_LCASE_COLLATION_ID))()
    val b = AttributeReference("b", StringType)()
    val hello = Literal("hello")

    val constraints = ExpressionSet(Seq(
      EqualTo(a, hello),
      GreaterThanOrEqual(b, a)))

    val helper = new ConstraintHelper {}
    val inferred = helper.inferAdditionalConstraints(constraints)

    assert(!inferred.exists {
      case GreaterThanOrEqual(attr: Attribute, lit: Literal) =>
        attr.semanticEquals(b) && lit.semanticEquals(hello)
      case _ => false
    }, s"Should not infer b >= 'hello' for non-binary-stable collation; got: $inferred")
  }
}
