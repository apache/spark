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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

class ConstraintPropagationSuite extends SparkFunSuite {

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

  test("propagating constraints in filters") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)

    assert(tr.analyze.constraints.isEmpty)

    assert(tr.where('a.attr > 10).select('c.attr, 'b.attr).analyze.constraints.isEmpty)

    verifyConstraints(tr
      .where('a.attr > 10)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") > 10,
        IsNotNull(resolveColumn(tr, "a")))))

    verifyConstraints(tr
      .where('a.attr > 10)
      .select('c.attr, 'a.attr)
      .where('c.attr =!= 100)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") > 10,
        resolveColumn(tr, "c") =!= 100,
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "c")))))
  }

  test("propagating constraints in aggregate") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)

    assert(tr.analyze.constraints.isEmpty)

    val aliasedRelation = tr.where('c.attr > 10 && 'a.attr < 5)
      .groupBy('a, 'c, 'b)('a, 'c.as("c1"), count('a).as("a3")).select('c1, 'a, 'a3).analyze

    // SPARK-16644: aggregate expression count(a) should not appear in the constraints.
    verifyConstraints(aliasedRelation.analyze.constraints,
      ExpressionSet(Seq(resolveColumn(aliasedRelation.analyze, "c1") > 10,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "c1")),
        resolveColumn(aliasedRelation.analyze, "a") < 5,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "a")),
        IsNotNull(resolveColumn(aliasedRelation.analyze, "a3")))))
  }

  test("propagating constraints in expand") {
    val tr = LocalRelation('a.int, 'b.int, 'c.int)

    assert(tr.analyze.constraints.isEmpty)

    // We add IsNotNull constraints for 'a, 'b and 'c into LocalRelation
    // by creating notNullRelation.
    val notNullRelation = tr.where('c.attr > 10 && 'a.attr < 5 && 'b.attr > 2)
    verifyConstraints(notNullRelation.analyze.constraints,
      ExpressionSet(Seq(resolveColumn(notNullRelation.analyze, "c") > 10,
        IsNotNull(resolveColumn(notNullRelation.analyze, "c")),
        resolveColumn(notNullRelation.analyze, "a") < 5,
        IsNotNull(resolveColumn(notNullRelation.analyze, "a")),
        resolveColumn(notNullRelation.analyze, "b") > 2,
        IsNotNull(resolveColumn(notNullRelation.analyze, "b")))))

    val expand = Expand(
          Seq(
            Seq('c, Literal.create(null, StringType), 1),
            Seq('c, 'a, 2)),
          Seq('c, 'a, 'gid.int),
          Project(Seq('a, 'c),
            notNullRelation))
    verifyConstraints(expand.analyze.constraints,
      ExpressionSet(Seq.empty[Expression]))
  }

  test("propagating constraints in aliases") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)

    assert(tr.where('c.attr > 10).select('a.as('x), 'b.as('y)).analyze.constraints.isEmpty)

    val aliasedRelation = tr.where('a.attr > 10).select('a.as('x), 'b, 'b.as('y), 'a.as('z))

    verifyConstraints(aliasedRelation.analyze.constraints,
      ExpressionSet(Seq(resolveColumn(aliasedRelation.analyze, "x") > 10,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "x")),
        resolveColumn(aliasedRelation.analyze, "b") <=> resolveColumn(aliasedRelation.analyze, "y"),
        resolveColumn(aliasedRelation.analyze, "z") > 10,
        IsNotNull(resolveColumn(aliasedRelation.analyze, "z")))))
  }

  test("propagating constraints in union") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('d.int, 'e.int, 'f.int)
    val tr3 = LocalRelation('g.int, 'h.int, 'i.int)

    assert(tr1
      .where('a.attr > 10)
      .union(tr2.where('e.attr > 10)
      .union(tr3.where('i.attr > 10)))
      .analyze.constraints.isEmpty)

    verifyConstraints(tr1
      .where('a.attr > 10)
      .union(tr2.where('d.attr > 10)
      .union(tr3.where('g.attr > 10)))
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr1, "a") > 10,
        IsNotNull(resolveColumn(tr1, "a")))))

    val a = resolveColumn(tr1, "a")
    verifyConstraints(tr1
      .where('a.attr > 10)
      .union(tr2.where('d.attr > 11))
      .analyze.constraints,
      ExpressionSet(Seq(a > 10 || a > 11, IsNotNull(a))))

    val b = resolveColumn(tr1, "b")
    verifyConstraints(tr1
      .where('a.attr > 10 && 'b.attr < 10)
      .union(tr2.where('d.attr > 11 && 'e.attr < 11))
      .analyze.constraints,
      ExpressionSet(Seq(a > 10 || a > 11, b < 10 || b < 11, IsNotNull(a), IsNotNull(b))))
  }

  test("propagating constraints in intersect") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int)

    verifyConstraints(tr1
      .where('a.attr > 10)
      .intersect(tr2.where('b.attr < 100))
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr1, "a") > 10,
        resolveColumn(tr1, "b") < 100,
        IsNotNull(resolveColumn(tr1, "a")),
        IsNotNull(resolveColumn(tr1, "b")))))
  }

  test("propagating constraints in except") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('a.int, 'b.int, 'c.int)
    verifyConstraints(tr1
      .where('a.attr > 10)
      .except(tr2.where('b.attr < 100))
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr1, "a") > 10,
        IsNotNull(resolveColumn(tr1, "a")))))
  }

  test("propagating constraints in inner join") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)
    verifyConstraints(tr1
      .where('a.attr > 10)
      .join(tr2.where('d.attr < 100), Inner, Some("tr1.a".attr === "tr2.a".attr))
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
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)
    verifyConstraints(tr1
      .where('a.attr > 10)
      .join(tr2.where('d.attr < 100), LeftSemi, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr1.resolveQuoted("a", caseInsensitiveResolution).get > 10,
        IsNotNull(tr1.resolveQuoted("a", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in left-outer join") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)
    verifyConstraints(tr1
      .where('a.attr > 10)
      .join(tr2.where('d.attr < 100), LeftOuter, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr1.resolveQuoted("a", caseInsensitiveResolution).get > 10,
        IsNotNull(tr1.resolveQuoted("a", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in right-outer join") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)
    verifyConstraints(tr1
      .where('a.attr > 10)
      .join(tr2.where('d.attr < 100), RightOuter, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints,
      ExpressionSet(Seq(tr2.resolveQuoted("d", caseInsensitiveResolution).get < 100,
        IsNotNull(tr2.resolveQuoted("d", caseInsensitiveResolution).get))))
  }

  test("propagating constraints in full-outer join") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)
    assert(tr1.where('a.attr > 10)
      .join(tr2.where('d.attr < 100), FullOuter, Some("tr1.a".attr === "tr2.a".attr))
      .analyze.constraints.isEmpty)
  }

  test("infer additional constraints in filters") {
    val tr = LocalRelation('a.int, 'b.int, 'c.int)

    verifyConstraints(tr
      .where('a.attr > 10 && 'a.attr === 'b.attr)
      .analyze.constraints,
      ExpressionSet(Seq(resolveColumn(tr, "a") > 10,
        resolveColumn(tr, "b") > 10,
        resolveColumn(tr, "a") === resolveColumn(tr, "b"),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")))))
  }

  test("infer constraints on cast") {
    val tr = LocalRelation('a.int, 'b.long, 'c.int, 'd.long, 'e.int)
    verifyConstraints(
      tr.where('a.attr === 'b.attr &&
        'c.attr + 100 > 'd.attr &&
        IsNotNull(Cast(Cast(resolveColumn(tr, "e"), LongType), LongType))).analyze.constraints,
      ExpressionSet(Seq(Cast(resolveColumn(tr, "a"), LongType) === resolveColumn(tr, "b"),
        Cast(resolveColumn(tr, "c") + 100, LongType) > resolveColumn(tr, "d"),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")),
        IsNotNull(Cast(Cast(resolveColumn(tr, "e"), LongType), LongType)))))
  }

  test("infer isnotnull constraints from compound expressions") {
    val tr = LocalRelation('a.int, 'b.long, 'c.int, 'd.long, 'e.int)
    verifyConstraints(
      tr.where('a.attr + 'b.attr === 'c.attr &&
        IsNotNull(
          Cast(
            Cast(Cast(resolveColumn(tr, "e"), LongType), LongType), LongType))).analyze.constraints,
      ExpressionSet(Seq(
        Cast(resolveColumn(tr, "a"), LongType) + resolveColumn(tr, "b") ===
          Cast(resolveColumn(tr, "c"), LongType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "e")),
        IsNotNull(Cast(Cast(Cast(resolveColumn(tr, "e"), LongType), LongType), LongType)))))

    verifyConstraints(
      tr.where(('a.attr * 'b.attr + 100) === 'c.attr && 'd / 10 === 'e).analyze.constraints,
      ExpressionSet(Seq(
        Cast(resolveColumn(tr, "a"), LongType) * resolveColumn(tr, "b") + Cast(100, LongType) ===
          Cast(resolveColumn(tr, "c"), LongType),
        Cast(resolveColumn(tr, "d"), DoubleType) /
          Cast(10, DoubleType) ===
            Cast(resolveColumn(tr, "e"), DoubleType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")))))

    verifyConstraints(
      tr.where(('a.attr * 'b.attr - 10) >= 'c.attr && 'd / 10 < 'e).analyze.constraints,
      ExpressionSet(Seq(
        Cast(resolveColumn(tr, "a"), LongType) * resolveColumn(tr, "b") - Cast(10, LongType) >=
          Cast(resolveColumn(tr, "c"), LongType),
        Cast(resolveColumn(tr, "d"), DoubleType) /
          Cast(10, DoubleType) <
            Cast(resolveColumn(tr, "e"), DoubleType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")))))

    verifyConstraints(
      tr.where('a.attr + 'b.attr - 'c.attr * 'd.attr > 'e.attr * 1000).analyze.constraints,
      ExpressionSet(Seq(
        (Cast(resolveColumn(tr, "a"), LongType) + resolveColumn(tr, "b")) -
          (Cast(resolveColumn(tr, "c"), LongType) * resolveColumn(tr, "d")) >
            Cast(resolveColumn(tr, "e") * 1000, LongType),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "b")),
        IsNotNull(resolveColumn(tr, "c")),
        IsNotNull(resolveColumn(tr, "d")),
        IsNotNull(resolveColumn(tr, "e")))))

    // The constraint IsNotNull(IsNotNull(expr)) doesn't guarantee expr is not null.
    verifyConstraints(
      tr.where('a.attr === 'c.attr &&
        IsNotNull(IsNotNull(resolveColumn(tr, "b")))).analyze.constraints,
      ExpressionSet(Seq(
        resolveColumn(tr, "a") === resolveColumn(tr, "c"),
        IsNotNull(IsNotNull(resolveColumn(tr, "b"))),
        IsNotNull(resolveColumn(tr, "a")),
        IsNotNull(resolveColumn(tr, "c")))))
  }

  test("infer IsNotNull constraints from non-nullable attributes") {
    val tr = LocalRelation('a.int, AttributeReference("b", IntegerType, nullable = false)(),
      AttributeReference("c", StringType, nullable = false)())

    verifyConstraints(tr.analyze.constraints,
      ExpressionSet(Seq(IsNotNull(resolveColumn(tr, "b")), IsNotNull(resolveColumn(tr, "c")))))
  }
}
