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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Coalesce, CollationKey, EvalMode, Expression, IsNull, Literal, RaiseError}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation}
import org.apache.spark.sql.types.{IntegerType, StringType}

class ExtractEquiJoinKeysSuite extends SparkFunSuite {

  private val c = AttributeReference("c", StringType("UTF8_LCASE"))()
  private val t = AttributeReference("t", StringType("UTF8_LCASE"))()
  private val left = LocalRelation($"a".int, $"s".string, c)
  private val right = LocalRelation($"z".int, t)
  private val Seq(a, s, _) = left.output
  private val z = right.output.head
  // The cast is built in ANSI mode, where it can fail.
  private val cast = Cast(s, IntegerType, evalMode = EvalMode.ANSI)
  private val throwing = IsNull(RaiseError(Literal("boom")))

  private def keysOf(join: Join): (Seq[Expression], Seq[Expression], Option[Expression]) =
    join match {
      case ExtractEquiJoinKeys(_, leftKeys, rightKeys, otherCondition, _, _, _, _) =>
        (leftKeys, rightKeys, otherCondition)
      case other => fail(s"Expected an equi-join, got $other")
    }

  private def join(condition: Expression): Join =
    left.join(right, Inner, Some(condition)).asInstanceOf[Join]

  test("SPARK-62241: an equality that can fail after a throwable conjunct is not a join key") {
    // A join computes its keys on every row, so after the throwable conjunct the equality on the
    // cast is not a key, and stays after the throwable conjunct in the other condition. `a = z`,
    // which can not fail, is a key. Every equality is a key of `ExtractAllEquiJoinKeys`.
    val condition = throwing && cast === z && a === z
    assert(keysOf(join(condition)) === ((Seq(a), Seq(z), Some(throwing && cast === z))))
    join(condition) match {
      case ExtractAllEquiJoinKeys(_, leftKeys, rightKeys, otherCondition, _, _, _, _) =>
        assert(leftKeys === Seq(cast, a))
        assert(rightKeys === Seq(z, z))
        assert(otherCondition === Some(throwing))
      case other => fail(s"Expected an equi-join, got $other")
    }
    // Written before the throwable conjunct, the equality on the cast is a key.
    assert(keysOf(join(cast === z && throwing)) === ((Seq(cast), Seq(z), Some(throwing))))
  }

  test("SPARK-62241: the first conjunct may be a join key, also when it is throwable") {
    // The first conjunct is evaluated first on every pair anyway. After it, only an equality that
    // can not fail is a key.
    val throwingKey = Coalesce(Seq(z, Cast(RaiseError(Literal("missing")), IntegerType)))
    assert(keysOf(join(a === throwingKey && cast === z && a === z)) ===
      ((Seq(a, a), Seq(throwingKey, z), Some(cast === z))))
  }

  test("SPARK-62241: a collation key after a throwable conjunct is a join key") {
    // `RewriteCollationJoin` compares strings of a non-binary collation by their collation keys,
    // which can not fail.
    assert(keysOf(join(throwing && CollationKey(c) === CollationKey(t))) ===
      ((Seq(CollationKey(c)), Seq(CollationKey(t)), Some(throwing))))
  }
}
