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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter._
import org.apache.spark.sql.execution.datasources.v2.FiltersV2Suite.ref
import org.apache.spark.sql.types.IntegerType

class FiltersV2Suite extends SparkFunSuite {

  test("nested columns") {
    val filter1 = new EqualTo(ref("a", "B"), LiteralValue(1, IntegerType))
    assert(filter1.references.map(_.describe()).toSeq == Seq("a.B"))
    assert(filter1.describe.equals("a.B = 1"))

    val filter2 = new EqualTo(ref("a", "b.c"), LiteralValue(1, IntegerType))
    assert(filter2.references.map(_.describe()).toSeq == Seq("a.`b.c`"))
    assert(filter2.describe.equals("a.`b.c` = 1"))

    val filter3 = new EqualTo(ref("`a`.b", "c"), LiteralValue(1, IntegerType))
    assert(filter3.references.map(_.describe()).toSeq == Seq("```a``.b`.c"))
    assert(filter3.describe.equals("```a``.b`.c = 1"))
  }

  test("EqualTo") {
    val filter = new EqualTo(ref("a"), LiteralValue(1, IntegerType))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a = 1"))
  }

  test("EqualNullSafe") {
    val filter = new EqualNullSafe(ref("a"), LiteralValue(1, IntegerType))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a <=> 1"))
  }

  test("GreaterThan") {
    val filter = new GreaterThan(ref("a"), LiteralValue(1, IntegerType))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a > 1"))
  }

  test("GreaterThanOrEqual") {
    val filter = new GreaterThanOrEqual(ref("a"), LiteralValue(1, IntegerType))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a >= 1"))
  }

  test("LessThan") {
    val filter = new LessThan(ref("a"), LiteralValue(1, IntegerType))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a < 1"))
  }

  test("LessThanOrEqual") {
    val filter = new LessThanOrEqual(ref("a"), LiteralValue(1, IntegerType))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a <= 1"))
  }

  test("In") {
    val filter1 = new In(ref("a"),
      Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType),
        LiteralValue(3, IntegerType), LiteralValue(4, IntegerType)))
    val filter2 = new In(ref("a"),
      Array(LiteralValue(1, IntegerType), LiteralValue(2, IntegerType),
        LiteralValue(3, IntegerType), LiteralValue(4, IntegerType)))
    assert(filter1.equals(filter2))
    assert(filter1.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter1.describe.equals("a IN (1, 2, 3, 4)"))
  }

  test("IsNull") {
    val filter = new IsNull(ref("a"))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a IS NULL"))
  }

  test("IsNotNull") {
    val filter = new IsNotNull(ref("a"))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("a IS NOT NULL"))
  }

  test("Not") {
    val filter = new Not(new LessThan(ref("a"), LiteralValue(1, IntegerType)))
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("NOT a < 1"))
  }

  test("And") {
    val filter = new And(new EqualTo(ref("a"), LiteralValue(1, IntegerType)),
      new EqualTo(ref("b"), LiteralValue(1, IntegerType)))
    assert(filter.references.map(_.describe()).toSeq == Seq("a", "b"))
    assert(filter.describe.equals("(a = 1) AND (b = 1)"))
  }

  test("Or") {
    val filter = new Or(new EqualTo(ref("a"), LiteralValue(1, IntegerType)),
      new EqualTo(ref("b"), LiteralValue(1, IntegerType)))
    assert(filter.references.map(_.describe()).toSeq == Seq("a", "b"))
    assert(filter.describe.equals("(a = 1) AND (b = 1)"))
  }

  test("StringStartsWith") {
    val filter = new StringStartsWith(ref("a"), "str")
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("STRING_STARTS_WITH(a, str)"))
  }

  test("StringEndsWith") {
    val filter = new StringEndsWith(ref("a"), "str")
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("STRING_ENDS_WITH(a, str)"))
  }

  test("StringContains") {
    val filter = new StringContains(ref("a"), "str")
    assert(filter.references.map(_.describe()).toSeq == Seq("a"))
    assert(filter.describe.equals("STRING_CONTAINS(a, str)"))
  }
}

object FiltersV2Suite {
  private[sql] def ref(parts: String*): FieldReference = {
    new FieldReference(parts)
  }
}
