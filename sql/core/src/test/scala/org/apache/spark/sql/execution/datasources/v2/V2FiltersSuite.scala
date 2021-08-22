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

  test("References with nested columns") {
    assert(new EqualTo(ref("a", "B"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a.B"))
    assert(new EqualTo(ref("a", "b.c"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a.`b.c`"))
    assert(new EqualTo(ref("`a`.b", "c"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("```a``.b`.c"))
  }

  test("EqualTo references") {
    assert(new EqualTo(ref("a"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new EqualTo(ref("a"), new EqualTo(ref("b"), LiteralValue(1, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("EqualNullSafe references") {
    assert(new EqualNullSafe(ref("a"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new EqualNullSafe(ref("a"), new EqualTo(ref("b"), LiteralValue(2, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("GreaterThan references") {
    assert(new GreaterThan(ref("a"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new GreaterThan(ref("a"), new EqualTo(ref("b"), LiteralValue(2, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("GreaterThanOrEqual references") {
    assert(new GreaterThanOrEqual(ref("a"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new GreaterThanOrEqual(ref("a"), new EqualTo(ref("b"), LiteralValue(1, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("LessThan references") {
    assert(new LessThan(ref("a"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new LessThan(ref("a"), new EqualTo(ref("b"), LiteralValue(2, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("LessThanOrEqual references") {
    assert(new LessThanOrEqual(ref("a"), LiteralValue(1, IntegerType))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new LessThanOrEqual(ref("a"), new EqualTo(ref("b"), LiteralValue(2, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("In references") {
    assert(new In(ref("a"), Array(LiteralValue(1, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a"))
    assert(new In(ref("a"),
      Array(LiteralValue(1, IntegerType), new EqualTo(ref("b"), LiteralValue(2, IntegerType)),
        new EqualTo(ref("c"), new EqualTo(ref("d"), LiteralValue(3, IntegerType)))))
      .references.map(_.describe()).toSeq == Seq("a", "b", "c", "d"))
  }

  test("IsNull references") {
    assert(new IsNull(ref("a")).references.map(_.describe()).toSeq == Seq("a"))
  }

  test("IsNotNull references") {
    assert(new IsNotNull(ref("a")).references.map(_.describe()).toSeq == Seq("a"))
  }

  test("And references") {
    assert(new And(new EqualTo(ref("a"), LiteralValue(1, IntegerType)),
      new EqualTo(ref("b"), LiteralValue(1, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("Or references") {
    assert(new Or(new EqualTo(ref("a"), LiteralValue(1, IntegerType)),
      new EqualTo(ref("b"), LiteralValue(1, IntegerType)))
      .references.map(_.describe()).toSeq == Seq("a", "b"))
  }

  test("StringStartsWith references") {
    assert(new StringStartsWith(ref("a"), "str").references.map(_.describe()).toSeq == Seq("a"))
  }

  test("StringEndsWith references") {
    assert(new StringEndsWith(ref("a"), "str").references.map(_.describe()).toSeq == Seq("a"))
  }

  test("StringContains references") {
    assert(new StringContains(ref("a"), "str").references.map(_.describe()).toSeq == Seq("a"))
  }
}

object FiltersV2Suite {
  private[sql] def ref(parts: String*): FieldReference = {
    new FieldReference(parts)
  }
}


