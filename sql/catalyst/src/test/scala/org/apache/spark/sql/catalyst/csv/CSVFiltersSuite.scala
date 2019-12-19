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

package org.apache.spark.sql.catalyst.csv

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.IntegerType

class CSVFiltersSuite extends SparkFunSuite {
  test("filter to expression conversion") {
    val ref = BoundReference(0, IntegerType, true)
    def check(f: Filter, expr: Expression): Unit = {
      assert(CSVFilters.filterToExpression(f, _ => Some(ref)).get === expr)
    }

    check(sources.AlwaysTrue, Literal(true))
    check(sources.AlwaysFalse, Literal(false))
    check(sources.IsNull("a"), IsNull(ref))
    check(sources.Not(sources.IsNull("a")), Not(IsNull(ref)))
    check(sources.IsNotNull("a"), IsNotNull(ref))
    check(sources.EqualTo("a", "b"), EqualTo(ref, Literal("b")))
    check(sources.EqualNullSafe("a", "b"), EqualNullSafe(ref, Literal("b")))
    check(sources.StringStartsWith("a", "b"), StartsWith(ref, Literal("b")))
    check(sources.StringEndsWith("a", "b"), EndsWith(ref, Literal("b")))
    check(sources.StringContains("a", "b"), Contains(ref, Literal("b")))
    check(sources.LessThanOrEqual("a", 1), LessThanOrEqual(ref, Literal(1)))
    check(sources.LessThan("a", 1), LessThan(ref, Literal(1)))
    check(sources.GreaterThanOrEqual("a", 1), GreaterThanOrEqual(ref, Literal(1)))
    check(sources.GreaterThan("a", 1), GreaterThan(ref, Literal(1)))
    check(sources.And(sources.AlwaysTrue, sources.AlwaysTrue), And(Literal(true), Literal(true)))
    check(sources.Or(sources.AlwaysTrue, sources.AlwaysTrue), Or(Literal(true), Literal(true)))
    check(sources.In("a", Array(1)), In(ref, Seq(Literal(1))))
  }
}
