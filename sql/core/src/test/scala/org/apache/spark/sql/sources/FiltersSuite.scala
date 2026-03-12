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

package org.apache.spark.sql.sources

import org.apache.spark.SparkFunSuite

/**
 * Unit test suites for data source filters.
 */
class FiltersSuite extends SparkFunSuite {

  private def withFieldNames(f: (String, Array[String]) => Unit): Unit = {
    Seq(("a", Array("a")),
      ("a.b", Array("a", "b")),
      ("`a.b`.c", Array("a.b", "c")),
      ("`a.b`.`c.d`.`e.f`", Array("a.b", "c.d", "e.f"))
    ).foreach { case (name, fieldNames) =>
      f(name, fieldNames)
    }
  }

  test("EqualTo references") { withFieldNames { (name, fieldNames) =>
    assert(EqualTo(name, "1").references.toSeq == Seq(name))
    assert(EqualTo(name, "1").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(EqualTo(name, EqualTo("b", "2")).references.toSeq == Seq(name, "b"))
    assert(EqualTo("b", EqualTo(name, "2")).references.toSeq == Seq("b", name))

    assert(EqualTo(name, EqualTo("b", "2")).v2references.toSeq.map(_.toSeq)
      == Seq(fieldNames.toSeq, Seq("b")))
    assert(EqualTo("b", EqualTo(name, "2")).v2references.toSeq.map(_.toSeq)
      == Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("EqualNullSafe references") { withFieldNames { (name, fieldNames) =>
    assert(EqualNullSafe(name, "1").references.toSeq == Seq(name))
    assert(EqualNullSafe(name, "1").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(EqualNullSafe(name, EqualTo("b", "2")).references.toSeq == Seq(name, "b"))
    assert(EqualNullSafe("b", EqualTo(name, "2")).references.toSeq == Seq("b", name))

    assert(EqualNullSafe(name, EqualTo("b", "2")).v2references.toSeq.map(_.toSeq)
      == Seq(fieldNames.toSeq, Seq("b")))
    assert(EqualNullSafe("b", EqualTo(name, "2")).v2references.toSeq.map(_.toSeq)
      == Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("GreaterThan references") { withFieldNames { (name, fieldNames) =>
    assert(GreaterThan(name, "1").references.toSeq == Seq(name))
    assert(GreaterThan(name, "1").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(GreaterThan(name, EqualTo("b", "2")).references.toSeq == Seq(name, "b"))
    assert(GreaterThan("b", EqualTo(name, "2")).references.toSeq == Seq("b", name))

    assert(GreaterThan(name, EqualTo("b", "2")).v2references.toSeq.map(_.toSeq)
      == Seq(fieldNames.toSeq, Seq("b")))
    assert(GreaterThan("b", EqualTo(name, "2")).v2references.toSeq.map(_.toSeq)
      == Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("GreaterThanOrEqual references") { withFieldNames { (name, fieldNames) =>
    assert(GreaterThanOrEqual(name, "1").references.toSeq == Seq(name))
    assert(GreaterThanOrEqual(name, "1").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(GreaterThanOrEqual(name, EqualTo("b", "2")).references.toSeq == Seq(name, "b"))
    assert(GreaterThanOrEqual("b", EqualTo(name, "2")).references.toSeq == Seq("b", name))

    assert(GreaterThanOrEqual(name, EqualTo("b", "2")).v2references.toSeq.map(_.toSeq)
      == Seq(fieldNames.toSeq, Seq("b")))
    assert(GreaterThanOrEqual("b", EqualTo(name, "2")).v2references.toSeq.map(_.toSeq)
      == Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("LessThan references") { withFieldNames { (name, fieldNames) =>
    assert(LessThan(name, "1").references.toSeq == Seq(name))
    assert(LessThan(name, "1").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(LessThan("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }}

  test("LessThanOrEqual references") { withFieldNames { (name, fieldNames) =>
    assert(LessThanOrEqual(name, "1").references.toSeq == Seq(name))
    assert(LessThanOrEqual(name, "1").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(LessThanOrEqual(name, EqualTo("b", "2")).references.toSeq == Seq(name, "b"))
    assert(LessThanOrEqual("b", EqualTo(name, "2")).references.toSeq == Seq("b", name))

    assert(LessThanOrEqual(name, EqualTo("b", "2")).v2references.toSeq.map(_.toSeq)
      == Seq(fieldNames.toSeq, Seq("b")))
    assert(LessThanOrEqual("b", EqualTo(name, "2")).v2references.toSeq.map(_.toSeq)
      == Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("In references") { withFieldNames { (name, fieldNames) =>
    assert(In(name, Array("1")).references.toSeq == Seq(name))
    assert(In(name, Array("1")).v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))

    assert(In(name, Array("1", EqualTo("b", "2"))).references.toSeq == Seq(name, "b"))
    assert(In("b", Array("1", EqualTo(name, "2"))).references.toSeq == Seq("b", name))

    assert(In(name, Array("1", EqualTo("b", "2"))).v2references.toSeq.map(_.toSeq)
      == Seq(fieldNames.toSeq, Seq("b")))
    assert(In("b", Array("1", EqualTo(name, "2"))).v2references.toSeq.map(_.toSeq)
      == Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("IsNull references") { withFieldNames { (name, fieldNames) =>
    assert(IsNull(name).references.toSeq == Seq(name))
    assert(IsNull(name).v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))
  }}

  test("IsNotNull references") { withFieldNames { (name, fieldNames) =>
    assert(IsNotNull(name).references.toSeq == Seq(name))
    assert(IsNull(name).v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))
  }}

  test("And references") { withFieldNames { (name, fieldNames) =>
    assert(And(EqualTo(name, "1"), EqualTo("b", "1")).references.toSeq == Seq(name, "b"))
    assert(And(EqualTo("b", "1"), EqualTo(name, "1")).references.toSeq == Seq("b", name))

    assert(And(EqualTo(name, "1"), EqualTo("b", "1")).v2references.toSeq.map(_.toSeq) ==
      Seq(fieldNames.toSeq, Seq("b")))
    assert(And(EqualTo("b", "1"), EqualTo(name, "1")).v2references.toSeq.map(_.toSeq) ==
      Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("Or references") { withFieldNames { (name, fieldNames) =>
    assert(Or(EqualTo(name, "1"), EqualTo("b", "1")).references.toSeq == Seq(name, "b"))
    assert(Or(EqualTo("b", "1"), EqualTo(name, "1")).references.toSeq == Seq("b", name))

    assert(Or(EqualTo(name, "1"), EqualTo("b", "1")).v2references.toSeq.map(_.toSeq) ==
      Seq(fieldNames.toSeq, Seq("b")))
    assert(Or(EqualTo("b", "1"), EqualTo(name, "1")).v2references.toSeq.map(_.toSeq) ==
      Seq(Seq("b"), fieldNames.toSeq))
  }}

  test("StringStartsWith references") { withFieldNames { (name, fieldNames) =>
    assert(StringStartsWith(name, "str").references.toSeq == Seq(name))
    assert(StringStartsWith(name, "str").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))
  }}

  test("StringEndsWith references") { withFieldNames { (name, fieldNames) =>
    assert(StringEndsWith(name, "str").references.toSeq == Seq(name))
    assert(StringEndsWith(name, "str").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))
  }}

  test("StringContains references") { withFieldNames { (name, fieldNames) =>
    assert(StringContains(name, "str").references.toSeq == Seq(name))
    assert(StringContains(name, "str").v2references.toSeq.map(_.toSeq) == Seq(fieldNames.toSeq))
  }}
}
