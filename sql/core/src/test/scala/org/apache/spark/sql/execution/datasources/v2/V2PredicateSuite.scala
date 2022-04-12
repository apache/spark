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
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, Literal, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter._
import org.apache.spark.sql.execution.datasources.v2.V2PredicateSuite.ref
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class V2PredicateSuite extends SparkFunSuite {

  test("nested columns") {
    val predicate1 =
      new Predicate("=", Array[Expression](ref("a", "B"), LiteralValue(1, IntegerType)))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a.B"))
    assert(predicate1.describe.equals("a.B = 1"))

    val predicate2 =
      new Predicate("=", Array[Expression](ref("a", "b.c"), LiteralValue(1, IntegerType)))
    assert(predicate2.references.map(_.describe()).toSeq == Seq("a.`b.c`"))
    assert(predicate2.describe.equals("a.`b.c` = 1"))

    val predicate3 =
      new Predicate("=", Array[Expression](ref("`a`.b", "c"), LiteralValue(1, IntegerType)))
    assert(predicate3.references.map(_.describe()).toSeq == Seq("```a``.b`.c"))
    assert(predicate3.describe.equals("```a``.b`.c = 1"))
  }

  test("AlwaysTrue") {
    val predicate1 = new AlwaysTrue
    val predicate2 = new AlwaysTrue
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).length == 0)
    assert(predicate1.describe.equals("TRUE"))
  }

  test("AlwaysFalse") {
    val predicate1 = new AlwaysFalse
    val predicate2 = new AlwaysFalse
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).length == 0)
    assert(predicate1.describe.equals("FALSE"))
  }

  test("EqualTo") {
    val predicate1 = new Predicate("=", Array[Expression](ref("a"), LiteralValue(1, IntegerType)))
    val predicate2 = new Predicate("=", Array[Expression](ref("a"), LiteralValue(1, IntegerType)))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a = 1"))
  }

  test("EqualNullSafe") {
    val predicate1 = new Predicate("<=>", Array[Expression](ref("a"), LiteralValue(1, IntegerType)))
    val predicate2 = new Predicate("<=>", Array[Expression](ref("a"), LiteralValue(1, IntegerType)))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("(a = 1) OR (a IS NULL AND 1 IS NULL)"))
  }

  test("In") {
    val predicate1 = new Predicate("IN",
      Array(ref("a"), LiteralValue(1, IntegerType), LiteralValue(2, IntegerType),
        LiteralValue(3, IntegerType), LiteralValue(4, IntegerType)))
    val predicate2 = new Predicate("IN",
      Array(ref("a"), LiteralValue(4, IntegerType), LiteralValue(2, IntegerType),
        LiteralValue(3, IntegerType), LiteralValue(1, IntegerType)))
    assert(!predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a IN (1, 2, 3, 4)"))
    val values: Array[Literal[_]] = new Array[Literal[_]](1000)
    var expected = "a IN ("
    for (i <- 0 until 1000) {
      values(i) = LiteralValue(i, IntegerType)
      expected += i + ", "
    }
    val predicate3 = new Predicate("IN", (ref("a") +: values).toArray[Expression])
    expected = expected.dropRight(2)  // remove the last ", "
    expected += ")"
    assert(predicate3.describe.equals(expected))
  }

  test("IsNull") {
    val predicate1 = new Predicate("IS_NULL", Array[Expression](ref("a")))
    val predicate2 = new Predicate("IS_NULL", Array[Expression](ref("a")))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a IS NULL"))
  }

  test("IsNotNull") {
    val predicate1 = new Predicate("IS_NOT_NULL", Array[Expression](ref("a")))
    val predicate2 = new Predicate("IS_NOT_NULL", Array[Expression](ref("a")))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a IS NOT NULL"))
  }

  test("Not") {
    val predicate1 = new Not(
      new Predicate("<", Array[Expression](ref("a"), LiteralValue(1, IntegerType))))
    val predicate2 = new Not(
      new Predicate("<", Array[Expression](ref("a"), LiteralValue(1, IntegerType))))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("NOT (a < 1)"))
  }

  test("And") {
    val predicate1 = new And(
      new Predicate("=", Array[Expression](ref("a"), LiteralValue(1, IntegerType))),
      new Predicate("=", Array[Expression](ref("b"), LiteralValue(1, IntegerType))))
    val predicate2 = new And(
      new Predicate("=", Array[Expression](ref("a"), LiteralValue(1, IntegerType))),
      new Predicate("=", Array[Expression](ref("b"), LiteralValue(1, IntegerType))))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a", "b"))
    assert(predicate1.describe.equals("(a = 1) AND (b = 1)"))
  }

  test("Or") {
    val predicate1 = new Or(
      new Predicate("=", Array[Expression](ref("a"), LiteralValue(1, IntegerType))),
      new Predicate("=", Array[Expression](ref("b"), LiteralValue(1, IntegerType))))
    val predicate2 = new Or(
      new Predicate("=", Array[Expression](ref("a"), LiteralValue(1, IntegerType))),
      new Predicate("=", Array[Expression](ref("b"), LiteralValue(1, IntegerType))))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a", "b"))
    assert(predicate1.describe.equals("(a = 1) OR (b = 1)"))
  }

  test("StringStartsWith") {
    val literal = LiteralValue(UTF8String.fromString("str"), StringType)
    val predicate1 = new Predicate("STARTS_WITH",
      Array[Expression](ref("a"), literal))
    val predicate2 = new Predicate("STARTS_WITH",
      Array[Expression](ref("a"), literal))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a LIKE 'str%'"))
  }

  test("StringEndsWith") {
    val literal = LiteralValue(UTF8String.fromString("str"), StringType)
    val predicate1 = new Predicate("ENDS_WITH",
      Array[Expression](ref("a"), literal))
    val predicate2 = new Predicate("ENDS_WITH",
      Array[Expression](ref("a"), literal))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a LIKE '%str'"))
  }

  test("StringContains") {
    val literal = LiteralValue(UTF8String.fromString("str"), StringType)
    val predicate1 = new Predicate("CONTAINS",
      Array[Expression](ref("a"), literal))
    val predicate2 = new Predicate("CONTAINS",
      Array[Expression](ref("a"), literal))
    assert(predicate1.equals(predicate2))
    assert(predicate1.references.map(_.describe()).toSeq == Seq("a"))
    assert(predicate1.describe.equals("a LIKE '%str%'"))
  }
}

object V2PredicateSuite {
  private[sql] def ref(parts: String*): FieldReference = {
    new FieldReference(parts)
  }
}
