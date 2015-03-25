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
package org.apache.spark.util

import org.scalatest.{Matchers, FunSuite}

import org.apache.spark.SparkException

class SparkEnumSuite extends FunSuite with Matchers {

  test("toString") {
    DummyEnum.Foo.toString should be ("Foo")
    DummyEnum.Bar.toString should be ("Bar")
  }

  test("parse") {
    DummyEnum.parse("Foo") should be (Some(DummyEnum.Foo))
    DummyEnum.parse("Bar") should be (Some(DummyEnum.Bar))

    DummyEnum.parse("") should be (None)
    DummyEnum.parse("foo") should be (None)
    DummyEnum.parse("bar") should be (None)
  }


  test("bad enums") {
    val ex = intercept[SparkException](BadEnum.enumNames)
    // I get different errors on each run, not sure why, but either is fine.
    ex.getMessage should (be ("It appears you have multiple constants with the same name.  " +
      "Perhaps your naming scheme is incompatible with SparkEnum. found names: Set(Bippy)") or
      be ("It appears you are using SparkEnum in a class which does not follow the naming" +
        " conventions"))
  }

  test("parseIgnoreCase") {
    DummyEnum.parseIgnoreCase("Foo") should be (Some(DummyEnum.Foo))
    DummyEnum.parseIgnoreCase("Bar") should be (Some(DummyEnum.Bar))

    DummyEnum.parseIgnoreCase("") should be (None)
    DummyEnum.parseIgnoreCase("foo") should be (Some(DummyEnum.Foo))
    DummyEnum.parseIgnoreCase("bar") should be (Some(DummyEnum.Bar))
  }
}


sealed abstract class DummyEnum extends SparkEnum

object DummyEnum extends SparkEnumCompanion[DummyEnum] {
  final val Foo = {
    case object Foo extends DummyEnum
    Foo
  }
  final val Bar = {
    case object Bar extends DummyEnum
    Bar
  }
  val values = Seq(
    Foo,
    Bar
  )
}

sealed abstract class BadEnum extends SparkEnum

object BadEnum extends SparkEnumCompanion[BadEnum] {
  case object Bippy extends BadEnum
  object Blah {
    case object Bippy extends BadEnum
  }

  val values = Seq(Bippy, Blah.Bippy)
}
