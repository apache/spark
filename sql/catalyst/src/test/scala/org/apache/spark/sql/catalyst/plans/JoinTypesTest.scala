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

class JoinTypesTest extends SparkFunSuite {

  test("construct an Inner type") {
    assert(JoinType.apply("inner") === Inner)
  }

  test("construct a FullOuter type") {
    assert(JoinType.apply("fullouter") === FullOuter)
    assert(JoinType.apply("full_outer") === FullOuter)
    assert(JoinType.apply("outer") === FullOuter)
    assert(JoinType.apply("full") === FullOuter)
  }

  test("construct a LeftOuter type") {
    assert(JoinType.apply("leftouter") === LeftOuter)
    assert(JoinType.apply("left_outer") === LeftOuter)
    assert(JoinType.apply("left") === LeftOuter)
  }

  test("construct a RightOuter type") {
    assert(JoinType.apply("rightouter") === RightOuter)
    assert(JoinType.apply("right_outer") === RightOuter)
    assert(JoinType.apply("right") === RightOuter)
  }

  test("construct a LeftSemi type") {
    assert(JoinType.apply("leftsemi") === LeftSemi)
    assert(JoinType.apply("left_semi") === LeftSemi)
  }

  test("construct a LeftAnti type") {
    assert(JoinType.apply("leftanti") === LeftAnti)
    assert(JoinType.apply("left_anti") === LeftAnti)
  }

  test("construct a Cross type") {
    assert(JoinType.apply("cross") === Cross)
  }

}
