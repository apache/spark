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
