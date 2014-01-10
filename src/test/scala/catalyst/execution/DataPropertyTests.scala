package catalyst.execution

import catalyst.dsl._

import org.scalatest.FunSuite

class DataPropertyTests extends FunSuite {

  protected def checkNeedExchange(
      outputDataProperty: DataProperty,
      inputDataProperty: DataProperty,
      expected: Boolean) {
    assert(
      outputDataProperty.needExchange(inputDataProperty) === expected,
      s"""
      |== Output data property ==
      |$outputDataProperty
      |== Input data property ==
      |$inputDataProperty
      |== Expected result of needExchange ==
      |$expected
      """.stripMargin)
  }

  test("needExchange test: GroupProperty is the output DataProperty") {
    // Cases which do not need an exchange between two data properties.
    checkNeedExchange(
      GroupProperty(Seq('a, 'b, 'c)),
      NotSpecifiedProperty(),
      false)

    checkNeedExchange(
      GroupProperty(Seq('a, 'b, 'c)),
      GroupProperty(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      GroupProperty(Seq('b, 'c)),
      GroupProperty(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      GroupProperty(Nil),
      GroupProperty(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      GroupProperty(Nil),
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    // Cases which need an exchange between two data properties.
    checkNeedExchange(
      GroupProperty(Seq('a, 'b, 'c)),
      GroupProperty(Seq('b, 'c)),
      true)

    checkNeedExchange(
      GroupProperty(Seq('a, 'b, 'c)),
      GroupProperty(Seq('d, 'e)),
      true)

    checkNeedExchange(
      GroupProperty(Seq('a, 'b, 'c)),
      GroupProperty(Nil),
      true)

    checkNeedExchange(
      GroupProperty(Seq('a, 'b, 'c)),
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    checkNeedExchange(
      GroupProperty(Seq('b, 'c)),
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    // TODO: We should check functional dependencies
    /*
    checkNeedExchange(
      GroupProperty(Seq('b)),
      GroupProperty(Seq('b + 1)),
      false)
    */
  }

  test("needExchange test: SortProperty is the output DataProperty") {
    // Cases which do not need an exchange between two data properties.
    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      NotSpecifiedProperty(),
      false)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      SortProperty(Seq('a.asc, 'b.asc)),
      false)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc, 'd.desc)),
      false)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      GroupProperty(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      GroupProperty(Seq('c, 'b, 'a)),
      false)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      GroupProperty(Seq('b, 'c, 'a, 'd)),
      false)

    // Cases which need an exchange between two data properties.
    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      SortProperty(Seq('a.asc, 'b.desc, 'c.asc)),
      true)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      SortProperty(Seq('b.asc, 'a.asc)),
      true)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      GroupProperty(Seq('a, 'b)),
      true)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      GroupProperty(Seq('c, 'd)),
      true)

    checkNeedExchange(
      SortProperty(Seq('a.asc, 'b.asc, 'c.asc)),
      GroupProperty(Nil),
      true)
  }
}