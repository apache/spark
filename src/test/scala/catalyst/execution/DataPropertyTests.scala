package catalyst.execution

import catalyst.dsl._

import org.scalatest.FunSuite

class DataPropertyTests extends FunSuite {

  protected def checkNeedExchange(
      outputDataProperty: Partitioned,
      inputDataProperty: Partitioned,
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
      HashPartitioned(Seq('a, 'b, 'c)),
      NotSpecified(),
      false)

    checkNeedExchange(
      HashPartitioned(Seq('a, 'b, 'c)),
      HashPartitioned(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      HashPartitioned(Seq('b, 'c)),
      HashPartitioned(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      HashPartitioned(Nil),
      HashPartitioned(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      HashPartitioned(Nil),
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    // Cases which need an exchange between two data properties.
    checkNeedExchange(
      HashPartitioned(Seq('a, 'b, 'c)),
      HashPartitioned(Seq('b, 'c)),
      true)

    checkNeedExchange(
      HashPartitioned(Seq('a, 'b, 'c)),
      HashPartitioned(Seq('d, 'e)),
      true)

    checkNeedExchange(
      HashPartitioned(Seq('a, 'b, 'c)),
      HashPartitioned(Nil),
      true)

    checkNeedExchange(
      HashPartitioned(Seq('a, 'b, 'c)),
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    checkNeedExchange(
      HashPartitioned(Seq('b, 'c)),
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
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
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      NotSpecified(),
      false)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      RangePartitioned(Seq('a.asc, 'b.asc)),
      false)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc, 'd.desc)),
      false)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      HashPartitioned(Seq('a, 'b, 'c)),
      false)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      HashPartitioned(Seq('c, 'b, 'a)),
      false)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      HashPartitioned(Seq('b, 'c, 'a, 'd)),
      false)

    // Cases which need an exchange between two data properties.
    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      RangePartitioned(Seq('a.asc, 'b.desc, 'c.asc)),
      true)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      RangePartitioned(Seq('b.asc, 'a.asc)),
      true)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      HashPartitioned(Seq('a, 'b)),
      true)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      HashPartitioned(Seq('c, 'd)),
      true)

    checkNeedExchange(
      RangePartitioned(Seq('a.asc, 'b.asc, 'c.asc)),
      HashPartitioned(Nil),
      true)
  }
}