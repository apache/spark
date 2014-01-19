package catalyst.execution

import catalyst.dsl._

import org.scalatest.FunSuite

class DistributionTests extends FunSuite {
  /*

  protected def checkSatisfied(
      inputDistribution: Distribution,
      requiredDistribution: Distribution,
      satisfied: Boolean) {
    if(inputDistribution.satisfies(requiredDistribution) != satisfied)
      fail(
        s"""
        |== Input Distribution ==
        |$inputDistribution
        |== Required Distribution ==
        |$requiredDistribution
        |== Does input distribution satisfy requirements? ==
        |Expected $satisfied got ${inputDistribution.satisfies(requiredDistribution)}
        """.stripMargin)
  }

  test("needExchange test: ClusteredDistribution is the output DataProperty") {
    // Cases which do not need an exchange between two data properties.
    checkSatisfied(
      ClusteredDistribution(Seq('a, 'b, 'c)),
      UnknownDistribution,
      true)

    checkSatisfied(
      ClusteredDistribution(Seq('a, 'b, 'c)),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      ClusteredDistribution(Seq('b, 'c)),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      ClusteredDistribution(Nil),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      ClusteredDistribution(Nil),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    // Cases which need an exchange between two data properties.
    checkSatisfied(
      ClusteredDistribution(Seq('a, 'b, 'c)),
      ClusteredDistribution(Seq('b, 'c)),
      false)

    checkSatisfied(
      ClusteredDistribution(Seq('a, 'b, 'c)),
      ClusteredDistribution(Seq('d, 'e)),
      false)

    checkSatisfied(
      ClusteredDistribution(Seq('a, 'b, 'c)),
      ClusteredDistribution(Nil),
      false)

    checkSatisfied(
      ClusteredDistribution(Seq('a, 'b, 'c)),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    checkSatisfied(
      ClusteredDistribution(Seq('b, 'c)),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    // TODO: We should check functional dependencies
    /*
    checkSatisfied(
      ClusteredDistribution(Seq('b)),
      ClusteredDistribution(Seq('b + 1)),
      true)
    */
  }

  test("needExchange test: OrderedDistribution is the output DataProperty") {
    // Cases which do not need an exchange between two data properties.
    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      UnknownDistribution,
      true)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      OrderedDistribution(Seq('a.asc, 'b.asc)),
      true)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc, 'd.desc)),
      true)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      ClusteredDistribution(Seq('c, 'b, 'a)),
      true)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      ClusteredDistribution(Seq('b, 'c, 'a, 'd)),
      true)

    // Cases which need an exchange between two data properties.
    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      OrderedDistribution(Seq('a.asc, 'b.desc, 'c.asc)),
      false)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      OrderedDistribution(Seq('b.asc, 'a.asc)),
      false)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      ClusteredDistribution(Seq('a, 'b)),
      false)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      ClusteredDistribution(Seq('c, 'd)),
      false)

    checkSatisfied(
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      ClusteredDistribution(Nil),
      false)
  }

  */
}