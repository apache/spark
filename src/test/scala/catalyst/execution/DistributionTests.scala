package catalyst
package plans
package physical

import org.scalatest.FunSuite

import catalyst.dsl._

class DistributionTests extends FunSuite {

  protected def checkSatisfied(
      inputPartitioning: Partitioning,
      requiredDistribution: Distribution,
      satisfied: Boolean) {
    if(inputPartitioning.satisfies(requiredDistribution) != satisfied)
      fail(
        s"""
        |== Input Distribution ==
        |$inputPartitioning
        |== Required Distribution ==
        |$requiredDistribution
        |== Does input distribution satisfy requirements? ==
        |Expected $satisfied got ${inputPartitioning.satisfies(requiredDistribution)}
        """.stripMargin)
  }

  test("needExchange test: ClusteredDistribution is the output DataProperty") {
    // Cases which do not need an exchange between two data properties.
    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      HashPartitioning(Seq('b, 'c), 10),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      SinglePartition,
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      SinglePartition,
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    // Cases which need an exchange between two data properties.
    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      ClusteredDistribution(Seq('b, 'c)),
      false)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      ClusteredDistribution(Seq('d, 'e)),
      false)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      AllTuples,
      false)

    checkSatisfied(
      HashPartitioning(Seq('a, 'b, 'c), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      false)

    checkSatisfied(
      HashPartitioning(Seq('b, 'c), 10),
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
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      UnspecifiedDistribution,
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.asc, 'c.asc, 'd.desc)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('a, 'b, 'c)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('c, 'b, 'a)),
      true)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('b, 'c, 'a, 'd)),
      true)

    // Cases which need an exchange between two data properties.
    // TODO: We can have an optimization to first sort the dataset
    // by a.asc and then sort b, and c in a partition. This optimization
    // should tradeoff the benefit of a less number of Exchange operators
    // and the parallelism.
    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('a.asc, 'b.desc, 'c.asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      OrderedDistribution(Seq('b.asc, 'a.asc)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('a, 'b)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Seq('c, 'd)),
      false)

    checkSatisfied(
      RangePartitioning(Seq('a.asc, 'b.asc, 'c.asc), 10),
      ClusteredDistribution(Nil),
      false)
  }
}