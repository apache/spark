package org.apache.spark.sql
package catalyst
package plans
package logical

import expressions._

/**
 * Performs a physical redistribution of the data.  Used when the consumer of the query
 * result have expectations about the distribution and ordering of partitioned input data.
 */
abstract class RedistributeData extends UnaryNode {
  self: Product =>

  def output = child.output
}

case class SortPartitions(sortExpressions: Seq[SortOrder], child: LogicalPlan)
    extends RedistributeData {

  def references = sortExpressions.flatMap(_.references).toSet
}

case class Repartition(partitionExpressions: Seq[Expression], child: LogicalPlan)
    extends RedistributeData {

  def references = partitionExpressions.flatMap(_.references).toSet
}

