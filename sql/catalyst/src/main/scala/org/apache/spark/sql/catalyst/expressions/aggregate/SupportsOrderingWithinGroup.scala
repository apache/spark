
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions.SortOrder

/**
 * The trait used to set the [[SortOrder]] for supporting functions.
 * By default ordering is optional.
 */
trait SupportsOrderingWithinGroup {
  def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction
  /** Indicator that ordering was set */
  def orderingFilled: Boolean = false
}
