package org.apache.spark.sql
package catalyst
package plans
package logical

abstract class BaseRelation extends LeafNode {
  self: Product =>

  def tableName: String
}
