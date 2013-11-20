package catalyst
package plans
package logical

import expressions._

object LocalRelation {
  def apply(output: Attribute*) =
    new LocalRelation(output)
}

case class LocalRelation(output: Seq[Attribute], data: Seq[Product] = Nil) extends LeafNode {
  // TODO: Validate schema compliance.
  def loadData(newData: Seq[Product]) = new LocalRelation(output, data ++ newData)
}