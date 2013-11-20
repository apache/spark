package catalyst
package plans
package logical

import expressions._

case class TestRelation(output: Attribute*) extends LeafNode