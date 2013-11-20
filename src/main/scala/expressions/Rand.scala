package catalyst
package expressions

import catalyst.types.FloatType

case object Rand extends LeafExpression {
  def dataType = FloatType
  def nullable = false
  def references = Set.empty
}