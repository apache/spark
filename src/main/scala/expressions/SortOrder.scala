package catalyst
package expressions

import types._

abstract sealed class SortDirection
case object Ascending extends SortDirection
case object Descending extends SortDirection

/**
 * An expression that can be used to sort a tuple.  This class extends expression primarily so that
 * transformations over expression will descend into its child.
 */
case class SortOrder(child: Expression, direction: SortDirection) extends UnaryExpression {
  // TODO: This might be a little sloppy... not clear integer is the right thing here.
  def dataType = IntegerType
  def nullable = child.nullable
}