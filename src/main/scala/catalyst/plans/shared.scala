package catalyst
package plans

sealed abstract class JoinType
case object Inner extends JoinType
case object LeftOuter extends JoinType
case object RightOuter extends JoinType
case object FullOuter extends JoinType