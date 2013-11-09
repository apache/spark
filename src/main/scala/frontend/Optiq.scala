package catalyst
package frontend

import org.eigenbase.sql._
import org.eigenbase.sql.SqlKind._

import analysis.{UnresolvedRelation, UnresolvedAttribute}
import expressions._
import plans._
import plans.logical._

import collection.JavaConversions._
import org.eigenbase.sql.SqlJoinOperator.JoinType

object Optiq {
  def parseSql(sql: String): LogicalPlan = {
    val sqlNode = (new parser.SqlParser(sql)).parseQuery
    sqlNodeToPlan(sqlNode)
  }

  protected object Call {
    def unapply(other: Any) = other match {
      case call: SqlCall =>
        Some((call.getKind, call.getOperands.toSeq))
      case other => None
    }
  }

  def sqlNodeToPlan(node: SqlNode): LogicalPlan = node match {
    case select: SqlSelect =>
      val selectList = select.getSelectList.map(sqlNodeToNamedExpr)
      val from = sqlNodeToPlan(select.getFrom)
      val where = Option(select.getWhere)
        .map(sqlNodeToExpr)
        .map(e => Filter(e, _: LogicalPlan))
        .getOrElse(identity[LogicalPlan](_))

      // Not implemented yet.
      val group = Option(select.getGroup).map(sqlNodeToExpr).isEmpty || ???
      val having = Option(select.getHaving).map(sqlNodeToExpr).isEmpty || ???
      val offset = Option(select.getOffset).map(sqlNodeToExpr).isEmpty || ???
      val order = Option(select.getOrderList).map(sqlNodeToExpr).isEmpty || ???
      // val windowList = Option(select.getWindowList).map(sqlNodeToExpr)

      Project(selectList.toSeq,
        where(
          from))

    case join: SqlJoin =>
      require(join.getCondition == null)
      Join(
        sqlNodeToPlan(join.getLeft),
        sqlNodeToPlan(join.getRight),
        join.getJoinType match {
          case JoinType.Full => FullOuter
          case JoinType.Inner => Inner
          case JoinType.Right => RightOuter
          case JoinType.Left => LeftOuter
        },
        Option(join.getCondition).map(sqlNodeToExpr))

    case ident: SqlIdentifier =>
      UnresolvedRelation(ident.toString, None)

    case unsupported =>
      val nodeString = try unsupported.toString catch { case e: Exception => "[FAILED toString]" }
      sys.error(s"no rule for sqlNode $nodeString, class: ${unsupported.getClass.getSimpleName}")
  }

  def sqlNodeToNamedExpr(node: SqlNode): NamedExpression = node match {
    case Call(AS, Seq(child, alias)) =>
      Alias(sqlNodeToExpr(child), alias.toString)()
    case ident: SqlIdentifier =>
      UnresolvedAttribute(ident.toString)
    case unsupported =>
      sys.error(s"no rule for sqlExpr $unsupported, class: ${unsupported.getClass.getSimpleName}")
  }

  def sqlNodeToExpr(node: SqlNode): Expression = node match {
    case Call(EQUALS, Seq(left,right)) => Equals(sqlNodeToExpr(left), sqlNodeToExpr(right))
    case Call(GREATER_THAN, Seq(left,right)) => GreaterThan(sqlNodeToExpr(left), sqlNodeToExpr(right))
    case Call(GREATER_THAN_OR_EQUAL, Seq(left,right)) => GreaterThanOrEqual(sqlNodeToExpr(left), sqlNodeToExpr(right))
    case Call(LESS_THAN, Seq(left,right)) => LessThan(sqlNodeToExpr(left), sqlNodeToExpr(right))
    case Call(LESS_THAN_OR_EQUAL, Seq(left,right)) => LessThanOrEqual(sqlNodeToExpr(left), sqlNodeToExpr(right))
    case named => sqlNodeToNamedExpr(named)
  }
}