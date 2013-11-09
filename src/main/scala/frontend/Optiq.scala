package catalyst
package frontend

import org.eigenbase.sql._
import org.eigenbase.sql.SqlKind._

import analysis.{UnresolvedRelation, UnresolvedAttribute}
import expressions._
import plans._
import plans.logical._

import collection.JavaConversions._

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
      // Lots of things not supported yet.
      require(select.getGroup == null)
      require(select.getHaving == null)
      require(select.getWhere == null)
      require(select.getOffset == null)
      require(select.getOrderList == null)
      // require(select.getWindowList == null)

      Project(selectList.toSeq, sqlNodeToPlan(select.getFrom))

    case join: SqlJoin =>
      Join(
        sqlNodeToPlan(join.getLeft),
        sqlNodeToPlan(join.getRight),
        Inner,
        None)

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
    case unsupported => sys.error(s"no rule for sqlExpr $unsupported, class: ${unsupported.getClass.getSimpleName}")
  }

  def sqlNodeToExpr(node: SqlNode): Expression = node match {
    case named => sqlNodeToNamedExpr(named)
  }
}