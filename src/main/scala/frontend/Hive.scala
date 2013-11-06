package catalyst
package frontend

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import expressions._
import plans.logical._

import collection.JavaConversions._

object Hive {
  def parseSql(sql: String): LogicalPlan = {
    val tree =
      ParseUtils.findRootNonNullToken(
        (new ParseDriver()).parse(sql, null /* no context required for parsing alone */))

    nodeToPlan(tree)
  }

  /** Extractor for matching Hive's AST Tokens */
  protected object Token {
    def unapply(t: Any) = t match {
      case t: ASTNode =>
        Some((t.getText, Option(t.getChildren).map(_.toList).getOrElse(Nil)))
      case _ => None
    }
  }

  protected def nodeToPlan(node: Node): LogicalPlan = node match {
    case Token("TOK_QUERY",
           fromClause ::
           insertClause :: Nil) =>
      dumpTree(fromClause)
      nodeToPlan(fromClause)
    case a: ASTNode =>
      println("FAILURE")
      dumpTree(a)
      ???
  }

  protected def nodeToExpr(node: Node): Expression = node match {
    case other => Literal(1) // WRONG
  }

  protected def dumpTree(node: Node, indent: Int = 0) {
    node match {
      case a: ASTNode => println(("  " * indent) + a.getText + " " + a.getClass.getName)
      case other => sys.error(s"Non ASTNode encountered: $other")
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(dumpTree(_, indent + 1))
  }
}