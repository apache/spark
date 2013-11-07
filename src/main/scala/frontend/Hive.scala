package catalyst
package frontend

import catalyst.analysis.UnresolvedRelation
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import analysis._
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
           Token("TOK_INSERT",
             Token("TOK_DESTINATION",
               Token("TOK_DIR",                                // For now only support queries with no INSERT clause.
                 Token("TOK_TMP_FILE", Nil) :: Nil) :: Nil) ::
             Token("TOK_SELECT",
                selectExprs) :: Nil) :: Nil) =>
      Project(selectExprs.map(nodeToExpr), nodeToPlan(fromClause))
    case Token("TOK_FROM",
           Token("TOK_TABREF",
             Token("TOK_TABNAME",
               Token(name, Nil) :: Nil) :: Nil) :: Nil) =>
      UnresolvedRelation(name, None)
    case a: ASTNode =>
      println("FAILURE")
      dumpTree(a)
      ???
  }

  protected def nodeToExpr(node: Node): NamedExpression = node match {
    case Token("TOK_SELEXPR",
           Token("TOK_TABLE_OR_COL",
             Token(name, Nil) :: Nil) :: Nil) =>
      UnresolvedAttribute(name)
  }

  protected def dumpTree(node: Node, indent: Int = 0) {
    node match {
      case a: ASTNode => println(("  " * indent) + a.getText)
      case other => sys.error(s"Non ASTNode encountered: $other")
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(dumpTree(_, indent + 1))
  }
}