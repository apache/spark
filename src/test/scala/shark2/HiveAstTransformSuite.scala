package catalyst

import org.scalatest.FunSuite

import frontend.hive._
import org.apache.hadoop.hive.ql.parse.{HiveParser, ASTNode}

import collection.JavaConversions._

object HiveDistinctToGroupBy {
  import Hive._

  def apply(n: ASTNode): ASTNode = n transform {
    case t @ Token(_, children) if getClauseOption("TOK_SELECTDI", children).isDefined =>
      val selectDistinctClause = getClause("TOK_SELECTDI", children)
      val selectExprs = selectDistinctClause.getChildren.map {
        case Token("TOK_SELEXPR", child :: Nil) => child
        case Token("TOK_SELEXPR", child :: _ /* discard alias */ :: Nil) => child

      }

      val groupByClause = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_GROUPBY))
        .withText("TOK_GROUPBY")
        .withChildren(selectExprs)

      t.withChildren(children.map {
        case Token("TOK_SELECTDI", selectExprs) =>
          new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_SELECT))
            .withText("TOK_SELECT")
            .withChildren(selectExprs)
        case other => other
      } :+ groupByClause)
  }
}

class HiveAstTransformSuite extends FunSuite {
  import Hive._
  test("simple transform") {
    val q1 = getAst("SELECT key FROM src")
    val q2 = getAst("SELECT value FROM src")
    val transformed = q1 transform {
        case t @ Token("key", Nil) => t.withText("value")
      }

    transformed checkEquals q2
  }

  test("distinct to group by") {
    val distinct = getAst("SELECT DISTINCT key FROM src")
    val groupBy = getAst("SELECT key FROM src GROUP BY key")
    val transformed = HiveDistinctToGroupBy(distinct)

    groupBy checkEquals transformed
  }

  test("nested distinct to group by") {
    val distinct = getAst("SELECT * FROM (SELECT DISTINCT key FROM src) a")
    val groupBy = getAst("SELECT * FROM (SELECT key FROM src GROUP BY key) a")
    val transformed = HiveDistinctToGroupBy(distinct)

    groupBy checkEquals transformed
  }

  test("distinct to group by with alias") {
    val distinct = getAst("SELECT DISTINCT key as k FROM src")
    val groupBy = getAst("SELECT key as k FROM src GROUP BY key")
    val transformed = HiveDistinctToGroupBy(distinct)

    groupBy checkEquals transformed
  }

  test("distinct to group by complex exprs") {
    val distinct = getAst("SELECT DISTINCT key + 1 FROM src")
    val groupBy = getAst("SELECT key + 1 FROM src GROUP BY key + 1")
    val transformed = HiveDistinctToGroupBy(distinct)

    groupBy checkEquals transformed
  }

}