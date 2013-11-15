package catalyst
package frontend

import catalyst.analysis.UnresolvedRelation
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import analysis._
import expressions._
import plans.logical._

import collection.JavaConversions._

/**
 * A logical node that represent a non-query command to be executed by the system.  For example,
 * commands can be used by parsers to represent DDL operations.
 */
abstract class Command extends LeafNode {
  self: Product =>
  def output = Seq.empty
}

/**
 * Returned for commands supported by the parser, but not catalyst.  In general these are DDL
 * commands that are passed directly to Hive.
 */
case class NativeCommand(cmd: String) extends Command

case class DfsCommand(cmd: String) extends Command

case class ShellCommand(cmd: String) extends Command

case class SourceCommand(filePath: String) extends Command

case class ConfigurationAssignment(cmd: String) extends Command

case class AddJar(jarPath: String) extends Command

case class AddFile(filePath: String) extends Command

object Hive {
  def parseSql(sql: String): LogicalPlan = {
    if(sql.toLowerCase.startsWith("set"))
      ConfigurationAssignment(sql)
    else if(sql.toLowerCase.startsWith("add jar"))
      AddJar(sql.drop(8))
    else if(sql.toLowerCase.startsWith("add file"))
      AddFile(sql.drop(9))
    else if(sql.startsWith("dfs"))
      DfsCommand(sql)
    else if(sql.startsWith("source"))
      SourceCommand(sql.split(" ").toSeq match { case Seq("source", filePath) => filePath })
    else if(sql.startsWith("!"))
      ShellCommand(sql.drop(1))
    else {
      val tree =
          try {
            ParseUtils.findRootNonNullToken(
              (new ParseDriver()).parse(sql, null /* no context required for parsing alone */))
          } catch {
            case pe: org.apache.hadoop.hive.ql.parse.ParseException =>
              sys.error(s"Failed to parse '$sql'")
          }

      nodeToPlan(tree) match {
        case NativePlaceholder(_) => NativeCommand(sql)
        case other => other
      }
    }
  }

  /** Extractor for matching Hive's AST Tokens */
  protected object Token {
    def unapply(t: Any) = t match {
      case t: ASTNode =>
        Some((t.getText, Option(t.getChildren).map(_.toList).getOrElse(Nil)))
      case _ => None
    }
  }

  // Kinda gross hack...
  case class NativePlaceholder(ast: Node) extends Command

  protected def nodeToPlan(node: Node): LogicalPlan = node match {
    case a @ Token("TOK_EXPLAIN", _) => NativePlaceholder(a)

    case a @ Token("TOK_DESCFUNCTION", _) => NativePlaceholder(a)
    case a @ Token("TOK_DESCTABLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_DESCDATABASE", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOW_TABLESTATUS", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWDATABASES", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWFUNCTIONS", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWINDEXES", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWINDEXES", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWPARTITIONS", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWTABLES", _) => NativePlaceholder(a)

    case a @ Token("TOK_LOCKTABLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOWLOCKS", _) => NativePlaceholder(a)
    case a @ Token("TOK_UNLOCKTABLE", _) => NativePlaceholder(a)

    case a @ Token("TOK_CREATEROLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_DROPROLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_GRANT", _) => NativePlaceholder(a)
    case a @ Token("TOK_GRANT_ROLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_REVOKE", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOW_GRANT", _) => NativePlaceholder(a)
    case a @ Token("TOK_SHOW_ROLE_GRANT", _) => NativePlaceholder(a)

    case a @ Token("TOK_CREATEFUNCTION", _) => NativePlaceholder(a)
    case a @ Token("TOK_DROPFUNCTION", _) => NativePlaceholder(a)

    case a @ Token("TOK_ALTERDATABASE_PROPERTIES", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERINDEX_PROPERTIES", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERINDEX_REBUILD", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_ADDCOLS", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_ADDPARTS", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_ARCHIVE", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_CLUSTER_SORT", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_DROPPARTS", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_PARTITION", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_PROPERTIES", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_RENAME", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_RENAMECOL", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_REPLACECOLS", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_TOUCH", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERTABLE_UNARCHIVE", _) => NativePlaceholder(a)
    case a @ Token("TOK_ANALYZE", _) => NativePlaceholder(a)
    case a @ Token("TOK_CREATEDATABASE", _) => NativePlaceholder(a)
    case a @ Token("TOK_CREATEINDEX", _) => NativePlaceholder(a)
    case a @ Token("TOK_CREATETABLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_DROPDATABASE", _) => NativePlaceholder(a)
    case a @ Token("TOK_DROPINDEX", _) => NativePlaceholder(a)
    case a @ Token("TOK_DROPTABLE", _) => NativePlaceholder(a)
    case a @ Token("TOK_MSCK", _) => NativePlaceholder(a)

    // TODO(marmbrus): Figure out how view are expanded by hive, as we might need to handle this.
    case a @ Token("TOK_ALTERVIEW_ADDPARTS", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERVIEW_DROPPARTS", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERVIEW_PROPERTIES", _) => NativePlaceholder(a)
    case a @ Token("TOK_ALTERVIEW_RENAME", _) => NativePlaceholder(a)
    case a @ Token("TOK_CREATEVIEW", _) => NativePlaceholder(a)
    case a @ Token("TOK_DROPVIEW", _) => NativePlaceholder(a)


    case a @ Token("TOK_EXPORT", _) => NativePlaceholder(a)
    case a @ Token("TOK_IMPORT", _) => NativePlaceholder(a)
    case a @ Token("TOK_LOAD", _) => NativePlaceholder(a)

    case a @ Token("TOK_SWITCHDATABASE", _) => NativePlaceholder(a)
    case a @ Token("TOK_EXPLAIN", _) => NativePlaceholder(a)

    case Token("TOK_QUERY",
           fromClause ::
           Token("TOK_INSERT",
             destClause ::
             Token("TOK_SELECT",
                selectExprs) :: Nil) :: Nil) =>
      nodeToDest(
        destClause,
        Project(selectExprs.map(nodeToExpr),
          nodeToPlan(fromClause)))

    // TODO: find a less redundant way to do this.
    case Token("TOK_QUERY",
           fromClause ::
           Token("TOK_INSERT",
             destClause ::
             Token("TOK_SELECT",
               selectExprs) ::
             Token("TOK_ORDERBY",
               orderByExprs) :: Nil) :: Nil) =>
      nodeToDest(
        destClause,
        Sort(orderByExprs.map(nodeToSortOrder),
          Project(selectExprs.map(nodeToExpr),
            nodeToPlan(fromClause))))

    case Token("TOK_FROM",
           Token("TOK_TABREF",
             Token("TOK_TABNAME",
               Token(name, Nil) :: Nil) :: Nil) :: Nil) =>
      UnresolvedRelation(name, None)
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  def nodeToSortOrder(node: Node): SortOrder = node match {
    case Token("TOK_TABSORTCOLNAMEASC",
           Token("TOK_TABLE_OR_COL",
             Token(name, Nil) :: Nil) :: Nil) =>
      SortOrder(UnresolvedAttribute(name), Ascending)

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToDest(node: Node, query: LogicalPlan): LogicalPlan = node match {
    case Token("TOK_DESTINATION",
           Token("TOK_DIR",
             Token("TOK_TMP_FILE", Nil) :: Nil) :: Nil) =>
      query
    case Token("TOK_DESTINATION",
           Token("TOK_TAB",
             Token("TOK_TABNAME",
               Token(tableName, Nil) :: Nil) :: Nil) :: Nil) =>
      InsertIntoHiveTable(tableName, query)
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToExpr(node: Node): NamedExpression = node match {
    case Token("TOK_SELEXPR",
           Token("TOK_TABLE_OR_COL",
             Token(name, Nil) :: Nil) :: Nil) =>
      UnresolvedAttribute(name)
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def dumpTree(node: Node, builder: StringBuilder = new StringBuilder, indent: Int = 0)
  : StringBuilder = {
    node match {
      case a: ASTNode => builder.append(("  " * indent) + a.getText + "\n")
      case other => sys.error(s"Non ASTNode encountered: $other")
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(dumpTree(_, builder, indent + 1))
    builder
  }
}