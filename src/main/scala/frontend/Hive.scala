package catalyst
package frontend
package hive

import catalyst.analysis.UnresolvedRelation
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import analysis._
import expressions._
import plans.logical._
import types._

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
  protected val nativeCommands = Seq(
    "TOK_EXPLAIN",

    "TOK_DESCFUNCTION",
    "TOK_DESCTABLE",
    "TOK_DESCDATABASE",
    "TOK_SHOW_TABLESTATUS",
    "TOK_SHOWDATABASES",
    "TOK_SHOWFUNCTIONS",
    "TOK_SHOWINDEXES",
    "TOK_SHOWINDEXES",
    "TOK_SHOWPARTITIONS",
    "TOK_SHOWTABLES",

    "TOK_LOCKTABLE",
    "TOK_SHOWLOCKS",
    "TOK_UNLOCKTABLE",

    "TOK_CREATEROLE",
    "TOK_DROPROLE",
    "TOK_GRANT",
    "TOK_GRANT_ROLE",
    "TOK_REVOKE",
    "TOK_SHOW_GRANT",
    "TOK_SHOW_ROLE_GRANT",

    "TOK_CREATEFUNCTION",
    "TOK_DROPFUNCTION",

    "TOK_ALTERDATABASE_PROPERTIES",
    "TOK_ALTERINDEX_PROPERTIES",
    "TOK_ALTERINDEX_REBUILD",
    "TOK_ALTERTABLE_ADDCOLS",
    "TOK_ALTERTABLE_ADDPARTS",
    "TOK_ALTERTABLE_ARCHIVE",
    "TOK_ALTERTABLE_CLUSTER_SORT",
    "TOK_ALTERTABLE_DROPPARTS",
    "TOK_ALTERTABLE_PARTITION",
    "TOK_ALTERTABLE_PROPERTIES",
    "TOK_ALTERTABLE_RENAME",
    "TOK_ALTERTABLE_RENAMECOL",
    "TOK_ALTERTABLE_REPLACECOLS",
    "TOK_ALTERTABLE_TOUCH",
    "TOK_ALTERTABLE_UNARCHIVE",
    "TOK_ANALYZE",
    "TOK_CREATEDATABASE",
    "TOK_CREATEINDEX",
    "TOK_CREATETABLE",
    "TOK_DROPDATABASE",
    "TOK_DROPINDEX",
    "TOK_DROPTABLE",
    "TOK_MSCK",

    // TODO(marmbrus): Figure out how view are expanded by hive, as we might need to handle this.
    "TOK_ALTERVIEW_ADDPARTS",
    "TOK_ALTERVIEW_DROPPARTS",
    "TOK_ALTERVIEW_PROPERTIES",
    "TOK_ALTERVIEW_RENAME",
    "TOK_CREATEVIEW",
    "TOK_DROPVIEW",

    "TOK_EXPORT",
    "TOK_IMPORT",
    "TOK_LOAD",

    "TOK_SWITCHDATABASE"
  )

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
              throw new RuntimeException(s"Failed to parse sql: '$sql'", pe)
          }

      if(nativeCommands contains tree.getText)
        NativeCommand(sql)
      else
        nodeToPlan(tree)
    }
  }

  def parseDdl(ddl: String): Seq[Attribute] = {
    val tree =
      try {
        ParseUtils.findRootNonNullToken(
          (new ParseDriver()).parse(ddl, null /* no context required for parsing alone */))
      } catch {
        case pe: org.apache.hadoop.hive.ql.parse.ParseException =>
          throw new RuntimeException(s"Failed to parse ddl: '$ddl'", pe)
      }
    assert(tree.asInstanceOf[ASTNode].getText == "TOK_CREATETABLE", "Only CREATE TABLE supported.")
    val tableOps = tree.getChildren
    val colList =
      tableOps
        .find(_.asInstanceOf[ASTNode].getText == "TOK_TABCOLLIST")
        .getOrElse(sys.error("No columnList!")).getChildren

    colList.map(nodeToAttribute)
  }

  /** Extractor for matching Hive's AST Tokens. */
  protected object Token {
    /** @returns matches of the form (tokenName, children). */
    def unapply(t: Any) = t match {
      case t: ASTNode =>
        Some((t.getText, Option(t.getChildren).map(_.toList).getOrElse(Nil)))
      case _ => None
    }
  }

  protected def getClauses(clauseNames: Seq[String], nodeList: Seq[Node]): Seq[Option[Node]] = {
    clauseNames.map(getClauseOption(_, nodeList))
  }

  protected def getClause(clauseName: String, nodeList: Seq[Node]) =
    getClauseOption(clauseName, nodeList)
      .getOrElse(sys.error(s"Expected clause $clauseName missing from ${nodeList.map(dumpTree(_)).mkString("\n")}"))

  protected def getClauseOption(clauseName: String, nodeList: Seq[Node]): Option[Node] = {
    nodeList.filter { case ast: ASTNode => ast.getText == clauseName } match {
      case Seq(oneMatch) => Some(oneMatch)
      case Seq() => None
      case _ => sys.error(s"Found multiple instances of clause $clauseName")
    }
  }

  protected def nodeToAttribute(node: Node): Attribute = node match {
    case Token("TOK_TABCOL",
           Token(colName, Nil) ::
           dataType :: Nil) =>
      AttributeReference(colName, nodeToDataType(dataType), true)()

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToDataType(node: Node): DataType = node match {
    case Token("TOK_BIGINT", Nil) => IntegerType
    case Token("TOK_INT", Nil) => IntegerType
    case Token("TOK_TINYINT", Nil) => IntegerType
    case Token("TOK_SMALLINT", Nil) => IntegerType
    case Token("TOK_BOOLEAN", Nil) => BooleanType
    case Token("TOK_STRING", Nil) => StringType
    case Token("TOK_FLOAT", Nil) => FloatType
    case Token("TOK_DOUBLE", Nil) => FloatType
    case Token("TOK_LIST", elementType :: Nil) => ArrayType(nodeToDataType(elementType))
    case Token("TOK_STRUCT",
           Token("TOK_TABCOLLIST", fields) :: Nil) =>
      StructType(fields.map(nodeToStructField))
    case Token("TOK_MAP",
           keyType ::
           valueType :: Nil) =>
      MapType(nodeToDataType(keyType), nodeToDataType(valueType))
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for DataType:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToStructField(node: Node): StructField = node match {
    case Token("TOK_TABCOL",
           Token(fieldName, Nil) ::
           dataType :: Nil) =>
      StructField(fieldName, nodeToDataType(dataType))
    case Token("TOK_TABCOL",
           Token(fieldName, Nil) ::
             dataType ::
             _ /* comment */:: Nil) =>
      StructField(fieldName, nodeToDataType(dataType) )
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for StructField:\n ${dumpTree(a).toString} ")
  }

  protected def nameExpressions(exprs: Seq[Expression]): Seq[NamedExpression] = {
    exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c_$i")()
    }
  }

  protected def nodeToPlan(node: Node): LogicalPlan = node match {
    case Token("TOK_QUERY",
           fromClause ::
           Token("TOK_INSERT", insertClauses) :: Nil) =>

      val (Some(destClause) ::
          Some(selectClause) ::
          whereClause ::
          orderByClause ::
          limitClause :: Nil) = getClauses(Seq("TOK_DESTINATION", "TOK_SELECT", "TOK_WHERE", "TOK_ORDERBY", "TOK_LIMIT"), insertClauses)

      val relations = nodeToPlan(fromClause)
      val withWhere = whereClause.map { whereNode =>
        val Seq(whereExpr) = whereNode.getChildren().toSeq
        Filter(nodeToExpr(whereExpr), relations)
      }.getOrElse(relations)

      val withProject = Project(nameExpressions(selectClause.getChildren.map(selExprNodeToExpr)), withWhere)
      val withSort = orderByClause.map(_.getChildren.map(nodeToSortOrder)).map(Sort(_, withProject)).getOrElse(withProject)
      val withLimit = limitClause.map(l => nodeToExpr(l.getChildren.head)).map(StopAfter(_, withSort)).getOrElse(withSort)

      nodeToDest(
        destClause,
        withLimit)

    case Token("TOK_FROM",
           Token("TOK_TABREF",
             Token("TOK_TABNAME",
               Token(name, Nil) :: Nil) :: Nil) :: Nil) =>
      UnresolvedRelation(name, None)
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  def nodeToSortOrder(node: Node): SortOrder = node match {
    case Token("TOK_TABSORTCOLNAMEASC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Ascending)

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

  protected def selExprNodeToExpr(node: Node): Expression = node match {
    case Token("TOK_SELEXPR",
           e :: Nil) =>
      nodeToExpr(e)
  }


  val numericAstTypes =
    Seq(HiveParser.Number, HiveParser.TinyintLiteral, HiveParser.SmallintLiteral, HiveParser.BigintLiteral)
  protected def nodeToExpr(node: Node): Expression = node match {
    case Token("TOK_TABLE_OR_COL",
           Token(name, Nil) :: Nil) =>
      UnresolvedAttribute(name)
    case Token("-", child :: Nil) => UnaryMinus(nodeToExpr(child))
    case Token("TOK_ALLCOLREF", Nil) => Star
    case Token("TOK_FUNCTION", Token("AVG", Nil) :: arg :: Nil) => Average(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token("count", Nil) :: arg :: Nil) => Count(nodeToExpr(arg))
    case Token("=", left :: right:: Nil) => Equals(nodeToExpr(left), nodeToExpr(right))
    case Token(">", left :: right:: Nil) => GreaterThan(nodeToExpr(left), nodeToExpr(right))
    case Token(">=", left :: right:: Nil) => GreaterThanOrEqual(nodeToExpr(left), nodeToExpr(right))
    case Token("<", left :: right:: Nil) => LessThan(nodeToExpr(left), nodeToExpr(right))
    case Token("<=", left :: right:: Nil) => LessThanOrEqual(nodeToExpr(left), nodeToExpr(right))
    case Token("TOK_FUNCTION", Token("RAND", Nil) :: Nil) => Rand
    case Token("TOK_STRINGLITERALSEQUENCE", strings) =>
      Literal(strings.map(s => BaseSemanticAnalyzer.unescapeSQLString(s.asInstanceOf[ASTNode].getText)).mkString)

    // This code is adapted from https://github.com/apache/hive/blob/branch-0.10/ql/src/java/org/apache/hadoop/hive/ql/parse/TypeCheckProcFactory.java#L223
    case ast: ASTNode if numericAstTypes contains ast.getType() =>
      var v: Literal = null
      try {
        if (ast.getText().endsWith("L")) {
          // Literal bigint.
          v = Literal(ast.getText().substring(0, ast.getText().length() - 1).toLong, LongType)
        } else if (ast.getText().endsWith("S")) {
          // Literal smallint.
          v = Literal(ast.getText().substring(0, ast.getText().length() - 1).toShort, ShortType)
        } else if (ast.getText().endsWith("Y")) {
          // Literal tinyint.
          v = Literal(ast.getText().substring(0, ast.getText().length() - 1).toByte, ByteType)
        } else if (ast.getText().endsWith("BD")) {
          throw new NotImplementedError("Hive Decimal not implemented yet")
          /*
          // Literal decimal
          val strVal = ast.getText().substring(0, ast.getText().length() - 2);
          HiveDecimal hd = HiveDecimal.create(strVal);
          int prec = 1;
          int scale = 0;
          if (hd != null) {
            prec = hd.precision();
            scale = hd.scale();
          }
          DecimalTypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(prec, scale);
          return new ExprNodeConstantDesc(typeInfo, strVal);   */
        } else {
          v = Literal(ast.getText().toDouble, DoubleType)
          v = Literal(ast.getText().toLong, LongType)
          v = Literal(ast.getText().toInt, IntegerType)
        }
      } catch {
        case nfe: NumberFormatException => // Do nothing
      }

      if(v == null)
        sys.error(s"Failed to parse number ${ast.getText}")
      else
        v

    case ast: ASTNode if ast.getType == HiveParser.StringLiteral =>
      Literal(BaseSemanticAnalyzer.unescapeSQLString(ast.getText))
    //case Token(singleQuotedLiteral(str), Nil) => Literal(str)
    case a: ASTNode =>
      throw new NotImplementedError(
        s"No parse rules for ASTNode type: ${a.getType}, text: ${a.getText} :\n ${dumpTree(a).toString}")
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