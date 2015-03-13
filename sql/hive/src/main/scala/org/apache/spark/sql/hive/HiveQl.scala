/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.sql.Date


import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.PlanUtils
import org.apache.spark.sql.{AnalysisException, SparkSQLParser}

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.ExplainCommand
import org.apache.spark.sql.sources.DescribeCommand
import org.apache.spark.sql.hive.execution.{HiveNativeCommand, DropTable, AnalyzeTable, HiveScriptIOSchema}
import org.apache.spark.sql.types._
import org.apache.spark.util.random.RandomSampler

/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * Used when we need to start parsing the AST before deciding that we are going to pass the command
 * back for Hive to execute natively.  Will be replaced with a native command that contains the
 * cmd string.
 */
private[hive] case object NativePlaceholder extends Command

/** Provides a mapping from HiveQL statements to catalyst logical plans and expression trees. */
private[hive] object HiveQl {
  protected val nativeCommands = Seq(
    "TOK_DESCFUNCTION",
    "TOK_DESCDATABASE",
    "TOK_SHOW_CREATETABLE",
    "TOK_SHOWCOLUMNS",
    "TOK_SHOW_TABLESTATUS",
    "TOK_SHOWDATABASES",
    "TOK_SHOWFUNCTIONS",
    "TOK_SHOWINDEXES",
    "TOK_SHOWINDEXES",
    "TOK_SHOWPARTITIONS",
    "TOK_SHOW_TBLPROPERTIES",

    "TOK_LOCKTABLE",
    "TOK_SHOWLOCKS",
    "TOK_UNLOCKTABLE",

    "TOK_SHOW_ROLES",
    "TOK_CREATEROLE",
    "TOK_DROPROLE",
    "TOK_GRANT",
    "TOK_GRANT_ROLE",
    "TOK_REVOKE",
    "TOK_SHOW_GRANT",
    "TOK_SHOW_ROLE_GRANT",
    "TOK_SHOW_SET_ROLE",

    "TOK_CREATEFUNCTION",
    "TOK_DROPFUNCTION",

    "TOK_ALTERDATABASE_PROPERTIES",
    "TOK_ALTERDATABASE_OWNER",
    "TOK_ALTERINDEX_PROPERTIES",
    "TOK_ALTERINDEX_REBUILD",
    "TOK_ALTERTABLE_ADDCOLS",
    "TOK_ALTERTABLE_ADDPARTS",
    "TOK_ALTERTABLE_ALTERPARTS",
    "TOK_ALTERTABLE_ARCHIVE",
    "TOK_ALTERTABLE_CLUSTER_SORT",
    "TOK_ALTERTABLE_DROPPARTS",
    "TOK_ALTERTABLE_PARTITION",
    "TOK_ALTERTABLE_PROPERTIES",
    "TOK_ALTERTABLE_RENAME",
    "TOK_ALTERTABLE_RENAMECOL",
    "TOK_ALTERTABLE_REPLACECOLS",
    "TOK_ALTERTABLE_SKEWED",
    "TOK_ALTERTABLE_TOUCH",
    "TOK_ALTERTABLE_UNARCHIVE",
    "TOK_CREATEDATABASE",
    "TOK_CREATEFUNCTION",
    "TOK_CREATEINDEX",
    "TOK_DROPDATABASE",
    "TOK_DROPINDEX",
    "TOK_DROPTABLE_PROPERTIES",
    "TOK_MSCK",

    "TOK_ALTERVIEW_ADDPARTS",
    "TOK_ALTERVIEW_AS",
    "TOK_ALTERVIEW_DROPPARTS",
    "TOK_ALTERVIEW_PROPERTIES",
    "TOK_ALTERVIEW_RENAME",
    "TOK_CREATEVIEW",
    "TOK_DROPVIEW_PROPERTIES",
    "TOK_DROPVIEW",

    "TOK_EXPORT",
    "TOK_IMPORT",
    "TOK_LOAD",

    "TOK_SWITCHDATABASE"
  )

  // Commands that we do not need to explain.
  protected val noExplainCommands = Seq(
    "TOK_DESCTABLE",
    "TOK_SHOWTABLES",
    "TOK_TRUNCATETABLE"     // truncate table" is a NativeCommand, does not need to explain.
  ) ++ nativeCommands

  protected val hqlParser = {
    val fallback = new ExtendedHiveQlParser
    new SparkSQLParser(fallback(_))
  }

  /**
   * A set of implicit transformations that allow Hive ASTNodes to be rewritten by transformations
   * similar to [[catalyst.trees.TreeNode]].
   *
   * Note that this should be considered very experimental and is not indented as a replacement
   * for TreeNode.  Primarily it should be noted ASTNodes are not immutable and do not appear to
   * have clean copy semantics.  Therefore, users of this class should take care when
   * copying/modifying trees that might be used elsewhere.
   */
  implicit class TransformableNode(n: ASTNode) {
    /**
     * Returns a copy of this node where `rule` has been recursively applied to it and all of its
     * children.  When `rule` does not apply to a given node it is left unchanged.
     * @param rule the function use to transform this nodes children
     */
    def transform(rule: PartialFunction[ASTNode, ASTNode]): ASTNode = {
      try {
        val afterRule = rule.applyOrElse(n, identity[ASTNode])
        afterRule.withChildren(
          nilIfEmpty(afterRule.getChildren)
            .asInstanceOf[Seq[ASTNode]]
            .map(ast => Option(ast).map(_.transform(rule)).orNull))
      } catch {
        case e: Exception =>
          println(dumpTree(n))
          throw e
      }
    }

    /**
     * Returns a scala.Seq equivalent to [s] or Nil if [s] is null.
     */
    private def nilIfEmpty[A](s: java.util.List[A]): Seq[A] =
      Option(s).map(_.toSeq).getOrElse(Nil)

    /**
     * Returns this ASTNode with the text changed to `newText`.
     */
    def withText(newText: String): ASTNode = {
      n.token.asInstanceOf[org.antlr.runtime.CommonToken].setText(newText)
      n
    }

    /**
     * Returns this ASTNode with the children changed to `newChildren`.
     */
    def withChildren(newChildren: Seq[ASTNode]): ASTNode = {
      (1 to n.getChildCount).foreach(_ => n.deleteChild(0))
      n.addChildren(newChildren)
      n
    }

    /**
     * Throws an error if this is not equal to other.
     *
     * Right now this function only checks the name, type, text and children of the node
     * for equality.
     */
    def checkEquals(other: ASTNode) {
      def check(field: String, f: ASTNode => Any) = if (f(n) != f(other)) {
        sys.error(s"$field does not match for trees. " +
          s"'${f(n)}' != '${f(other)}' left: ${dumpTree(n)}, right: ${dumpTree(other)}")
      }
      check("name", _.getName)
      check("type", _.getType)
      check("text", _.getText)
      check("numChildren", n => nilIfEmpty(n.getChildren).size)

      val leftChildren = nilIfEmpty(n.getChildren).asInstanceOf[Seq[ASTNode]]
      val rightChildren = nilIfEmpty(other.getChildren).asInstanceOf[Seq[ASTNode]]
      leftChildren zip rightChildren foreach {
        case (l,r) => l checkEquals r
      }
    }
  }

  /**
   * Returns the AST for the given SQL string.
   */
  def getAst(sql: String): ASTNode = {
    /*
     * Context has to be passed in hive0.13.1.
     * Otherwise, there will be Null pointer exception,
     * when retrieving properties form HiveConf.
     */
    val hContext = new Context(new HiveConf())
    val node = ParseUtils.findRootNonNullToken((new ParseDriver).parse(sql, hContext))
    hContext.clear()
    node
  }


  /** Returns a LogicalPlan for a given HiveQL string. */
  def parseSql(sql: String): LogicalPlan = hqlParser(sql)

  val errorRegEx = "line (\\d+):(\\d+) (.*)".r

  /** Creates LogicalPlan for a given HiveQL string. */
  def createPlan(sql: String): LogicalPlan = {
    try {
      val tree = getAst(sql)
      if (nativeCommands contains tree.getText) {
        HiveNativeCommand(sql)
      } else {
        nodeToPlan(tree) match {
          case NativePlaceholder => HiveNativeCommand(sql)
          case other => other
        }
      }
    } catch {
      case pe: org.apache.hadoop.hive.ql.parse.ParseException =>
        pe.getMessage match {
          case errorRegEx(line, start, message) =>
            throw new AnalysisException(message, Some(line.toInt), Some(start.toInt))
          case otherMessage =>
            throw new AnalysisException(otherMessage)
        }
      case e: Exception =>
        throw new AnalysisException(e.getMessage)
      case e: NotImplementedError =>
        throw new AnalysisException(
          s"""
            |Unsupported language features in query: $sql
            |${dumpTree(getAst(sql))}
            |$e
            |${e.getStackTrace.head}
          """.stripMargin)
    }
  }

  /** Creates LogicalPlan for a given VIEW */
  def createPlanForView(view: Table, alias: Option[String]) = alias match {
    // because hive use things like `_c0` to build the expanded text
    // currently we cannot support view from "create view v1(c1) as ..."
    case None => Subquery(view.getTableName, createPlan(view.getViewExpandedText))
    case Some(aliasText) => Subquery(aliasText, createPlan(view.getViewExpandedText))
  }

  def parseDdl(ddl: String): Seq[Attribute] = {
    val tree =
      try {
        ParseUtils.findRootNonNullToken(
          (new ParseDriver).parse(ddl, null /* no context required for parsing alone */))
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
  object Token {
    /** @return matches of the form (tokenName, children). */
    def unapply(t: Any): Option[(String, Seq[ASTNode])] = t match {
      case t: ASTNode =>
        CurrentOrigin.setPosition(t.getLine, t.getCharPositionInLine)
        Some((t.getText,
          Option(t.getChildren).map(_.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
      case _ => None
    }
  }

  protected def getClauses(clauseNames: Seq[String], nodeList: Seq[ASTNode]): Seq[Option[Node]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.getText.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) matches.tail else Nil)
      matches.headOption
    }

    if (remainingNodes.nonEmpty) {
      sys.error(
        s"""Unhandled clauses: ${remainingNodes.map(dumpTree(_)).mkString("\n")}.
           |You are likely trying to use an unsupported Hive feature."""".stripMargin)
    }
    clauses
  }

  def getClause(clauseName: String, nodeList: Seq[Node]) =
    getClauseOption(clauseName, nodeList).getOrElse(sys.error(
      s"Expected clause $clauseName missing from ${nodeList.map(dumpTree(_)).mkString("\n")}"))

  def getClauseOption(clauseName: String, nodeList: Seq[Node]): Option[Node] = {
    nodeList.filter { case ast: ASTNode => ast.getText == clauseName } match {
      case Seq(oneMatch) => Some(oneMatch)
      case Seq() => None
      case _ => sys.error(s"Found multiple instances of clause $clauseName")
    }
  }

  protected def nodeToAttribute(node: Node): Attribute = node match {
    case Token("TOK_TABCOL", Token(colName, Nil) :: dataType :: Nil) =>
      AttributeReference(colName, nodeToDataType(dataType), true)()

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToDataType(node: Node): DataType = node match {
    case Token("TOK_DECIMAL", precision :: scale :: Nil) =>
      DecimalType(precision.getText.toInt, scale.getText.toInt)
    case Token("TOK_DECIMAL", precision :: Nil) =>
      DecimalType(precision.getText.toInt, 0)
    case Token("TOK_DECIMAL", Nil) => DecimalType.Unlimited
    case Token("TOK_BIGINT", Nil) => LongType
    case Token("TOK_INT", Nil) => IntegerType
    case Token("TOK_TINYINT", Nil) => ByteType
    case Token("TOK_SMALLINT", Nil) => ShortType
    case Token("TOK_BOOLEAN", Nil) => BooleanType
    case Token("TOK_STRING", Nil) => StringType
    case Token("TOK_VARCHAR", Token(_, Nil) :: Nil) => StringType
    case Token("TOK_FLOAT", Nil) => FloatType
    case Token("TOK_DOUBLE", Nil) => DoubleType
    case Token("TOK_DATE", Nil) => DateType
    case Token("TOK_TIMESTAMP", Nil) => TimestampType
    case Token("TOK_BINARY", Nil) => BinaryType
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
      StructField(fieldName, nodeToDataType(dataType), nullable = true)
    case Token("TOK_TABCOL",
           Token(fieldName, Nil) ::
             dataType ::
             _ /* comment */:: Nil) =>
      StructField(fieldName, nodeToDataType(dataType), nullable = true)
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for StructField:\n ${dumpTree(a).toString} ")
  }

  protected def nameExpressions(exprs: Seq[Expression]): Seq[NamedExpression] = {
    exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"_c$i")()
    }
  }

  protected def extractDbNameTableName(tableNameParts: Node): (Option[String], String) = {
    val (db, tableName) =
      tableNameParts.getChildren.map { case Token(part, Nil) => cleanIdentifier(part) } match {
        case Seq(tableOnly) => (None, tableOnly)
        case Seq(databaseName, table) => (Some(databaseName), table)
      }

    (db, tableName)
  }

  protected def extractTableIdent(tableNameParts: Node): Seq[String] = {
    tableNameParts.getChildren.map { case Token(part, Nil) => cleanIdentifier(part) } match {
      case Seq(tableOnly) => Seq(tableOnly)
      case Seq(databaseName, table) => Seq(databaseName, table)
      case other => sys.error("Hive only supports tables names like 'tableName' " +
        s"or 'databaseName.tableName', found '$other'")
    }
  }

  /**
   * SELECT MAX(value) FROM src GROUP BY k1, k2, k3 GROUPING SETS((k1, k2), (k2)) 
   * is equivalent to 
   * SELECT MAX(value) FROM src GROUP BY k1, k2 UNION SELECT MAX(value) FROM src GROUP BY k2
   * Check the following link for details.
   * 
https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup
   *
   * The bitmask denotes the grouping expressions validity for a grouping set,
   * the bitmask also be called as grouping id (`GROUPING__ID`, the virtual column in Hive)
   * e.g. In superset (k1, k2, k3), (bit 0: k1, bit 1: k2, and bit 2: k3), the grouping id of 
   * GROUPING SETS (k1, k2) and (k2) should be 3 and 2 respectively.
   */
  protected def extractGroupingSet(children: Seq[ASTNode]): (Seq[Expression], Seq[Int]) = {
    val (keyASTs, setASTs) = children.partition( n => n match {
        case Token("TOK_GROUPING_SETS_EXPRESSION", children) => false // grouping sets
        case _ => true // grouping keys
      })

    val keys = keyASTs.map(nodeToExpr).toSeq
    val keyMap = keyASTs.map(_.toStringTree).zipWithIndex.toMap

    val bitmasks: Seq[Int] = setASTs.map(set => set match {
      case Token("TOK_GROUPING_SETS_EXPRESSION", null) => 0
      case Token("TOK_GROUPING_SETS_EXPRESSION", children) => 
        children.foldLeft(0)((bitmap, col) => {
          val colString = col.asInstanceOf[ASTNode].toStringTree()
          require(keyMap.contains(colString), s"$colString doens't show up in the GROUP BY list")
          bitmap | 1 << keyMap(colString)
        })
      case _ => sys.error("Expect GROUPING SETS clause")
    })

    (keys, bitmasks)
  }

  protected def nodeToPlan(node: Node): LogicalPlan = node match {
    // Special drop table that also uncaches.
    case Token("TOK_DROPTABLE",
           Token("TOK_TABNAME", tableNameParts) ::
           ifExists) =>
      val tableName = tableNameParts.map { case Token(p, Nil) => p }.mkString(".")
      DropTable(tableName, ifExists.nonEmpty)
    // Support "ANALYZE TABLE tableNmae COMPUTE STATISTICS noscan"
    case Token("TOK_ANALYZE",
            Token("TOK_TAB", Token("TOK_TABNAME", tableNameParts) :: partitionSpec) ::
            isNoscan) =>
      // Reference:
      // https://cwiki.apache.org/confluence/display/Hive/StatsDev#StatsDev-ExistingTables
      if (partitionSpec.nonEmpty) {
        // Analyze partitions will be treated as a Hive native command.
        NativePlaceholder
      } else if (isNoscan.isEmpty) {
        // If users do not specify "noscan", it will be treated as a Hive native command.
        NativePlaceholder
      } else {
        val tableName = tableNameParts.map { case Token(p, Nil) => p }.mkString(".")
        AnalyzeTable(tableName)
      }
    // Just fake explain for any of the native commands.
    case Token("TOK_EXPLAIN", explainArgs)
      if noExplainCommands.contains(explainArgs.head.getText) =>
      ExplainCommand(NoRelation)
    case Token("TOK_EXPLAIN", explainArgs)
      if "TOK_CREATETABLE" == explainArgs.head.getText =>
      val Some(crtTbl) :: _ :: extended :: Nil =
        getClauses(Seq("TOK_CREATETABLE", "FORMATTED", "EXTENDED"), explainArgs)
      ExplainCommand(
        nodeToPlan(crtTbl),
        extended = extended.isDefined)
    case Token("TOK_EXPLAIN", explainArgs) =>
      // Ignore FORMATTED if present.
      val Some(query) :: _ :: extended :: Nil =
        getClauses(Seq("TOK_QUERY", "FORMATTED", "EXTENDED"), explainArgs)
      ExplainCommand(
        nodeToPlan(query),
        extended = extended.isDefined)

    case Token("TOK_DESCTABLE", describeArgs) =>
      // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
      val Some(tableType) :: formatted :: extended :: pretty :: Nil =
        getClauses(Seq("TOK_TABTYPE", "FORMATTED", "EXTENDED", "PRETTY"), describeArgs)
      if (formatted.isDefined || pretty.isDefined) {
        // FORMATTED and PRETTY are not supported and this statement will be treated as
        // a Hive native command.
        NativePlaceholder
      } else {
        tableType match {
          case Token("TOK_TABTYPE", nameParts) if nameParts.size == 1 => {
            nameParts.head match {
              case Token(".", dbName :: tableName :: Nil) =>
                // It is describing a table with the format like "describe db.table".
                // TODO: Actually, a user may mean tableName.columnName. Need to resolve this issue.
                val tableIdent = extractTableIdent(nameParts.head)
                DescribeCommand(
                  UnresolvedRelation(tableIdent, None), isExtended = extended.isDefined)
              case Token(".", dbName :: tableName :: colName :: Nil) =>
                // It is describing a column with the format like "describe db.table column".
                NativePlaceholder
              case tableName =>
                // It is describing a table with the format like "describe table".
                DescribeCommand(
                  UnresolvedRelation(Seq(tableName.getText), None), isExtended = extended.isDefined)
            }
          }
          // All other cases.
          case _ => NativePlaceholder
        }
      }

    case Token("TOK_CREATETABLE", children)
        if children.collect { case t @ Token("TOK_QUERY", _) => t }.nonEmpty =>
      // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
      val (
          Some(tableNameParts) ::
          _ /* likeTable */ ::
          Some(query) ::
          allowExisting +:
          ignores) =
        getClauses(
          Seq(
            "TOK_TABNAME",
            "TOK_LIKETABLE",
            "TOK_QUERY",
            "TOK_IFNOTEXISTS",
            "TOK_TABLECOMMENT",
            "TOK_TABCOLLIST",
            "TOK_TABLEPARTCOLS", // Partitioned by
            "TOK_TABLEBUCKETS", // Clustered by
            "TOK_TABLESKEWED", // Skewed by
            "TOK_TABLEROWFORMAT",
            "TOK_TABLESERIALIZER",
            "TOK_FILEFORMAT_GENERIC", // For file formats not natively supported by Hive.
            "TOK_TBLSEQUENCEFILE", // Stored as SequenceFile
            "TOK_TBLTEXTFILE", // Stored as TextFile
            "TOK_TBLRCFILE", // Stored as RCFile
            "TOK_TBLORCFILE", // Stored as ORC File
            "TOK_TBLPARQUETFILE", // Stored as PARQUET
            "TOK_TABLEFILEFORMAT", // User-provided InputFormat and OutputFormat
            "TOK_STORAGEHANDLER", // Storage handler
            "TOK_TABLELOCATION",
            "TOK_TABLEPROPERTIES"),
          children)
      val (db, tableName) = extractDbNameTableName(tableNameParts)

      CreateTableAsSelect(db, tableName, nodeToPlan(query), allowExisting != None, Some(node))

    // If its not a "CREATE TABLE AS" like above then just pass it back to hive as a native command.
    case Token("TOK_CREATETABLE", _) => NativePlaceholder

    // Support "TRUNCATE TABLE table_name [PARTITION partition_spec]"
    case Token("TOK_TRUNCATETABLE",
          Token("TOK_TABLE_PARTITION",table)::Nil) =>  NativePlaceholder

    case Token("TOK_QUERY", queryArgs)
        if Seq("TOK_FROM", "TOK_INSERT").contains(queryArgs.head.getText) =>

      val (fromClause: Option[ASTNode], insertClauses) = queryArgs match {
        case Token("TOK_FROM", args: Seq[ASTNode]) :: insertClauses =>
          (Some(args.head), insertClauses)
        case Token("TOK_INSERT", _) :: Nil => (None, queryArgs)
      }

      // Return one query for each insert clause.
      val queries = insertClauses.map { case Token("TOK_INSERT", singleInsert) =>
        val (
            intoClause ::
            destClause ::
            selectClause ::
            selectDistinctClause ::
            whereClause ::
            groupByClause ::
            rollupGroupByClause ::
            cubeGroupByClause ::
            groupingSetsClause ::
            orderByClause ::
            havingClause ::
            sortByClause ::
            clusterByClause ::
            distributeByClause ::
            limitClause ::
            lateralViewClause :: Nil) = {
          getClauses(
            Seq(
              "TOK_INSERT_INTO",
              "TOK_DESTINATION",
              "TOK_SELECT",
              "TOK_SELECTDI",
              "TOK_WHERE",
              "TOK_GROUPBY",
              "TOK_ROLLUP_GROUPBY",
              "TOK_CUBE_GROUPBY",
              "TOK_GROUPING_SETS",
              "TOK_ORDERBY",
              "TOK_HAVING",
              "TOK_SORTBY",
              "TOK_CLUSTERBY",
              "TOK_DISTRIBUTEBY",
              "TOK_LIMIT",
              "TOK_LATERAL_VIEW"),
            singleInsert)
        }
 
        val relations = fromClause match {
          case Some(f) => nodeToRelation(f)
          case None => NoRelation
        }
 
        val withWhere = whereClause.map { whereNode =>
          val Seq(whereExpr) = whereNode.getChildren.toSeq
          Filter(nodeToExpr(whereExpr), relations)
        }.getOrElse(relations)

        val select =
          (selectClause orElse selectDistinctClause).getOrElse(sys.error("No select clause."))

        // Script transformations are expressed as a select clause with a single expression of type
        // TOK_TRANSFORM
        val transformation = select.getChildren.head match {
          case Token("TOK_SELEXPR",
                 Token("TOK_TRANSFORM",
                   Token("TOK_EXPLIST", inputExprs) ::
                   Token("TOK_SERDE", inputSerdeClause) ::
                   Token("TOK_RECORDWRITER", writerClause) ::
                   // TODO: Need to support other types of (in/out)put
                   Token(script, Nil) ::
                   Token("TOK_SERDE", outputSerdeClause) ::
                   Token("TOK_RECORDREADER", readerClause) ::
                   outputClause) :: Nil) =>

            val (output, schemaLess) = outputClause match {
              case Token("TOK_ALIASLIST", aliases) :: Nil =>
                (aliases.map { case Token(name, Nil) => AttributeReference(name, StringType)() },
                  false)
              case Token("TOK_TABCOLLIST", attributes) :: Nil =>
                (attributes.map { case Token("TOK_TABCOL", Token(name, Nil) :: dataType :: Nil) =>
                  AttributeReference(name, nodeToDataType(dataType))() }, false)
              case Nil =>
                (List(AttributeReference("key", StringType)(),
                  AttributeReference("value", StringType)()), true)
            }

            def matchSerDe(clause: Seq[ASTNode]) = clause match {
              case Token("TOK_SERDEPROPS", propsClause) :: Nil =>
                val rowFormat = propsClause.map {
                  case Token(name, Token(value, Nil) :: Nil) => (name, value)
                }
                (rowFormat, "", Nil)

              case Token("TOK_SERDENAME", Token(serdeClass, Nil) :: Nil) :: Nil =>
                (Nil, serdeClass, Nil)

              case Token("TOK_SERDENAME", Token(serdeClass, Nil) ::
                Token("TOK_TABLEPROPERTIES",
                Token("TOK_TABLEPROPLIST", propsClause) :: Nil) :: Nil) :: Nil =>
                val serdeProps = propsClause.map {
                  case Token("TOK_TABLEPROPERTY", Token(name, Nil) :: Token(value, Nil) :: Nil) =>
                    (name, value)
                } 
                (Nil, serdeClass, serdeProps)

              case Nil => (Nil, "", Nil)
            }

            val (inRowFormat, inSerdeClass, inSerdeProps) = matchSerDe(inputSerdeClause)
            val (outRowFormat, outSerdeClass, outSerdeProps) = matchSerDe(outputSerdeClause)

            val unescapedScript = BaseSemanticAnalyzer.unescapeSQLString(script)

            val schema = HiveScriptIOSchema(
              inRowFormat, outRowFormat,
              inSerdeClass, outSerdeClass,
              inSerdeProps, outSerdeProps, schemaLess)

            Some(
              logical.ScriptTransformation(
                inputExprs.map(nodeToExpr),
                unescapedScript,
                output,
                withWhere, schema))
          case _ => None
        }

        val withLateralView = lateralViewClause.map { lv =>
          val Token("TOK_SELECT",
          Token("TOK_SELEXPR", clauses) :: Nil) = lv.getChildren.head

          val alias =
            getClause("TOK_TABALIAS", clauses).getChildren.head.asInstanceOf[ASTNode].getText

          Generate(
            nodesToGenerator(clauses),
            join = true,
            outer = false,
            Some(alias.toLowerCase),
            withWhere)
        }.getOrElse(withWhere)

        // The projection of the query can either be a normal projection, an aggregation
        // (if there is a group by) or a script transformation.
        val withProject: LogicalPlan = transformation.getOrElse {
          val selectExpressions = 
            nameExpressions(select.getChildren.flatMap(selExprNodeToExpr).toSeq)
          Seq(
            groupByClause.map(e => e match {
              case Token("TOK_GROUPBY", children) =>
                // Not a transformation so must be either project or aggregation.
                Aggregate(children.map(nodeToExpr), selectExpressions, withLateralView)
              case _ => sys.error("Expect GROUP BY")
            }),
            groupingSetsClause.map(e => e match {
              case Token("TOK_GROUPING_SETS", children) =>
                val(groupByExprs, masks) = extractGroupingSet(children)
                GroupingSets(masks, groupByExprs, withLateralView, selectExpressions)
              case _ => sys.error("Expect GROUPING SETS")
            }),
            rollupGroupByClause.map(e => e match {
              case Token("TOK_ROLLUP_GROUPBY", children) =>
                Rollup(children.map(nodeToExpr), withLateralView, selectExpressions)
              case _ => sys.error("Expect WITH ROLLUP")
            }),
            cubeGroupByClause.map(e => e match {
              case Token("TOK_CUBE_GROUPBY", children) =>
                Cube(children.map(nodeToExpr), withLateralView, selectExpressions)
              case _ => sys.error("Expect WITH CUBE")
            }), 
            Some(Project(selectExpressions, withLateralView))).flatten.head
        }

        val withDistinct =
          if (selectDistinctClause.isDefined) Distinct(withProject) else withProject

        val withHaving = havingClause.map { h =>
          val havingExpr = h.getChildren.toSeq match { case Seq(hexpr) => nodeToExpr(hexpr) }
          // Note that we added a cast to boolean. If the expression itself is already boolean,
          // the optimizer will get rid of the unnecessary cast.
          Filter(Cast(havingExpr, BooleanType), withDistinct)
        }.getOrElse(withDistinct)

        val withSort =
          (orderByClause, sortByClause, distributeByClause, clusterByClause) match {
            case (Some(totalOrdering), None, None, None) =>
              Sort(totalOrdering.getChildren.map(nodeToSortOrder), true, withHaving)
            case (None, Some(perPartitionOrdering), None, None) =>
              Sort(perPartitionOrdering.getChildren.map(nodeToSortOrder), false, withHaving)
            case (None, None, Some(partitionExprs), None) =>
              Repartition(partitionExprs.getChildren.map(nodeToExpr), withHaving)
            case (None, Some(perPartitionOrdering), Some(partitionExprs), None) =>
              Sort(perPartitionOrdering.getChildren.map(nodeToSortOrder), false,
                Repartition(partitionExprs.getChildren.map(nodeToExpr), withHaving))
            case (None, None, None, Some(clusterExprs)) =>
              Sort(clusterExprs.getChildren.map(nodeToExpr).map(SortOrder(_, Ascending)), false,
                Repartition(clusterExprs.getChildren.map(nodeToExpr), withHaving))
            case (None, None, None, None) => withHaving
            case _ => sys.error("Unsupported set of ordering / distribution clauses.")
          }

        val withLimit =
          limitClause.map(l => nodeToExpr(l.getChildren.head))
            .map(Limit(_, withSort))
            .getOrElse(withSort)

        // TOK_INSERT_INTO means to add files to the table.
        // TOK_DESTINATION means to overwrite the table.
        val resultDestination =
          (intoClause orElse destClause).getOrElse(sys.error("No destination found."))
        val overwrite = intoClause.isEmpty
        nodeToDest(
          resultDestination,
          withLimit,
          overwrite)
      }

      // If there are multiple INSERTS just UNION them together into on query.
      queries.reduceLeft(Union)

    case Token("TOK_UNION", left :: right :: Nil) => Union(nodeToPlan(left), nodeToPlan(right))

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  val allJoinTokens = "(TOK_.*JOIN)".r
  val laterViewToken = "TOK_LATERAL_VIEW(.*)".r
  def nodeToRelation(node: Node): LogicalPlan = node match {
    case Token("TOK_SUBQUERY",
           query :: Token(alias, Nil) :: Nil) =>
      Subquery(cleanIdentifier(alias), nodeToPlan(query))

    case Token(laterViewToken(isOuter), selectClause :: relationClause :: Nil) =>
      val Token("TOK_SELECT",
            Token("TOK_SELEXPR", clauses) :: Nil) = selectClause

      val alias = getClause("TOK_TABALIAS", clauses).getChildren.head.asInstanceOf[ASTNode].getText

      Generate(
        nodesToGenerator(clauses),
        join = true,
        outer = isOuter.nonEmpty,
        Some(alias.toLowerCase),
        nodeToRelation(relationClause))

    /* All relations, possibly with aliases or sampling clauses. */
    case Token("TOK_TABREF", clauses) =>
      // If the last clause is not a token then it's the alias of the table.
      val (nonAliasClauses, aliasClause) =
        if (clauses.last.getText.startsWith("TOK")) {
          (clauses, None)
        } else {
          (clauses.dropRight(1), Some(clauses.last))
        }

      val (Some(tableNameParts) ::
          splitSampleClause ::
          bucketSampleClause :: Nil) = {
        getClauses(Seq("TOK_TABNAME", "TOK_TABLESPLITSAMPLE", "TOK_TABLEBUCKETSAMPLE"),
          nonAliasClauses)
      }

      val tableIdent =
        tableNameParts.getChildren.map{ case Token(part, Nil) => cleanIdentifier(part)} match {
          case Seq(tableOnly) => Seq(tableOnly)
          case Seq(databaseName, table) => Seq(databaseName, table)
          case other => sys.error("Hive only supports tables names like 'tableName' " +
            s"or 'databaseName.tableName', found '$other'")
      }
      val alias = aliasClause.map { case Token(a, Nil) => cleanIdentifier(a) }
      val relation = UnresolvedRelation(tableIdent, alias)

      // Apply sampling if requested.
      (bucketSampleClause orElse splitSampleClause).map {
        case Token("TOK_TABLESPLITSAMPLE",
               Token("TOK_ROWCOUNT", Nil) ::
               Token(count, Nil) :: Nil) =>
          Limit(Literal(count.toInt), relation)
        case Token("TOK_TABLESPLITSAMPLE",
               Token("TOK_PERCENT", Nil) ::
               Token(fraction, Nil) :: Nil) =>
          // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
          // function takes X PERCENT as the input and the range of X is [0, 100], we need to
          // adjust the fraction.
          require(
            fraction.toDouble >= (0.0 - RandomSampler.roundingEpsilon)
              && fraction.toDouble <= (100.0 + RandomSampler.roundingEpsilon),
            s"Sampling fraction ($fraction) must be on interval [0, 100]")
          Sample(fraction.toDouble / 100, withReplacement = false, (math.random * 1000).toInt,
            relation)
        case Token("TOK_TABLEBUCKETSAMPLE",
               Token(numerator, Nil) ::
               Token(denominator, Nil) :: Nil) =>
          val fraction = numerator.toDouble / denominator.toDouble
          Sample(fraction, withReplacement = false, (math.random * 1000).toInt, relation)
        case a: ASTNode =>
          throw new NotImplementedError(
            s"""No parse rules for sampling clause: ${a.getType}, text: ${a.getText} :
           |${dumpTree(a).toString}" +
         """.stripMargin)
      }.getOrElse(relation)

    case Token("TOK_UNIQUEJOIN", joinArgs) =>
      val tableOrdinals =
        joinArgs.zipWithIndex.filter {
          case (arg, i) => arg.getText == "TOK_TABREF"
        }.map(_._2)

      val isPreserved = tableOrdinals.map(i => (i - 1 < 0) || joinArgs(i - 1).getText == "PRESERVE")
      val tables = tableOrdinals.map(i => nodeToRelation(joinArgs(i)))
      val joinExpressions = tableOrdinals.map(i => joinArgs(i + 1).getChildren.map(nodeToExpr))

      val joinConditions = joinExpressions.sliding(2).map {
        case Seq(c1, c2) =>
          val predicates = (c1, c2).zipped.map { case (e1, e2) => EqualTo(e1, e2): Expression }
          predicates.reduceLeft(And)
      }.toBuffer

      val joinType = isPreserved.sliding(2).map {
        case Seq(true, true) => FullOuter
        case Seq(true, false) => LeftOuter
        case Seq(false, true) => RightOuter
        case Seq(false, false) => Inner
      }.toBuffer

      val joinedTables = tables.reduceLeft(Join(_,_, Inner, None))

      // Must be transform down.
      val joinedResult = joinedTables transform {
        case j: Join =>
          j.copy(
            condition = Some(joinConditions.remove(joinConditions.length - 1)),
            joinType = joinType.remove(joinType.length - 1))
      }

      val groups = (0 until joinExpressions.head.size).map(i => Coalesce(joinExpressions.map(_(i))))

      // Unique join is not really the same as an outer join so we must group together results where
      // the joinExpressions are the same, taking the First of each value is only okay because the
      // user of a unique join is implicitly promising that there is only one result.
      // TODO: This doesn't actually work since [[Star]] is not a valid aggregate expression.
      // instead we should figure out how important supporting this feature is and whether it is
      // worth the number of hacks that will be required to implement it.  Namely, we need to add
      // some sort of mapped star expansion that would expand all child output row to be similarly
      // named output expressions where some aggregate expression has been applied (i.e. First).
      ??? // Aggregate(groups, Star(None, First(_)) :: Nil, joinedResult)

    case Token(allJoinTokens(joinToken),
           relation1 ::
           relation2 :: other) =>
      if (!(other.size <= 1)) {
        sys.error(s"Unsupported join operation: $other")
      }

      val joinType = joinToken match {
        case "TOK_JOIN" => Inner
        case "TOK_CROSSJOIN" => Inner
        case "TOK_RIGHTOUTERJOIN" => RightOuter
        case "TOK_LEFTOUTERJOIN" => LeftOuter
        case "TOK_FULLOUTERJOIN" => FullOuter
        case "TOK_LEFTSEMIJOIN" => LeftSemi
      }
      Join(nodeToRelation(relation1),
        nodeToRelation(relation2),
        joinType,
        other.headOption.map(nodeToExpr))

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  def nodeToSortOrder(node: Node): SortOrder = node match {
    case Token("TOK_TABSORTCOLNAMEASC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Ascending)
    case Token("TOK_TABSORTCOLNAMEDESC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Descending)

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  val destinationToken = "TOK_DESTINATION|TOK_INSERT_INTO".r
  protected def nodeToDest(
      node: Node,
      query: LogicalPlan,
      overwrite: Boolean): LogicalPlan = node match {
    case Token(destinationToken(),
           Token("TOK_DIR",
             Token("TOK_TMP_FILE", Nil) :: Nil) :: Nil) =>
      query

    case Token(destinationToken(),
           Token("TOK_TAB",
              tableArgs) :: Nil) =>
      val Some(tableNameParts) :: partitionClause :: Nil =
        getClauses(Seq("TOK_TABNAME", "TOK_PARTSPEC"), tableArgs)

      val tableIdent = extractTableIdent(tableNameParts)

      val partitionKeys = partitionClause.map(_.getChildren.map {
        // Parse partitions. We also make keys case insensitive.
        case Token("TOK_PARTVAL", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> Some(PlanUtils.stripQuotes(value))
        case Token("TOK_PARTVAL", Token(key, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> None
      }.toMap).getOrElse(Map.empty)

      InsertIntoTable(UnresolvedRelation(tableIdent, None), partitionKeys, query, overwrite)

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def selExprNodeToExpr(node: Node): Option[Expression] = node match {
    case Token("TOK_SELEXPR", e :: Nil) =>
      Some(nodeToExpr(e))

    case Token("TOK_SELEXPR", e :: Token(alias, Nil) :: Nil) =>
      Some(Alias(nodeToExpr(e), cleanIdentifier(alias))())

    case Token("TOK_SELEXPR", e :: aliasChildren) =>
      var aliasNames = ArrayBuffer[String]()
      aliasChildren.foreach { _ match {
        case Token(name, Nil) => aliasNames += cleanIdentifier(name)
        case _ =>
        }
      }
      Some(MultiAlias(nodeToExpr(e), aliasNames))

    /* Hints are ignored */
    case Token("TOK_HINTLIST", _) => None

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }


  protected val escapedIdentifier = "`([^`]+)`".r
  /** Strips backticks from ident if present */
  protected def cleanIdentifier(ident: String): String = ident match {
    case escapedIdentifier(i) => i
    case plainIdent => plainIdent
  }

  val numericAstTypes = Seq(
    HiveParser.Number,
    HiveParser.TinyintLiteral,
    HiveParser.SmallintLiteral,
    HiveParser.BigintLiteral,
    HiveParser.DecimalLiteral)

  /* Case insensitive matches */
  val ARRAY = "(?i)ARRAY".r
  val COALESCE = "(?i)COALESCE".r
  val COUNT = "(?i)COUNT".r
  val AVG = "(?i)AVG".r
  val SUM = "(?i)SUM".r
  val MAX = "(?i)MAX".r
  val MIN = "(?i)MIN".r
  val UPPER = "(?i)UPPER".r
  val LOWER = "(?i)LOWER".r
  val RAND = "(?i)RAND".r
  val AND = "(?i)AND".r
  val OR = "(?i)OR".r
  val NOT = "(?i)NOT".r
  val TRUE = "(?i)TRUE".r
  val FALSE = "(?i)FALSE".r
  val LIKE = "(?i)LIKE".r
  val RLIKE = "(?i)RLIKE".r
  val REGEXP = "(?i)REGEXP".r
  val IN = "(?i)IN".r
  val DIV = "(?i)DIV".r
  val BETWEEN = "(?i)BETWEEN".r
  val WHEN = "(?i)WHEN".r
  val CASE = "(?i)CASE".r
  val SUBSTR = "(?i)SUBSTR(?:ING)?".r
  val SQRT = "(?i)SQRT".r

  protected def nodeToExpr(node: Node): Expression = node match {
    /* Attribute References */
    case Token("TOK_TABLE_OR_COL",
           Token(name, Nil) :: Nil) =>
      UnresolvedAttribute(cleanIdentifier(name))
    case Token(".", qualifier :: Token(attr, Nil) :: Nil) =>
      nodeToExpr(qualifier) match {
        case UnresolvedAttribute(qualifierName) =>
          UnresolvedAttribute(qualifierName + "." + cleanIdentifier(attr))
        case other => UnresolvedGetField(other, attr)
      }

    /* Stars (*) */
    case Token("TOK_ALLCOLREF", Nil) => UnresolvedStar(None)
    // The format of dbName.tableName.* cannot be parsed by HiveParser. TOK_TABNAME will only
    // has a single child which is tableName.
    case Token("TOK_ALLCOLREF", Token("TOK_TABNAME", Token(name, Nil) :: Nil) :: Nil) =>
      UnresolvedStar(Some(name))

    /* Aggregate Functions */
    case Token("TOK_FUNCTION", Token(AVG(), Nil) :: arg :: Nil) => Average(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(COUNT(), Nil) :: arg :: Nil) => Count(nodeToExpr(arg))
    case Token("TOK_FUNCTIONSTAR", Token(COUNT(), Nil) :: Nil) => Count(Literal(1))
    case Token("TOK_FUNCTIONDI", Token(COUNT(), Nil) :: args) => CountDistinct(args.map(nodeToExpr))
    case Token("TOK_FUNCTION", Token(SUM(), Nil) :: arg :: Nil) => Sum(nodeToExpr(arg))
    case Token("TOK_FUNCTIONDI", Token(SUM(), Nil) :: arg :: Nil) => SumDistinct(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(MAX(), Nil) :: arg :: Nil) => Max(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(MIN(), Nil) :: arg :: Nil) => Min(nodeToExpr(arg))

    /* System functions about string operations */
    case Token("TOK_FUNCTION", Token(UPPER(), Nil) :: arg :: Nil) => Upper(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(LOWER(), Nil) :: arg :: Nil) => Lower(nodeToExpr(arg))

    /* Casts */
    case Token("TOK_FUNCTION", Token("TOK_STRING", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), StringType)
    case Token("TOK_FUNCTION", Token("TOK_VARCHAR", _) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), StringType)
    case Token("TOK_FUNCTION", Token("TOK_CHAR", _) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), StringType)
    case Token("TOK_FUNCTION", Token("TOK_INT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), IntegerType)
    case Token("TOK_FUNCTION", Token("TOK_BIGINT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), LongType)
    case Token("TOK_FUNCTION", Token("TOK_FLOAT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), FloatType)
    case Token("TOK_FUNCTION", Token("TOK_DOUBLE", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DoubleType)
    case Token("TOK_FUNCTION", Token("TOK_SMALLINT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), ShortType)
    case Token("TOK_FUNCTION", Token("TOK_TINYINT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), ByteType)
    case Token("TOK_FUNCTION", Token("TOK_BINARY", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), BinaryType)
    case Token("TOK_FUNCTION", Token("TOK_BOOLEAN", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), BooleanType)
    case Token("TOK_FUNCTION", Token("TOK_DECIMAL", precision :: scale :: nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DecimalType(precision.getText.toInt, scale.getText.toInt))
    case Token("TOK_FUNCTION", Token("TOK_DECIMAL", precision :: Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DecimalType(precision.getText.toInt, 0))
    case Token("TOK_FUNCTION", Token("TOK_DECIMAL", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DecimalType.Unlimited)
    case Token("TOK_FUNCTION", Token("TOK_TIMESTAMP", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), TimestampType)
    case Token("TOK_FUNCTION", Token("TOK_DATE", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DateType)

    /* Arithmetic */
    case Token("+", child :: Nil) => nodeToExpr(child)
    case Token("-", child :: Nil) => UnaryMinus(nodeToExpr(child))
    case Token("~", child :: Nil) => BitwiseNot(nodeToExpr(child))
    case Token("+", left :: right:: Nil) => Add(nodeToExpr(left), nodeToExpr(right))
    case Token("-", left :: right:: Nil) => Subtract(nodeToExpr(left), nodeToExpr(right))
    case Token("*", left :: right:: Nil) => Multiply(nodeToExpr(left), nodeToExpr(right))
    case Token("/", left :: right:: Nil) => Divide(nodeToExpr(left), nodeToExpr(right))
    case Token(DIV(), left :: right:: Nil) =>
      Cast(Divide(nodeToExpr(left), nodeToExpr(right)), LongType)
    case Token("%", left :: right:: Nil) => Remainder(nodeToExpr(left), nodeToExpr(right))
    case Token("&", left :: right:: Nil) => BitwiseAnd(nodeToExpr(left), nodeToExpr(right))
    case Token("|", left :: right:: Nil) => BitwiseOr(nodeToExpr(left), nodeToExpr(right))
    case Token("^", left :: right:: Nil) => BitwiseXor(nodeToExpr(left), nodeToExpr(right))
    case Token("TOK_FUNCTION", Token(SQRT(), Nil) :: arg :: Nil) => Sqrt(nodeToExpr(arg))

    /* Comparisons */
    case Token("=", left :: right:: Nil) => EqualTo(nodeToExpr(left), nodeToExpr(right))
    case Token("==", left :: right:: Nil) => EqualTo(nodeToExpr(left), nodeToExpr(right))
    case Token("<=>", left :: right:: Nil) => EqualNullSafe(nodeToExpr(left), nodeToExpr(right))
    case Token("!=", left :: right:: Nil) => Not(EqualTo(nodeToExpr(left), nodeToExpr(right)))
    case Token("<>", left :: right:: Nil) => Not(EqualTo(nodeToExpr(left), nodeToExpr(right)))
    case Token(">", left :: right:: Nil) => GreaterThan(nodeToExpr(left), nodeToExpr(right))
    case Token(">=", left :: right:: Nil) => GreaterThanOrEqual(nodeToExpr(left), nodeToExpr(right))
    case Token("<", left :: right:: Nil) => LessThan(nodeToExpr(left), nodeToExpr(right))
    case Token("<=", left :: right:: Nil) => LessThanOrEqual(nodeToExpr(left), nodeToExpr(right))
    case Token(LIKE(), left :: right:: Nil) => Like(nodeToExpr(left), nodeToExpr(right))
    case Token(RLIKE(), left :: right:: Nil) => RLike(nodeToExpr(left), nodeToExpr(right))
    case Token(REGEXP(), left :: right:: Nil) => RLike(nodeToExpr(left), nodeToExpr(right))
    case Token("TOK_FUNCTION", Token("TOK_ISNOTNULL", Nil) :: child :: Nil) =>
      IsNotNull(nodeToExpr(child))
    case Token("TOK_FUNCTION", Token("TOK_ISNULL", Nil) :: child :: Nil) =>
      IsNull(nodeToExpr(child))
    case Token("TOK_FUNCTION", Token(IN(), Nil) :: value :: list) =>
      In(nodeToExpr(value), list.map(nodeToExpr))
    case Token("TOK_FUNCTION",
           Token(BETWEEN(), Nil) ::
           kw ::
           target ::
           minValue ::
           maxValue :: Nil) =>

      val targetExpression = nodeToExpr(target)
      val betweenExpr =
        And(
          GreaterThanOrEqual(targetExpression, nodeToExpr(minValue)),
          LessThanOrEqual(targetExpression, nodeToExpr(maxValue)))
      kw match {
        case Token("KW_FALSE", Nil) => betweenExpr
        case Token("KW_TRUE", Nil) => Not(betweenExpr)
      }

    /* Boolean Logic */
    case Token(AND(), left :: right:: Nil) => And(nodeToExpr(left), nodeToExpr(right))
    case Token(OR(), left :: right:: Nil) => Or(nodeToExpr(left), nodeToExpr(right))
    case Token(NOT(), child :: Nil) => Not(nodeToExpr(child))
    case Token("!", child :: Nil) => Not(nodeToExpr(child))

    /* Case statements */
    case Token("TOK_FUNCTION", Token(WHEN(), Nil) :: branches) =>
      CaseWhen(branches.map(nodeToExpr))
    case Token("TOK_FUNCTION", Token(CASE(), Nil) :: branches) =>
      val transformed = branches.drop(1).sliding(2, 2).map {
        case Seq(condVal, value) =>
          // FIXME (SPARK-2155): the key will get evaluated for multiple times in CaseWhen's eval().
          // Hence effectful / non-deterministic key expressions are *not* supported at the moment.
          // We should consider adding new Expressions to get around this.
          Seq(EqualTo(nodeToExpr(branches(0)), nodeToExpr(condVal)),
              nodeToExpr(value))
        case Seq(elseVal) => Seq(nodeToExpr(elseVal))
      }.toSeq.reduce(_ ++ _)
      CaseWhen(transformed)

    /* Complex datatype manipulation */
    case Token("[", child :: ordinal :: Nil) =>
      GetItem(nodeToExpr(child), nodeToExpr(ordinal))

    /* Other functions */
    case Token("TOK_FUNCTION", Token(ARRAY(), Nil) :: children) =>
      CreateArray(children.map(nodeToExpr))
    case Token("TOK_FUNCTION", Token(RAND(), Nil) :: Nil) => Rand
    case Token("TOK_FUNCTION", Token(SUBSTR(), Nil) :: string :: pos :: Nil) =>
      Substring(nodeToExpr(string), nodeToExpr(pos), Literal(Integer.MAX_VALUE, IntegerType))
    case Token("TOK_FUNCTION", Token(SUBSTR(), Nil) :: string :: pos :: length :: Nil) =>
      Substring(nodeToExpr(string), nodeToExpr(pos), nodeToExpr(length))
    case Token("TOK_FUNCTION", Token(COALESCE(), Nil) :: list) => Coalesce(list.map(nodeToExpr))

    /* UDFs - Must be last otherwise will preempt built in functions */
    case Token("TOK_FUNCTION", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, args.map(nodeToExpr))
    case Token("TOK_FUNCTIONSTAR", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, UnresolvedStar(None) :: Nil)

    /* Literals */
    case Token("TOK_NULL", Nil) => Literal(null, NullType)
    case Token(TRUE(), Nil) => Literal(true, BooleanType)
    case Token(FALSE(), Nil) => Literal(false, BooleanType)
    case Token("TOK_STRINGLITERALSEQUENCE", strings) =>
      Literal(strings.map(s => BaseSemanticAnalyzer.unescapeSQLString(s.getText)).mkString)

    // This code is adapted from
    // /ql/src/java/org/apache/hadoop/hive/ql/parse/TypeCheckProcFactory.java#L223
    case ast: ASTNode if numericAstTypes contains ast.getType =>
      var v: Literal = null
      try {
        if (ast.getText.endsWith("L")) {
          // Literal bigint.
          v = Literal(ast.getText.substring(0, ast.getText.length() - 1).toLong, LongType)
        } else if (ast.getText.endsWith("S")) {
          // Literal smallint.
          v = Literal(ast.getText.substring(0, ast.getText.length() - 1).toShort, ShortType)
        } else if (ast.getText.endsWith("Y")) {
          // Literal tinyint.
          v = Literal(ast.getText.substring(0, ast.getText.length() - 1).toByte, ByteType)
        } else if (ast.getText.endsWith("BD") || ast.getText.endsWith("D")) {
          // Literal decimal
          val strVal = ast.getText.stripSuffix("D").stripSuffix("B")
          v = Literal(Decimal(strVal))
        } else {
          v = Literal(ast.getText.toDouble, DoubleType)
          v = Literal(ast.getText.toLong, LongType)
          v = Literal(ast.getText.toInt, IntegerType)
        }
      } catch {
        case nfe: NumberFormatException => // Do nothing
      }

      if (v == null) {
        sys.error(s"Failed to parse number '${ast.getText}'.")
      } else {
        v
      }

    case ast: ASTNode if ast.getType == HiveParser.StringLiteral =>
      Literal(BaseSemanticAnalyzer.unescapeSQLString(ast.getText))

    case ast: ASTNode if ast.getType == HiveParser.TOK_DATELITERAL =>
      Literal(Date.valueOf(ast.getText.substring(1, ast.getText.length - 1)))

    case ast: ASTNode if ast.getType == HiveParser.TOK_CHARSETLITERAL =>
      Literal(BaseSemanticAnalyzer.charSetString(ast.getChild(0).getText, ast.getChild(1).getText))

    case a: ASTNode =>
      throw new NotImplementedError(
        s"""No parse rules for ASTNode type: ${a.getType}, text: ${a.getText} :
           |${dumpTree(a).toString}" +
         """.stripMargin)
  }


  val explode = "(?i)explode".r
  def nodesToGenerator(nodes: Seq[Node]): Generator = {
    val function = nodes.head

    val attributes = nodes.flatMap {
      case Token(a, Nil) => a.toLowerCase :: Nil
      case _ => Nil
    }

    function match {
      case Token("TOK_FUNCTION", Token(explode(), Nil) :: child :: Nil) =>
        Explode(attributes, nodeToExpr(child))

      case Token("TOK_FUNCTION", Token(functionName, Nil) :: children) =>
        HiveGenericUdtf(
          new HiveFunctionWrapper(functionName),
          attributes,
          children.map(nodeToExpr))

      case a: ASTNode =>
        throw new NotImplementedError(
          s"""No parse rules for ASTNode type: ${a.getType}, text: ${a.getText}, tree:
             |${dumpTree(a).toString}
           """.stripMargin)
    }
  }

  def dumpTree(node: Node, builder: StringBuilder = new StringBuilder, indent: Int = 0)
    : StringBuilder = {
    node match {
      case a: ASTNode => builder.append(
        ("  " * indent) + a.getText + " " +
          a.getLine + ", " +
          a.getTokenStartIndex + "," +
          a.getTokenStopIndex + ", " +
          a.getCharPositionInLine + "\n")
      case other => sys.error(s"Non ASTNode encountered: $other")
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(dumpTree(_, builder, indent + 1))
    builder
  }
}
