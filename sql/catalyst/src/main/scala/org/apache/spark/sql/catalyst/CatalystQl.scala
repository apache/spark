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
package org.apache.spark.sql.catalyst

import java.sql.Date

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.random.RandomSampler

/**
 * This class translates SQL to Catalyst [[LogicalPlan]]s or [[Expression]]s.
 */
private[sql] class CatalystQl(val conf: ParserConf = SimpleParserConf()) extends ParserInterface {
  object Token {
    def unapply(node: ASTNode): Some[(String, List[ASTNode])] = {
      CurrentOrigin.setPosition(node.line, node.positionInLine)
      node.pattern
    }
  }

  /**
   * The safeParse method allows a user to focus on the parsing/AST transformation logic. This
   * method will take care of possible errors during the parsing process.
   */
  protected def safeParse[T](sql: String, ast: ASTNode)(toResult: ASTNode => T): T = {
    try {
      toResult(ast)
    } catch {
      case e: MatchError => throw e
      case e: AnalysisException => throw e
      case e: Exception =>
        throw new AnalysisException(e.getMessage)
      case e: NotImplementedError =>
        throw new AnalysisException(
          s"""Unsupported language features in query
             |== SQL ==
             |$sql
             |== AST ==
             |${ast.treeString}
             |== Error ==
             |$e
             |== Stacktrace ==
             |${e.getStackTrace.head}
          """.stripMargin)
    }
  }

  /** Creates LogicalPlan for a given SQL string. */
  def parsePlan(sql: String): LogicalPlan =
    safeParse(sql, ParseDriver.parsePlan(sql, conf))(nodeToPlan)

  /** Creates Expression for a given SQL string. */
  def parseExpression(sql: String): Expression =
    safeParse(sql, ParseDriver.parseExpression(sql, conf))(selExprNodeToExpr(_).get)

  /** Creates TableIdentifier for a given SQL string. */
  def parseTableIdentifier(sql: String): TableIdentifier =
    safeParse(sql, ParseDriver.parseTableName(sql, conf))(extractTableIdent)

  def parseDdl(sql: String): Seq[Attribute] = {
    safeParse(sql, ParseDriver.parseExpression(sql, conf)) { ast =>
      val Token("TOK_CREATETABLE", children) = ast
      children
        .find(_.text == "TOK_TABCOLLIST")
        .getOrElse(sys.error("No columnList!"))
        .flatMap(_.children.map(nodeToAttribute))
    }
  }

  protected def getClauses(
      clauseNames: Seq[String],
      nodeList: Seq[ASTNode]): Seq[Option[ASTNode]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.text.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) matches.tail else Nil)
      matches.headOption
    }

    if (remainingNodes.nonEmpty) {
      sys.error(
        s"""Unhandled clauses: ${remainingNodes.map(_.treeString).mkString("\n")}.
            |You are likely trying to use an unsupported Hive feature."""".stripMargin)
    }
    clauses
  }

  protected def getClause(clauseName: String, nodeList: Seq[ASTNode]): ASTNode =
    getClauseOption(clauseName, nodeList).getOrElse(sys.error(
      s"Expected clause $clauseName missing from ${nodeList.map(_.treeString).mkString("\n")}"))

  protected def getClauseOption(clauseName: String, nodeList: Seq[ASTNode]): Option[ASTNode] = {
    nodeList.filter { case ast: ASTNode => ast.text == clauseName } match {
      case Seq(oneMatch) => Some(oneMatch)
      case Seq() => None
      case _ => sys.error(s"Found multiple instances of clause $clauseName")
    }
  }

  protected def nodeToAttribute(node: ASTNode): Attribute = node match {
    case Token("TOK_TABCOL", Token(colName, Nil) :: dataType :: Nil) =>
      AttributeReference(colName, nodeToDataType(dataType), nullable = true)()
    case _ =>
      noParseRule("Attribute", node)
  }

  protected def nodeToDataType(node: ASTNode): DataType = node match {
    case Token("TOK_DECIMAL", precision :: scale :: Nil) =>
      DecimalType(precision.text.toInt, scale.text.toInt)
    case Token("TOK_DECIMAL", precision :: Nil) =>
      DecimalType(precision.text.toInt, 0)
    case Token("TOK_DECIMAL", Nil) => DecimalType.USER_DEFAULT
    case Token("TOK_BIGINT", Nil) => LongType
    case Token("TOK_INT", Nil) => IntegerType
    case Token("TOK_TINYINT", Nil) => ByteType
    case Token("TOK_SMALLINT", Nil) => ShortType
    case Token("TOK_BOOLEAN", Nil) => BooleanType
    case Token("TOK_STRING", Nil) => StringType
    case Token("TOK_VARCHAR", Token(_, Nil) :: Nil) => StringType
    case Token("TOK_CHAR", Token(_, Nil) :: Nil) => StringType
    case Token("TOK_FLOAT", Nil) => FloatType
    case Token("TOK_DOUBLE", Nil) => DoubleType
    case Token("TOK_DATE", Nil) => DateType
    case Token("TOK_TIMESTAMP", Nil) => TimestampType
    case Token("TOK_BINARY", Nil) => BinaryType
    case Token("TOK_LIST", elementType :: Nil) => ArrayType(nodeToDataType(elementType))
    case Token("TOK_STRUCT", Token("TOK_TABCOLLIST", fields) :: Nil) =>
      StructType(fields.map(nodeToStructField))
    case Token("TOK_MAP", keyType :: valueType :: Nil) =>
      MapType(nodeToDataType(keyType), nodeToDataType(valueType))
    case _ =>
      noParseRule("DataType", node)
  }

  protected def nodeToStructField(node: ASTNode): StructField = node match {
    case Token("TOK_TABCOL", Token(fieldName, Nil) :: dataType :: Nil) =>
      StructField(cleanIdentifier(fieldName), nodeToDataType(dataType), nullable = true)
    case Token("TOK_TABCOL", Token(fieldName, Nil) :: dataType :: comment :: Nil) =>
      val meta = new MetadataBuilder().putString("comment", unquoteString(comment.text)).build()
      StructField(cleanIdentifier(fieldName), nodeToDataType(dataType), nullable = true, meta)
    case _ =>
      noParseRule("StructField", node)
  }

  protected def extractTableIdent(tableNameParts: ASTNode): TableIdentifier = {
    tableNameParts.children.map {
      case Token(part, Nil) => cleanIdentifier(part)
    } match {
      case Seq(tableOnly) => TableIdentifier(tableOnly)
      case Seq(databaseName, table) => TableIdentifier(table, Some(databaseName))
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
   * e.g. In superset (k1, k2, k3), (bit 2: k1, bit 1: k2, and bit 0: k3), the grouping id of
   * GROUPING SETS (k1, k2) and (k2) should be 1 and 5 respectively.
   */
  protected def extractGroupingSet(children: Seq[ASTNode]): (Seq[Expression], Seq[Int]) = {
    val (keyASTs, setASTs) = children.partition {
      case Token("TOK_GROUPING_SETS_EXPRESSION", _) => false // grouping sets
      case _ => true // grouping keys
    }

    val keys = keyASTs.map(nodeToExpr)
    val keyMap = keyASTs.zipWithIndex.toMap

    val mask = (1 << keys.length) - 1
    val bitmasks: Seq[Int] = setASTs.map {
      case Token("TOK_GROUPING_SETS_EXPRESSION", columns) =>
        columns.foldLeft(mask)((bitmap, col) => {
          val keyIndex = keyMap.find(_._1.treeEquals(col)).map(_._2).getOrElse(
            throw new AnalysisException(s"${col.treeString} doesn't show up in the GROUP BY list"))
          // 0 means that the column at the given index is a grouping column, 1 means it is not,
          // so we unset the bit in bitmap.
          bitmap & ~(1 << (keys.length - 1 - keyIndex))
        })
      case _ => sys.error("Expect GROUPING SETS clause")
    }

    (keys, bitmasks)
  }

  protected def nodeToPlan(node: ASTNode): LogicalPlan = node match {
    case Token("TOK_SHOWFUNCTIONS", args) =>
      // Skip LIKE.
      val pattern = args match {
        case like :: nodes if like.text.toUpperCase == "LIKE" => nodes
        case nodes => nodes
      }

      // Extract Database and Function name
      pattern match {
        case Nil =>
          ShowFunctions(None, None)
        case Token(name, Nil) :: Nil =>
          ShowFunctions(None, Some(unquoteString(cleanIdentifier(name))))
        case Token(db, Nil) :: Token(name, Nil) :: Nil =>
          ShowFunctions(Some(unquoteString(cleanIdentifier(db))),
            Some(unquoteString(cleanIdentifier(name))))
        case _ =>
          noParseRule("SHOW FUNCTIONS", node)
      }

    case Token("TOK_DESCFUNCTION", Token(functionName, Nil) :: isExtended) =>
      DescribeFunction(cleanIdentifier(functionName), isExtended.nonEmpty)

    case Token("TOK_QUERY", queryArgs @ Token("TOK_CTE" | "TOK_FROM" | "TOK_INSERT", _) :: _) =>
      val (fromClause: Option[ASTNode], insertClauses, cteRelations) =
        queryArgs match {
          case Token("TOK_CTE", ctes) :: Token("TOK_FROM", from) :: inserts =>
            val cteRelations = ctes.map { node =>
              val relation = nodeToRelation(node).asInstanceOf[Subquery]
              relation.alias -> relation
            }
            (Some(from.head), inserts, Some(cteRelations.toMap))
          case Token("TOK_FROM", from) :: inserts =>
            (Some(from.head), inserts, None)
          case Token("TOK_INSERT", _) :: Nil =>
            (None, queryArgs, None)
        }

      // Return one query for each insert clause.
      val queries = insertClauses.map {
        case Token("TOK_INSERT", singleInsert) =>
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
              lateralViewClause ::
              windowClause :: Nil) = {
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
                "TOK_LATERAL_VIEW",
                "WINDOW"),
              singleInsert)
          }

          val relations = fromClause match {
            case Some(f) => nodeToRelation(f)
            case None => OneRowRelation
          }

          val withWhere = whereClause.map { whereNode =>
            val Seq(whereExpr) = whereNode.children
            Filter(nodeToExpr(whereExpr), relations)
          }.getOrElse(relations)

          val select = (selectClause orElse selectDistinctClause)
            .getOrElse(sys.error("No select clause."))

          val transformation = nodeToTransformation(select.children.head, withWhere)

          val withLateralView = lateralViewClause.map { lv =>
            nodeToGenerate(lv.children.head, outer = false, withWhere)
          }.getOrElse(withWhere)

          // The projection of the query can either be a normal projection, an aggregation
          // (if there is a group by) or a script transformation.
          val withProject: LogicalPlan = transformation.getOrElse {
            val selectExpressions =
              select.children.flatMap(selExprNodeToExpr).map(UnresolvedAlias(_))
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
                  Aggregate(
                    Seq(Rollup(children.map(nodeToExpr))),
                    selectExpressions,
                    withLateralView)
                case _ => sys.error("Expect WITH ROLLUP")
              }),
              cubeGroupByClause.map(e => e match {
                case Token("TOK_CUBE_GROUPBY", children) =>
                  Aggregate(
                    Seq(Cube(children.map(nodeToExpr))),
                    selectExpressions,
                    withLateralView)
                case _ => sys.error("Expect WITH CUBE")
              }),
              Some(Project(selectExpressions, withLateralView))).flatten.head
          }

          // Handle HAVING clause.
          val withHaving = havingClause.map { h =>
            val havingExpr = h.children match { case Seq(hexpr) => nodeToExpr(hexpr) }
            // Note that we added a cast to boolean. If the expression itself is already boolean,
            // the optimizer will get rid of the unnecessary cast.
            Filter(Cast(havingExpr, BooleanType), withProject)
          }.getOrElse(withProject)

          // Handle SELECT DISTINCT
          val withDistinct =
            if (selectDistinctClause.isDefined) Distinct(withHaving) else withHaving

          // Handle ORDER BY, SORT BY, DISTRIBUTE BY, and CLUSTER BY clause.
          val withSort =
            (orderByClause, sortByClause, distributeByClause, clusterByClause) match {
              case (Some(totalOrdering), None, None, None) =>
                Sort(totalOrdering.children.map(nodeToSortOrder), global = true, withDistinct)
              case (None, Some(perPartitionOrdering), None, None) =>
                Sort(
                  perPartitionOrdering.children.map(nodeToSortOrder),
                  global = false, withDistinct)
              case (None, None, Some(partitionExprs), None) =>
                RepartitionByExpression(
                  partitionExprs.children.map(nodeToExpr), withDistinct)
              case (None, Some(perPartitionOrdering), Some(partitionExprs), None) =>
                Sort(
                  perPartitionOrdering.children.map(nodeToSortOrder), global = false,
                  RepartitionByExpression(
                    partitionExprs.children.map(nodeToExpr),
                    withDistinct))
              case (None, None, None, Some(clusterExprs)) =>
                Sort(
                  clusterExprs.children.map(nodeToExpr).map(SortOrder(_, Ascending)),
                  global = false,
                  RepartitionByExpression(
                    clusterExprs.children.map(nodeToExpr),
                    withDistinct))
              case (None, None, None, None) => withDistinct
              case _ => sys.error("Unsupported set of ordering / distribution clauses.")
            }

          val withLimit =
            limitClause.map(l => nodeToExpr(l.children.head))
              .map(Limit(_, withSort))
              .getOrElse(withSort)

          // Collect all window specifications defined in the WINDOW clause.
          val windowDefinitions = windowClause.map(_.children.collect {
            case Token("TOK_WINDOWDEF",
            Token(windowName, Nil) :: Token("TOK_WINDOWSPEC", spec) :: Nil) =>
              windowName -> nodesToWindowSpecification(spec)
          }.toMap)
          // Handle cases like
          // window w1 as (partition by p_mfgr order by p_name
          //               range between 2 preceding and 2 following),
          //        w2 as w1
          val resolvedCrossReference = windowDefinitions.map {
            windowDefMap => windowDefMap.map {
              case (windowName, WindowSpecReference(other)) =>
                (windowName, windowDefMap(other).asInstanceOf[WindowSpecDefinition])
              case o => o.asInstanceOf[(String, WindowSpecDefinition)]
            }
          }

          val withWindowDefinitions =
            resolvedCrossReference.map(WithWindowDefinition(_, withLimit)).getOrElse(withLimit)

          // TOK_INSERT_INTO means to add files to the table.
          // TOK_DESTINATION means to overwrite the table.
          val resultDestination =
            (intoClause orElse destClause).getOrElse(sys.error("No destination found."))
          val overwrite = intoClause.isEmpty
          nodeToDest(
            resultDestination,
            withWindowDefinitions,
            overwrite)
      }

      // If there are multiple INSERTS just UNION them together into one query.
      val query = if (queries.length == 1) queries.head else Union(queries)

      // return With plan if there is CTE
      cteRelations.map(With(query, _)).getOrElse(query)

    case Token("TOK_UNIONALL", left :: right :: Nil) =>
      Union(nodeToPlan(left), nodeToPlan(right))
    case Token("TOK_UNIONDISTINCT", left :: right :: Nil) =>
      Distinct(Union(nodeToPlan(left), nodeToPlan(right)))
    case Token("TOK_EXCEPT", left :: right :: Nil) =>
      Except(nodeToPlan(left), nodeToPlan(right))
    case Token("TOK_INTERSECT", left :: right :: Nil) =>
      Intersect(nodeToPlan(left), nodeToPlan(right))

    case _ =>
      noParseRule("Plan", node)
  }

  val allJoinTokens = "(TOK_.*JOIN)".r
  val laterViewToken = "TOK_LATERAL_VIEW(.*)".r
  protected def nodeToRelation(node: ASTNode): LogicalPlan = {
    node match {
      case Token("TOK_SUBQUERY", query :: Token(alias, Nil) :: Nil) =>
        Subquery(cleanIdentifier(alias), nodeToPlan(query))

      case Token(laterViewToken(isOuter), selectClause :: relationClause :: Nil) =>
        nodeToGenerate(
          selectClause,
          outer = isOuter.nonEmpty,
          nodeToRelation(relationClause))

      /* All relations, possibly with aliases or sampling clauses. */
      case Token("TOK_TABREF", clauses) =>
        // If the last clause is not a token then it's the alias of the table.
        val (nonAliasClauses, aliasClause) =
          if (clauses.last.text.startsWith("TOK")) {
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

        val tableIdent = extractTableIdent(tableNameParts)
        val alias = aliasClause.map { case Token(a, Nil) => cleanIdentifier(a) }
        val relation = UnresolvedRelation(tableIdent, alias)

        // Apply sampling if requested.
        (bucketSampleClause orElse splitSampleClause).map {
          case Token("TOK_TABLESPLITSAMPLE",
          Token("TOK_ROWCOUNT", Nil) :: Token(count, Nil) :: Nil) =>
            Limit(Literal(count.toInt), relation)
          case Token("TOK_TABLESPLITSAMPLE",
          Token("TOK_PERCENT", Nil) :: Token(fraction, Nil) :: Nil) =>
            // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
            // function takes X PERCENT as the input and the range of X is [0, 100], we need to
            // adjust the fraction.
            require(
              fraction.toDouble >= (0.0 - RandomSampler.roundingEpsilon)
                && fraction.toDouble <= (100.0 + RandomSampler.roundingEpsilon),
              s"Sampling fraction ($fraction) must be on interval [0, 100]")
            Sample(0.0, fraction.toDouble / 100, withReplacement = false,
              (math.random * 1000).toInt,
              relation)
          case Token("TOK_TABLEBUCKETSAMPLE",
          Token(numerator, Nil) ::
            Token(denominator, Nil) :: Nil) =>
            val fraction = numerator.toDouble / denominator.toDouble
            Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, relation)
          case a =>
            noParseRule("Sampling", a)
        }.getOrElse(relation)

      case Token(allJoinTokens(joinToken), relation1 :: relation2 :: other) =>
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
          case "TOK_UNIQUEJOIN" => noParseRule("Unique Join", node)
          case "TOK_ANTIJOIN" => noParseRule("Anti Join", node)
          case "TOK_NATURALJOIN" => NaturalJoin(Inner)
          case "TOK_NATURALRIGHTOUTERJOIN" => NaturalJoin(RightOuter)
          case "TOK_NATURALLEFTOUTERJOIN" => NaturalJoin(LeftOuter)
          case "TOK_NATURALFULLOUTERJOIN" => NaturalJoin(FullOuter)
        }
        Join(nodeToRelation(relation1),
          nodeToRelation(relation2),
          joinType,
          other.headOption.map(nodeToExpr))

      case _ =>
        noParseRule("Relation", node)
    }
  }

  protected def nodeToSortOrder(node: ASTNode): SortOrder = node match {
    case Token("TOK_TABSORTCOLNAMEASC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Ascending)
    case Token("TOK_TABSORTCOLNAMEDESC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Descending)
    case _ =>
      noParseRule("SortOrder", node)
  }

  val destinationToken = "TOK_DESTINATION|TOK_INSERT_INTO".r
  protected def nodeToDest(
      node: ASTNode,
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

      val partitionKeys = partitionClause.map(_.children.map {
        // Parse partitions. We also make keys case insensitive.
        case Token("TOK_PARTVAL", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> Some(unquoteString(value))
        case Token("TOK_PARTVAL", Token(key, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> None
      }.toMap).getOrElse(Map.empty)

      InsertIntoTable(
        UnresolvedRelation(tableIdent, None), partitionKeys, query, overwrite, ifNotExists = false)

    case Token(destinationToken(),
    Token("TOK_TAB",
    tableArgs) ::
      Token("TOK_IFNOTEXISTS",
      ifNotExists) :: Nil) =>
      val Some(tableNameParts) :: partitionClause :: Nil =
        getClauses(Seq("TOK_TABNAME", "TOK_PARTSPEC"), tableArgs)

      val tableIdent = extractTableIdent(tableNameParts)

      val partitionKeys = partitionClause.map(_.children.map {
        // Parse partitions. We also make keys case insensitive.
        case Token("TOK_PARTVAL", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> Some(unquoteString(value))
        case Token("TOK_PARTVAL", Token(key, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> None
      }.toMap).getOrElse(Map.empty)

      InsertIntoTable(
        UnresolvedRelation(tableIdent, None), partitionKeys, query, overwrite, ifNotExists = true)

    case _ =>
      noParseRule("Destination", node)
  }

  protected def selExprNodeToExpr(node: ASTNode): Option[Expression] = node match {
    case Token("TOK_SELEXPR", e :: Nil) =>
      Some(nodeToExpr(e))

    case Token("TOK_SELEXPR", e :: Token(alias, Nil) :: Nil) =>
      Some(Alias(nodeToExpr(e), cleanIdentifier(alias))())

    case Token("TOK_SELEXPR", e :: aliasChildren) =>
      val aliasNames = aliasChildren.collect {
        case Token(name, Nil) => cleanIdentifier(name)
      }
      Some(MultiAlias(nodeToExpr(e), aliasNames))

    /* Hints are ignored */
    case Token("TOK_HINTLIST", _) => None

    case _ =>
      noParseRule("Select", node)
  }

  protected val escapedIdentifier = "`(.+)`".r
  protected val doubleQuotedString = "\"([^\"]+)\"".r
  protected val singleQuotedString = "'([^']+)'".r

  protected def unquoteString(str: String) = str match {
    case singleQuotedString(s) => s
    case doubleQuotedString(s) => s
    case other => other
  }

  /** Strips backticks from ident if present */
  protected def cleanIdentifier(ident: String): String = ident match {
    case escapedIdentifier(i) => i
    case plainIdent => plainIdent
  }

  /* Case insensitive matches */
  val COUNT = "(?i)COUNT".r
  val SUM = "(?i)SUM".r
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

  val INTEGRAL = "[+-]?\\d+".r
  val DECIMAL = "[+-]?((\\d+(\\.\\d*)?)|(\\.\\d+))".r

  protected def nodeToExpr(node: ASTNode): Expression = node match {
    /* Attribute References */
    case Token("TOK_TABLE_OR_COL", Token(name, Nil) :: Nil) =>
      UnresolvedAttribute.quoted(cleanIdentifier(name))
    case Token(".", qualifier :: Token(attr, Nil) :: Nil) =>
      nodeToExpr(qualifier) match {
        case UnresolvedAttribute(nameParts) =>
          UnresolvedAttribute(nameParts :+ cleanIdentifier(attr))
        case other => UnresolvedExtractValue(other, Literal(cleanIdentifier(attr)))
      }

    /* Stars (*) */
    case Token("TOK_ALLCOLREF", Nil) => UnresolvedStar(None)
    // The format of dbName.tableName.* cannot be parsed by HiveParser. TOK_TABNAME will only
    // has a single child which is tableName.
    case Token("TOK_ALLCOLREF", Token("TOK_TABNAME", target) :: Nil) if target.nonEmpty =>
      UnresolvedStar(Some(target.map(x => cleanIdentifier(x.text))))

    /* Aggregate Functions */
    case Token("TOK_FUNCTIONDI", Token(COUNT(), Nil) :: args) =>
      Count(args.map(nodeToExpr)).toAggregateExpression(isDistinct = true)
    case Token("TOK_FUNCTIONSTAR", Token(COUNT(), Nil) :: Nil) =>
      Count(Literal(1)).toAggregateExpression()

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
      Cast(nodeToExpr(arg), DecimalType(precision.text.toInt, scale.text.toInt))
    case Token("TOK_FUNCTION", Token("TOK_DECIMAL", precision :: Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DecimalType(precision.text.toInt, 0))
    case Token("TOK_FUNCTION", Token("TOK_DECIMAL", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DecimalType.USER_DEFAULT)
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
      CaseWhen.createFromParser(branches.map(nodeToExpr))
    case Token("TOK_FUNCTION", Token(CASE(), Nil) :: branches) =>
      val keyExpr = nodeToExpr(branches.head)
      CaseKeyWhen(keyExpr, branches.drop(1).map(nodeToExpr))

    /* Complex datatype manipulation */
    case Token("[", child :: ordinal :: Nil) =>
      UnresolvedExtractValue(nodeToExpr(child), nodeToExpr(ordinal))

    /* Window Functions */
    case Token(text, args :+ Token("TOK_WINDOWSPEC", spec)) =>
      val function = nodeToExpr(node.copy(children = node.children.init))
      nodesToWindowSpecification(spec) match {
        case reference: WindowSpecReference =>
          UnresolvedWindowExpression(function, reference)
        case definition: WindowSpecDefinition =>
          WindowExpression(function, definition)
      }

    /* UDFs - Must be last otherwise will preempt built in functions */
    case Token("TOK_FUNCTION", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, args.map(nodeToExpr), isDistinct = false)
    // Aggregate function with DISTINCT keyword.
    case Token("TOK_FUNCTIONDI", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, args.map(nodeToExpr), isDistinct = true)
    case Token("TOK_FUNCTIONSTAR", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, UnresolvedStar(None) :: Nil, isDistinct = false)

    /* Literals */
    case Token("TOK_NULL", Nil) => Literal.create(null, NullType)
    case Token(TRUE(), Nil) => Literal.create(true, BooleanType)
    case Token(FALSE(), Nil) => Literal.create(false, BooleanType)
    case Token("TOK_STRINGLITERALSEQUENCE", strings) =>
      Literal(strings.map(s => ParseUtils.unescapeSQLString(s.text)).mkString)

    case ast if ast.tokenType == SparkSqlParser.TinyintLiteral =>
      Literal.create(ast.text.substring(0, ast.text.length() - 1).toByte, ByteType)

    case ast if ast.tokenType == SparkSqlParser.SmallintLiteral =>
      Literal.create(ast.text.substring(0, ast.text.length() - 1).toShort, ShortType)

    case ast if ast.tokenType == SparkSqlParser.BigintLiteral =>
      Literal.create(ast.text.substring(0, ast.text.length() - 1).toLong, LongType)

    case ast if ast.tokenType == SparkSqlParser.DoubleLiteral =>
      Literal(ast.text.toDouble)

    case ast if ast.tokenType == SparkSqlParser.Number =>
      val text = ast.text
      text match {
        case INTEGRAL() =>
          BigDecimal(text) match {
            case v if v.isValidInt =>
              Literal(v.intValue())
            case v if v.isValidLong =>
              Literal(v.longValue())
            case v => Literal(v.underlying())
          }
        case DECIMAL(_*) =>
          Literal(BigDecimal(text).underlying())
        case _ =>
          // Convert a scientifically notated decimal into a double.
          Literal(text.toDouble)
      }
    case ast if ast.tokenType == SparkSqlParser.StringLiteral =>
      Literal(ParseUtils.unescapeSQLString(ast.text))

    case ast if ast.tokenType == SparkSqlParser.TOK_DATELITERAL =>
      Literal(Date.valueOf(ast.text.substring(1, ast.text.length - 1)))

    case ast if ast.tokenType == SparkSqlParser.TOK_INTERVAL_YEAR_MONTH_LITERAL =>
      Literal(CalendarInterval.fromYearMonthString(ast.children.head.text))

    case ast if ast.tokenType == SparkSqlParser.TOK_INTERVAL_DAY_TIME_LITERAL =>
      Literal(CalendarInterval.fromDayTimeString(ast.children.head.text))

    case Token("TOK_INTERVAL", elements) =>
      var interval = new CalendarInterval(0, 0)
      var updated = false
      elements.foreach {
        // The interval node will always contain children for all possible time units. A child node
        // is only useful when it contains exactly one (numeric) child.
        case e @ Token(name, Token(value, Nil) :: Nil) =>
          val unit = name match {
            case "TOK_INTERVAL_YEAR_LITERAL" => "year"
            case "TOK_INTERVAL_MONTH_LITERAL" => "month"
            case "TOK_INTERVAL_WEEK_LITERAL" => "week"
            case "TOK_INTERVAL_DAY_LITERAL" => "day"
            case "TOK_INTERVAL_HOUR_LITERAL" => "hour"
            case "TOK_INTERVAL_MINUTE_LITERAL" => "minute"
            case "TOK_INTERVAL_SECOND_LITERAL" => "second"
            case "TOK_INTERVAL_MILLISECOND_LITERAL" => "millisecond"
            case "TOK_INTERVAL_MICROSECOND_LITERAL" => "microsecond"
            case _ => noParseRule(s"Interval($name)", e)
          }
          interval = interval.add(CalendarInterval.fromSingleUnitString(unit, value))
          updated = true
        case _ =>
      }
      if (!updated) {
        throw new AnalysisException("at least one time unit should be given for interval literal")
      }
      Literal(interval)

    case _ =>
      noParseRule("Expression", node)
  }

  /* Case insensitive matches for Window Specification */
  val PRECEDING = "(?i)preceding".r
  val FOLLOWING = "(?i)following".r
  val CURRENT = "(?i)current".r
  protected def nodesToWindowSpecification(nodes: Seq[ASTNode]): WindowSpec = nodes match {
    case Token(windowName, Nil) :: Nil =>
      // Refer to a window spec defined in the window clause.
      WindowSpecReference(windowName)
    case Nil =>
      // OVER()
      WindowSpecDefinition(
        partitionSpec = Nil,
        orderSpec = Nil,
        frameSpecification = UnspecifiedFrame)
    case spec =>
      val (partitionClause :: rowFrame :: rangeFrame :: Nil) =
        getClauses(
          Seq(
            "TOK_PARTITIONINGSPEC",
            "TOK_WINDOWRANGE",
            "TOK_WINDOWVALUES"),
          spec)

      // Handle Partition By and Order By.
      val (partitionSpec, orderSpec) = partitionClause.map { partitionAndOrdering =>
        val (partitionByClause :: orderByClause :: sortByClause :: clusterByClause :: Nil) =
          getClauses(
            Seq("TOK_DISTRIBUTEBY", "TOK_ORDERBY", "TOK_SORTBY", "TOK_CLUSTERBY"),
            partitionAndOrdering.children)

        (partitionByClause, orderByClause.orElse(sortByClause), clusterByClause) match {
          case (Some(partitionByExpr), Some(orderByExpr), None) =>
            (partitionByExpr.children.map(nodeToExpr),
              orderByExpr.children.map(nodeToSortOrder))
          case (Some(partitionByExpr), None, None) =>
            (partitionByExpr.children.map(nodeToExpr), Nil)
          case (None, Some(orderByExpr), None) =>
            (Nil, orderByExpr.children.map(nodeToSortOrder))
          case (None, None, Some(clusterByExpr)) =>
            val expressions = clusterByExpr.children.map(nodeToExpr)
            (expressions, expressions.map(SortOrder(_, Ascending)))
          case _ =>
            noParseRule("Partition & Ordering", partitionAndOrdering)
        }
      }.getOrElse {
        (Nil, Nil)
      }

      // Handle Window Frame
      val windowFrame =
        if (rowFrame.isEmpty && rangeFrame.isEmpty) {
          UnspecifiedFrame
        } else {
          val frameType = rowFrame.map(_ => RowFrame).getOrElse(RangeFrame)
          def nodeToBoundary(node: ASTNode): FrameBoundary = node match {
            case Token(PRECEDING(), Token(count, Nil) :: Nil) =>
              if (count.toLowerCase() == "unbounded") {
                UnboundedPreceding
              } else {
                ValuePreceding(count.toInt)
              }
            case Token(FOLLOWING(), Token(count, Nil) :: Nil) =>
              if (count.toLowerCase() == "unbounded") {
                UnboundedFollowing
              } else {
                ValueFollowing(count.toInt)
              }
            case Token(CURRENT(), Nil) => CurrentRow
            case _ =>
              noParseRule("Window Frame Boundary", node)
          }

          rowFrame.orElse(rangeFrame).map { frame =>
            frame.children match {
              case precedingNode :: followingNode :: Nil =>
                SpecifiedWindowFrame(
                  frameType,
                  nodeToBoundary(precedingNode),
                  nodeToBoundary(followingNode))
              case precedingNode :: Nil =>
                SpecifiedWindowFrame(frameType, nodeToBoundary(precedingNode), CurrentRow)
              case _ =>
                noParseRule("Window Frame", frame)
            }
          }.getOrElse(sys.error(s"If you see this, please file a bug report with your query."))
        }

      WindowSpecDefinition(partitionSpec, orderSpec, windowFrame)
  }

  protected def nodeToTransformation(
      node: ASTNode,
      child: LogicalPlan): Option[ScriptTransformation] = None

  val explode = "(?i)explode".r
  val jsonTuple = "(?i)json_tuple".r
  protected def nodeToGenerate(node: ASTNode, outer: Boolean, child: LogicalPlan): Generate = {
    val Token("TOK_SELECT", Token("TOK_SELEXPR", clauses) :: Nil) = node

    val alias = cleanIdentifier(getClause("TOK_TABALIAS", clauses).children.head.text)

    val generator = clauses.head match {
      case Token("TOK_FUNCTION", Token(explode(), Nil) :: childNode :: Nil) =>
        Explode(nodeToExpr(childNode))
      case Token("TOK_FUNCTION", Token(jsonTuple(), Nil) :: children) =>
        JsonTuple(children.map(nodeToExpr))
      case other =>
        nodeToGenerator(other)
    }

    val attributes = clauses.collect {
      case Token(a, Nil) => UnresolvedAttribute(a.toLowerCase)
    }

    Generate(generator, join = true, outer = outer, Some(alias.toLowerCase), attributes, child)
  }

  protected def nodeToGenerator(node: ASTNode): Generator = noParseRule("Generator", node)

  protected def noParseRule(msg: String, node: ASTNode): Nothing = throw new NotImplementedError(
    s"[$msg]: No parse rules for ASTNode type: ${node.tokenType}, tree:\n${node.treeString}")
}
