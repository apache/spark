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

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.{FunctionInfo, FunctionRegistry}
import org.apache.hadoop.hive.ql.parse.EximUtil
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.ParseUtils._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkQl
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.types._
import org.apache.spark.sql.AnalysisException

/**
 * Used when we need to start parsing the AST before deciding that we are going to pass the command
 * back for Hive to execute natively.  Will be replaced with a native command that contains the
 * cmd string.
 */
private[hive] case object NativePlaceholder extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty
  override def output: Seq[Attribute] = Seq.empty
}

private[hive] case class CreateTableAsSelect(
    tableDesc: HiveTable,
    child: LogicalPlan,
    allowExisting: Boolean) extends UnaryNode with Command {

  override def output: Seq[Attribute] = Seq.empty[Attribute]
  override lazy val resolved: Boolean =
    tableDesc.specifiedDatabase.isDefined &&
    tableDesc.schema.nonEmpty &&
    tableDesc.serde.isDefined &&
    tableDesc.inputFormat.isDefined &&
    tableDesc.outputFormat.isDefined &&
    childrenResolved
}

private[hive] case class CreateViewAsSelect(
    tableDesc: HiveTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    sql: String) extends UnaryNode with Command {
  override def output: Seq[Attribute] = Seq.empty[Attribute]
  override lazy val resolved: Boolean = false
}

/** Provides a mapping from HiveQL statements to catalyst logical plans and expression trees. */
private[hive] class HiveQl(conf: ParserConf) extends SparkQl(conf) with Logging {
  protected val nativeCommands = Seq(
    "TOK_ALTERDATABASE_OWNER",
    "TOK_ALTERDATABASE_PROPERTIES",
    "TOK_ALTERINDEX_PROPERTIES",
    "TOK_ALTERINDEX_REBUILD",
    "TOK_ALTERTABLE",
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
    "TOK_ALTERVIEW_ADDPARTS",
    "TOK_ALTERVIEW_AS",
    "TOK_ALTERVIEW_DROPPARTS",
    "TOK_ALTERVIEW_PROPERTIES",
    "TOK_ALTERVIEW_RENAME",

    "TOK_CREATEDATABASE",
    "TOK_CREATEFUNCTION",
    "TOK_CREATEINDEX",
    "TOK_CREATEMACRO",
    "TOK_CREATEROLE",

    "TOK_DESCDATABASE",

    "TOK_DROPDATABASE",
    "TOK_DROPFUNCTION",
    "TOK_DROPINDEX",
    "TOK_DROPMACRO",
    "TOK_DROPROLE",
    "TOK_DROPTABLE_PROPERTIES",
    "TOK_DROPVIEW",
    "TOK_DROPVIEW_PROPERTIES",

    "TOK_EXPORT",

    "TOK_GRANT",
    "TOK_GRANT_ROLE",

    "TOK_IMPORT",

    "TOK_LOAD",

    "TOK_LOCKTABLE",

    "TOK_MSCK",

    "TOK_REVOKE",

    "TOK_SHOW_COMPACTIONS",
    "TOK_SHOW_CREATETABLE",
    "TOK_SHOW_GRANT",
    "TOK_SHOW_ROLE_GRANT",
    "TOK_SHOW_ROLE_PRINCIPALS",
    "TOK_SHOW_ROLES",
    "TOK_SHOW_SET_ROLE",
    "TOK_SHOW_TABLESTATUS",
    "TOK_SHOW_TBLPROPERTIES",
    "TOK_SHOW_TRANSACTIONS",
    "TOK_SHOWCOLUMNS",
    "TOK_SHOWDATABASES",
    "TOK_SHOWINDEXES",
    "TOK_SHOWLOCKS",
    "TOK_SHOWPARTITIONS",

    "TOK_UNLOCKTABLE"
  )

  // Commands that we do not need to explain.
  protected val noExplainCommands = Seq(
    "TOK_DESCTABLE",
    "TOK_SHOWTABLES",
    "TOK_TRUNCATETABLE"     // truncate table" is a NativeCommand, does not need to explain.
  ) ++ nativeCommands

  /**
   * Returns the HiveConf
   */
  private[this] def hiveConf: HiveConf = {
    var ss = SessionState.get()
    // SessionState is lazy initialization, it can be null here
    if (ss == null) {
      val original = Thread.currentThread().getContextClassLoader
      val conf = new HiveConf(classOf[SessionState])
      conf.setClassLoader(original)
      ss = new SessionState(conf)
      SessionState.start(ss)
    }
    ss.getConf
  }

  protected def getProperties(node: ASTNode): Seq[(String, String)] = node match {
    case Token("TOK_TABLEPROPLIST", list) =>
      list.map {
        case Token("TOK_TABLEPROPERTY", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
          unquoteString(key) -> unquoteString(value)
      }
  }

  private def createView(
      view: ASTNode,
      viewNameParts: ASTNode,
      query: ASTNode,
      schema: Seq[HiveColumn],
      properties: Map[String, String],
      allowExist: Boolean,
      replace: Boolean): CreateViewAsSelect = {
    val TableIdentifier(viewName, dbName) = extractTableIdent(viewNameParts)

    val originalText = query.source

    val tableDesc = HiveTable(
      specifiedDatabase = dbName,
      name = viewName,
      schema = schema,
      partitionColumns = Seq.empty[HiveColumn],
      properties = properties,
      serdeProperties = Map[String, String](),
      tableType = VirtualView,
      location = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      viewText = Some(originalText))

    // We need to keep the original SQL string so that if `spark.sql.nativeView` is
    // false, we can fall back to use hive native command later.
    // We can remove this when parser is configurable(can access SQLConf) in the future.
    val sql = view.source
    CreateViewAsSelect(tableDesc, nodeToPlan(query), allowExist, replace, sql)
  }

  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sql: String): LogicalPlan = {
    safeParse(sql, ParseDriver.parsePlan(sql, conf)) { ast =>
      if (nativeCommands.contains(ast.text)) {
        HiveNativeCommand(sql)
      } else {
        nodeToPlan(ast) match {
          case NativePlaceholder => HiveNativeCommand(sql)
          case plan => plan
        }
      }
    }
  }

  protected override def isNoExplainCommand(command: String): Boolean =
    noExplainCommands.contains(command)

  protected override def nodeToPlan(node: ASTNode): LogicalPlan = {
    node match {
      case Token("TOK_DFS", Nil) =>
        HiveNativeCommand(node.source + " " + node.remainder)

      case Token("TOK_ADDFILE", Nil) =>
        AddFile(node.remainder)

      case Token("TOK_ADDJAR", Nil) =>
        AddJar(node.remainder)

      // Special drop table that also uncaches.
      case Token("TOK_DROPTABLE", Token("TOK_TABNAME", tableNameParts) :: ifExists) =>
        val tableName = tableNameParts.map { case Token(p, Nil) => p }.mkString(".")
        DropTable(tableName, ifExists.nonEmpty)

      // Support "ANALYZE TABLE tableNmae COMPUTE STATISTICS noscan"
      case Token("TOK_ANALYZE",
        Token("TOK_TAB", Token("TOK_TABNAME", tableNameParts) :: partitionSpec) :: isNoscan) =>
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

      case view @ Token("TOK_ALTERVIEW", children) =>
        val Some(nameParts) :: maybeQuery :: _ =
          getClauses(Seq(
            "TOK_TABNAME",
            "TOK_QUERY",
            "TOK_ALTERVIEW_ADDPARTS",
            "TOK_ALTERVIEW_DROPPARTS",
            "TOK_ALTERVIEW_PROPERTIES",
            "TOK_ALTERVIEW_RENAME"), children)

        // if ALTER VIEW doesn't have query part, let hive to handle it.
        maybeQuery.map { query =>
          createView(view, nameParts, query, Nil, Map(), allowExist = false, replace = true)
        }.getOrElse(NativePlaceholder)

      case view @ Token("TOK_CREATEVIEW", children)
        if children.collect { case t @ Token("TOK_QUERY", _) => t }.nonEmpty =>
        val Seq(
        Some(viewNameParts),
        Some(query),
        maybeComment,
        replace,
        allowExisting,
        maybeProperties,
        maybeColumns,
        maybePartCols
        ) = getClauses(Seq(
          "TOK_TABNAME",
          "TOK_QUERY",
          "TOK_TABLECOMMENT",
          "TOK_ORREPLACE",
          "TOK_IFNOTEXISTS",
          "TOK_TABLEPROPERTIES",
          "TOK_TABCOLNAME",
          "TOK_VIEWPARTCOLS"), children)

        // If the view is partitioned, we let hive handle it.
        if (maybePartCols.isDefined) {
          NativePlaceholder
        } else {
          val schema = maybeColumns.map { cols =>
            // We can't specify column types when create view, so fill it with null first, and
            // update it after the schema has been resolved later.
            nodeToColumns(cols, lowerCase = true).map(_.copy(hiveType = null))
          }.getOrElse(Seq.empty[HiveColumn])

          val properties = scala.collection.mutable.Map.empty[String, String]

          maybeProperties.foreach {
            case Token("TOK_TABLEPROPERTIES", list :: Nil) =>
              properties ++= getProperties(list)
          }

          maybeComment.foreach {
            case Token("TOK_TABLECOMMENT", child :: Nil) =>
              val comment = unescapeSQLString(child.text)
              if (comment ne null) {
                properties += ("comment" -> comment)
              }
          }

          createView(view, viewNameParts, query, schema, properties.toMap,
            allowExisting.isDefined, replace.isDefined)
        }

      case Token("TOK_CREATETABLE", children)
        if children.collect { case t @ Token("TOK_QUERY", _) => t }.nonEmpty =>
        // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        val (
          Some(tableNameParts) ::
            _ /* likeTable */ ::
            externalTable ::
            Some(query) ::
            allowExisting +:
              _) =
          getClauses(
            Seq(
              "TOK_TABNAME",
              "TOK_LIKETABLE",
              "EXTERNAL",
              "TOK_QUERY",
              "TOK_IFNOTEXISTS",
              "TOK_TABLECOMMENT",
              "TOK_TABCOLLIST",
              "TOK_TABLEPARTCOLS", // Partitioned by
              "TOK_TABLEBUCKETS", // Clustered by
              "TOK_TABLESKEWED", // Skewed by
              "TOK_TABLEROWFORMAT",
              "TOK_TABLESERIALIZER",
              "TOK_FILEFORMAT_GENERIC",
              "TOK_TABLEFILEFORMAT", // User-provided InputFormat and OutputFormat
              "TOK_STORAGEHANDLER", // Storage handler
              "TOK_TABLELOCATION",
              "TOK_TABLEPROPERTIES"),
            children)
        val TableIdentifier(tblName, dbName) = extractTableIdent(tableNameParts)

        // TODO add bucket support
        var tableDesc: HiveTable = HiveTable(
          specifiedDatabase = dbName,
          name = tblName,
          schema = Seq.empty[HiveColumn],
          partitionColumns = Seq.empty[HiveColumn],
          properties = Map[String, String](),
          serdeProperties = Map[String, String](),
          tableType = if (externalTable.isDefined) ExternalTable else ManagedTable,
          location = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          viewText = None)

        // default storage type abbreviation (e.g. RCFile, ORC, PARQUET etc.)
        val defaultStorageType = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT)
        // handle the default format for the storage type abbreviation
        val hiveSerDe = HiveSerDe.sourceToSerDe(defaultStorageType, hiveConf).getOrElse {
          HiveSerDe(
            inputFormat = Option("org.apache.hadoop.mapred.TextInputFormat"),
            outputFormat = Option("org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"))
        }

        hiveSerDe.inputFormat.foreach(f => tableDesc = tableDesc.copy(inputFormat = Some(f)))
        hiveSerDe.outputFormat.foreach(f => tableDesc = tableDesc.copy(outputFormat = Some(f)))
        hiveSerDe.serde.foreach(f => tableDesc = tableDesc.copy(serde = Some(f)))

        children.collect {
          case list @ Token("TOK_TABCOLLIST", _) =>
            val cols = nodeToColumns(list, lowerCase = true)
            if (cols != null) {
              tableDesc = tableDesc.copy(schema = cols)
            }
          case Token("TOK_TABLECOMMENT", child :: Nil) =>
            val comment = unescapeSQLString(child.text)
            // TODO support the sql text
            tableDesc = tableDesc.copy(viewText = Option(comment))
          case Token("TOK_TABLEPARTCOLS", list @ Token("TOK_TABCOLLIST", _) :: Nil) =>
            val cols = nodeToColumns(list.head, lowerCase = false)
            if (cols != null) {
              tableDesc = tableDesc.copy(partitionColumns = cols)
            }
          case Token("TOK_TABLEROWFORMAT", Token("TOK_SERDEPROPS", child :: Nil) :: Nil) =>
            val serdeParams = new java.util.HashMap[String, String]()
            child match {
              case Token("TOK_TABLEROWFORMATFIELD", rowChild1 :: rowChild2) =>
                val fieldDelim = unescapeSQLString (rowChild1.text)
                serdeParams.put(serdeConstants.FIELD_DELIM, fieldDelim)
                serdeParams.put(serdeConstants.SERIALIZATION_FORMAT, fieldDelim)
                if (rowChild2.length > 1) {
                  val fieldEscape = unescapeSQLString (rowChild2.head.text)
                  serdeParams.put(serdeConstants.ESCAPE_CHAR, fieldEscape)
                }
              case Token("TOK_TABLEROWFORMATCOLLITEMS", rowChild :: Nil) =>
                val collItemDelim = unescapeSQLString(rowChild.text)
                serdeParams.put(serdeConstants.COLLECTION_DELIM, collItemDelim)
              case Token("TOK_TABLEROWFORMATMAPKEYS", rowChild :: Nil) =>
                val mapKeyDelim = unescapeSQLString(rowChild.text)
                serdeParams.put(serdeConstants.MAPKEY_DELIM, mapKeyDelim)
              case Token("TOK_TABLEROWFORMATLINES", rowChild :: Nil) =>
                val lineDelim = unescapeSQLString(rowChild.text)
                if (!(lineDelim == "\n") && !(lineDelim == "10")) {
                  throw new AnalysisException(
                    s"LINES TERMINATED BY only supports newline '\\n' right now: $rowChild")
                }
                serdeParams.put(serdeConstants.LINE_DELIM, lineDelim)
              case Token("TOK_TABLEROWFORMATNULL", rowChild :: Nil) =>
                val nullFormat = unescapeSQLString(rowChild.text)
              // TODO support the nullFormat
              case _ => assert(false)
            }
            tableDesc = tableDesc.copy(
              serdeProperties = tableDesc.serdeProperties ++ serdeParams.asScala)
          case Token("TOK_TABLELOCATION", child :: Nil) =>
            val location = EximUtil.relativeToAbsolutePath(hiveConf, unescapeSQLString(child.text))
            tableDesc = tableDesc.copy(location = Option(location))
          case Token("TOK_TABLESERIALIZER", child :: Nil) =>
            tableDesc = tableDesc.copy(
              serde = Option(unescapeSQLString(child.children.head.text)))
            if (child.numChildren == 2) {
              // This is based on the readProps(..) method in
              // ql/src/java/org/apache/hadoop/hive/ql/parse/BaseSemanticAnalyzer.java:
              val serdeParams = child.children(1).children.head.children.map {
                case Token(_, Token(prop, Nil) :: valueNode) =>
                  val value = valueNode.headOption
                    .map(_.text)
                    .map(unescapeSQLString)
                    .orNull
                  (unescapeSQLString(prop), value)
              }.toMap
              tableDesc = tableDesc.copy(serdeProperties = tableDesc.serdeProperties ++ serdeParams)
            }
          case Token("TOK_FILEFORMAT_GENERIC", child :: Nil) =>
            child.text.toLowerCase(Locale.ENGLISH) match {
              case "orc" =>
                tableDesc = tableDesc.copy(
                  inputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
                  outputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
                if (tableDesc.serde.isEmpty) {
                  tableDesc = tableDesc.copy(
                    serde = Option("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
                }

              case "parquet" =>
                tableDesc = tableDesc.copy(
                  inputFormat =
                    Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
                  outputFormat =
                    Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
                if (tableDesc.serde.isEmpty) {
                  tableDesc = tableDesc.copy(
                    serde = Option("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
                }

              case "rcfile" =>
                tableDesc = tableDesc.copy(
                  inputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileInputFormat"),
                  outputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
                if (tableDesc.serde.isEmpty) {
                  tableDesc = tableDesc.copy(serde =
                    Option("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"))
                }

              case "textfile" =>
                tableDesc = tableDesc.copy(
                  inputFormat =
                    Option("org.apache.hadoop.mapred.TextInputFormat"),
                  outputFormat =
                    Option("org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"))

              case "sequencefile" =>
                tableDesc = tableDesc.copy(
                  inputFormat = Option("org.apache.hadoop.mapred.SequenceFileInputFormat"),
                  outputFormat = Option("org.apache.hadoop.mapred.SequenceFileOutputFormat"))

              case "avro" =>
                tableDesc = tableDesc.copy(
                  inputFormat =
                    Option("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"),
                  outputFormat =
                    Option("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
                if (tableDesc.serde.isEmpty) {
                  tableDesc = tableDesc.copy(
                    serde = Option("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
                }

              case _ =>
                throw new AnalysisException(
                  s"Unrecognized file format in STORED AS clause: ${child.text}")
            }

          case Token("TOK_TABLESERIALIZER",
          Token("TOK_SERDENAME", Token(serdeName, Nil) :: otherProps) :: Nil) =>
            tableDesc = tableDesc.copy(serde = Option(unquoteString(serdeName)))

            otherProps match {
              case Token("TOK_TABLEPROPERTIES", list :: Nil) :: Nil =>
                tableDesc = tableDesc.copy(
                  serdeProperties = tableDesc.serdeProperties ++ getProperties(list))
              case _ =>
            }

          case Token("TOK_TABLEPROPERTIES", list :: Nil) =>
            tableDesc = tableDesc.copy(properties = tableDesc.properties ++ getProperties(list))
          case list @ Token("TOK_TABLEFILEFORMAT", _) =>
            tableDesc = tableDesc.copy(
              inputFormat =
                Option(unescapeSQLString(list.children.head.text)),
              outputFormat =
                Option(unescapeSQLString(list.children(1).text)))
          case Token("TOK_STORAGEHANDLER", _) =>
            throw new AnalysisException(
              "CREATE TABLE AS SELECT cannot be used for a non-native table")
          case _ => // Unsupport features
        }

        CreateTableAsSelect(tableDesc, nodeToPlan(query), allowExisting.isDefined)

      // If its not a "CTAS" like above then take it as a native command
      case Token("TOK_CREATETABLE", _) =>
        NativePlaceholder

      // Support "TRUNCATE TABLE table_name [PARTITION partition_spec]"
      case Token("TOK_TRUNCATETABLE", Token("TOK_TABLE_PARTITION", table) :: Nil) =>
        NativePlaceholder

      case _ =>
        super.nodeToPlan(node)
    }
  }

  protected override def nodeToDescribeFallback(node: ASTNode): LogicalPlan = NativePlaceholder

  protected override def nodeToTransformation(
      node: ASTNode,
      child: LogicalPlan): Option[logical.ScriptTransformation] = node match {
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
        case _ =>
          noParseRule("Transform", node)
      }

      type SerDeInfo = (
        Seq[(String, String)],  // Input row format information
          Option[String],         // Optional input SerDe class
          Seq[(String, String)],  // Input SerDe properties
          Boolean                 // Whether to use default record reader/writer
        )

      def matchSerDe(clause: Seq[ASTNode]): SerDeInfo = clause match {
        case Token("TOK_SERDEPROPS", propsClause) :: Nil =>
          val rowFormat = propsClause.map {
            case Token(name, Token(value, Nil) :: Nil) => (name, value)
          }
          (rowFormat, None, Nil, false)

        case Token("TOK_SERDENAME", Token(serdeClass, Nil) :: Nil) :: Nil =>
          (Nil, Some(unescapeSQLString(serdeClass)), Nil, false)

        case Token("TOK_SERDENAME", Token(serdeClass, Nil) ::
          Token("TOK_TABLEPROPERTIES",
          Token("TOK_TABLEPROPLIST", propsClause) :: Nil) :: Nil) :: Nil =>
          val serdeProps = propsClause.map {
            case Token("TOK_TABLEPROPERTY", Token(name, Nil) :: Token(value, Nil) :: Nil) =>
              (unescapeSQLString(name), unescapeSQLString(value))
          }

          // SPARK-10310: Special cases LazySimpleSerDe
          // TODO Fully supports user-defined record reader/writer classes
          val unescapedSerDeClass = unescapeSQLString(serdeClass)
          val useDefaultRecordReaderWriter =
            unescapedSerDeClass == classOf[LazySimpleSerDe].getCanonicalName
          (Nil, Some(unescapedSerDeClass), serdeProps, useDefaultRecordReaderWriter)

        case Nil =>
          // Uses default TextRecordReader/TextRecordWriter, sets field delimiter here
          val serdeProps = Seq(serdeConstants.FIELD_DELIM -> "\t")
          (Nil, Option(hiveConf.getVar(ConfVars.HIVESCRIPTSERDE)), serdeProps, true)
      }

      val (inRowFormat, inSerdeClass, inSerdeProps, useDefaultRecordReader) =
        matchSerDe(inputSerdeClause)

      val (outRowFormat, outSerdeClass, outSerdeProps, useDefaultRecordWriter) =
        matchSerDe(outputSerdeClause)

      val unescapedScript = unescapeSQLString(script)

      // TODO Adds support for user-defined record reader/writer classes
      val recordReaderClass = if (useDefaultRecordReader) {
        Option(hiveConf.getVar(ConfVars.HIVESCRIPTRECORDREADER))
      } else {
        None
      }

      val recordWriterClass = if (useDefaultRecordWriter) {
        Option(hiveConf.getVar(ConfVars.HIVESCRIPTRECORDWRITER))
      } else {
        None
      }

      val schema = HiveScriptIOSchema(
        inRowFormat, outRowFormat,
        inSerdeClass, outSerdeClass,
        inSerdeProps, outSerdeProps,
        recordReaderClass, recordWriterClass,
        schemaLess)

      Some(
        logical.ScriptTransformation(
          inputExprs.map(nodeToExpr),
          unescapedScript,
          output,
          child, schema))
    case _ => None
  }

  protected override def nodeToGenerator(node: ASTNode): Generator = node match {
    case Token("TOK_FUNCTION", Token(functionName, Nil) :: children) =>
      val functionInfo: FunctionInfo =
        Option(FunctionRegistry.getFunctionInfo(functionName.toLowerCase)).getOrElse(
          sys.error(s"Couldn't find function $functionName"))
      val functionClassName = functionInfo.getFunctionClass.getName
      HiveGenericUDTF(
        functionName, new HiveFunctionWrapper(functionClassName), children.map(nodeToExpr))
    case other => super.nodeToGenerator(node)
  }

  // This is based the getColumns methods in
  // ql/src/java/org/apache/hadoop/hive/ql/parse/BaseSemanticAnalyzer.java
  protected def nodeToColumns(node: ASTNode, lowerCase: Boolean): Seq[HiveColumn] = {
    node.children.map(_.children).collect {
      case Token(rawColName, Nil) :: colTypeNode :: comment =>
        val colName = if (!lowerCase) rawColName
        else rawColName.toLowerCase
        HiveColumn(
          cleanIdentifier(colName),
          nodeToTypeString(colTypeNode),
          comment.headOption.map(n => unescapeSQLString(n.text)).orNull)
    }
  }

  // This is based on the following methods in
  // ql/src/java/org/apache/hadoop/hive/ql/parse/BaseSemanticAnalyzer.java:
  //  getTypeStringFromAST
  //  getStructTypeStringFromAST
  //  getUnionTypeStringFromAST
  protected def nodeToTypeString(node: ASTNode): String = node.tokenType match {
    case SparkSqlParser.TOK_LIST =>
      val listType :: Nil = node.children
      val listTypeString = nodeToTypeString(listType)
      s"${serdeConstants.LIST_TYPE_NAME}<$listTypeString>"

    case SparkSqlParser.TOK_MAP =>
      val keyType :: valueType :: Nil = node.children
      val keyTypeString = nodeToTypeString(keyType)
      val valueTypeString = nodeToTypeString(valueType)
      s"${serdeConstants.MAP_TYPE_NAME}<$keyTypeString,$valueTypeString>"

    case SparkSqlParser.TOK_STRUCT =>
      val typeNode = node.children.head
      require(typeNode.children.nonEmpty, "Struct must have one or more columns.")
      val structColStrings = typeNode.children.map { columnNode =>
        val Token(colName, Nil) :: colTypeNode :: Nil = columnNode.children
        cleanIdentifier(colName) + ":" + nodeToTypeString(colTypeNode)
      }
      s"${serdeConstants.STRUCT_TYPE_NAME}<${structColStrings.mkString(",")}>"

    case SparkSqlParser.TOK_UNIONTYPE =>
      val typeNode = node.children.head
      val unionTypesString = typeNode.children.map(nodeToTypeString).mkString(",")
      s"${serdeConstants.UNION_TYPE_NAME}<$unionTypesString>"

    case SparkSqlParser.TOK_CHAR =>
      val Token(size, Nil) :: Nil = node.children
      s"${serdeConstants.CHAR_TYPE_NAME}($size)"

    case SparkSqlParser.TOK_VARCHAR =>
      val Token(size, Nil) :: Nil = node.children
      s"${serdeConstants.VARCHAR_TYPE_NAME}($size)"

    case SparkSqlParser.TOK_DECIMAL =>
      val precisionAndScale = node.children match {
        case Token(precision, Nil) :: Token(scale, Nil) :: Nil =>
          precision + "," + scale
        case Token(precision, Nil) :: Nil =>
          precision + "," + HiveDecimal.USER_DEFAULT_SCALE
        case Nil =>
          HiveDecimal.USER_DEFAULT_PRECISION + "," + HiveDecimal.USER_DEFAULT_SCALE
        case _ =>
          noParseRule("Decimal", node)
      }
      s"${serdeConstants.DECIMAL_TYPE_NAME}($precisionAndScale)"

    // Simple data types.
    case SparkSqlParser.TOK_BOOLEAN => serdeConstants.BOOLEAN_TYPE_NAME
    case SparkSqlParser.TOK_TINYINT => serdeConstants.TINYINT_TYPE_NAME
    case SparkSqlParser.TOK_SMALLINT => serdeConstants.SMALLINT_TYPE_NAME
    case SparkSqlParser.TOK_INT => serdeConstants.INT_TYPE_NAME
    case SparkSqlParser.TOK_BIGINT => serdeConstants.BIGINT_TYPE_NAME
    case SparkSqlParser.TOK_FLOAT => serdeConstants.FLOAT_TYPE_NAME
    case SparkSqlParser.TOK_DOUBLE => serdeConstants.DOUBLE_TYPE_NAME
    case SparkSqlParser.TOK_STRING => serdeConstants.STRING_TYPE_NAME
    case SparkSqlParser.TOK_BINARY => serdeConstants.BINARY_TYPE_NAME
    case SparkSqlParser.TOK_DATE => serdeConstants.DATE_TYPE_NAME
    case SparkSqlParser.TOK_TIMESTAMP => serdeConstants.TIMESTAMP_TYPE_NAME
    case SparkSqlParser.TOK_INTERVAL_YEAR_MONTH => serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME
    case SparkSqlParser.TOK_INTERVAL_DAY_TIME => serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME
    case SparkSqlParser.TOK_DATETIME => serdeConstants.DATETIME_TYPE_NAME
    case _ => null
  }

}
