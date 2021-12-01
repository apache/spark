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

package org.apache.spark.sql.execution

import java.time.ZoneOffset
import java.util.{Locale, TimeZone}
import javax.ws.rs.core.UriBuilder

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}

/**
 * Concrete parser for Spark SQL statements.
 */
class SparkSqlParser extends AbstractSqlParser {
  val astBuilder = new SparkSqlAstBuilder()

  private val substitutor = new VariableSubstitution()

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class SparkSqlAstBuilder extends AstBuilder {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  private val configKeyValueDef = """([a-zA-Z_\d\\.:]+)\s*=([^;]*);*""".r
  private val configKeyDef = """([a-zA-Z_\d\\.:]+)$""".r
  private val configValueDef = """([^;]*);*""".r

  /**
   * Create a [[SetCommand]] logical plan.
   *
   * Note that we assume that everything after the SET keyword is assumed to be a part of the
   * key-value pair. The split between key and value is made by searching for the first `=`
   * character in the raw string.
   */
  override def visitSetConfiguration(ctx: SetConfigurationContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.configKey() != null) {
      val keyStr = ctx.configKey().getText
      if (ctx.EQ() != null) {
        remainder(ctx.EQ().getSymbol).trim match {
          case configValueDef(valueStr) => SetCommand(Some(keyStr -> Option(valueStr)))
          case other => throw new ParseException(s"'$other' is an invalid property " +
            s"value, please use quotes, e.g. SET `$keyStr`=`$other`", ctx)
        }
      } else {
        SetCommand(Some(keyStr -> None))
      }
    } else {
      remainder(ctx.SET.getSymbol).trim match {
        case configKeyValueDef(key, value) =>
          SetCommand(Some(key -> Option(value.trim)))
        case configKeyDef(key) =>
          SetCommand(Some(key -> None))
        case s if s == "-v" =>
          SetCommand(Some("-v" -> None))
        case s if s.isEmpty =>
          SetCommand(None)
        case _ => throw new ParseException("Expected format is 'SET', 'SET key', or " +
          "'SET key=value'. If you want to include special characters in key, or include " +
          "semicolon in value, please use quotes, e.g., SET `ke y`=`v;alue`.", ctx)
      }
    }
  }

  override def visitSetQuotedConfiguration(
      ctx: SetQuotedConfigurationContext): LogicalPlan = withOrigin(ctx) {
    assert(ctx.configValue() != null)
    if (ctx.configKey() != null) {
      SetCommand(Some(ctx.configKey().getText -> Option(ctx.configValue().getText)))
    } else {
      val valueStr = ctx.configValue().getText
      val keyCandidate = interval(ctx.SET().getSymbol, ctx.EQ().getSymbol).trim
      keyCandidate match {
        case configKeyDef(key) => SetCommand(Some(key -> Option(valueStr)))
        case _ => throw new ParseException(s"'$keyCandidate' is an invalid property " +
          s"key, please use quotes, e.g. SET `$keyCandidate`=`$valueStr`", ctx)
      }
    }
  }

  /**
   * Create a [[ResetCommand]] logical plan.
   * Example SQL :
   * {{{
   *   RESET;
   *   RESET spark.sql.session.timeZone;
   * }}}
   */
  override def visitResetConfiguration(
      ctx: ResetConfigurationContext): LogicalPlan = withOrigin(ctx) {
    remainder(ctx.RESET.getSymbol).trim match {
      case configKeyDef(key) =>
        ResetCommand(Some(key))
      case s if s.trim.isEmpty =>
        ResetCommand(None)
      case _ => throw new ParseException("Expected format is 'RESET' or 'RESET key'. " +
        "If you want to include special characters in key, " +
        "please use quotes, e.g., RESET `ke y`.", ctx)
    }
  }

  override def visitResetQuotedConfiguration(
      ctx: ResetQuotedConfigurationContext): LogicalPlan = withOrigin(ctx) {
    ResetCommand(Some(ctx.configKey().getText))
  }

  /**
   * Create a [[SetCommand]] logical plan to set [[SQLConf.SESSION_LOCAL_TIMEZONE]]
   * Example SQL :
   * {{{
   *   SET TIME ZONE LOCAL;
   *   SET TIME ZONE 'Asia/Shanghai';
   *   SET TIME ZONE INTERVAL 10 HOURS;
   * }}}
   */
  override def visitSetTimeZone(ctx: SetTimeZoneContext): LogicalPlan = withOrigin(ctx) {
    val key = SQLConf.SESSION_LOCAL_TIMEZONE.key
    if (ctx.interval != null) {
      val interval = parseIntervalLiteral(ctx.interval)
      if (interval.months != 0 || interval.days != 0 ||
        math.abs(interval.microseconds) > 18 * DateTimeConstants.MICROS_PER_HOUR ||
        interval.microseconds % DateTimeConstants.MICROS_PER_SECOND != 0) {
        throw new ParseException("The interval value must be in the range of [-18, +18] hours" +
          " with second precision",
          ctx.interval())
      } else {
        val seconds = (interval.microseconds / DateTimeConstants.MICROS_PER_SECOND).toInt
        SetCommand(Some(key -> Some(ZoneOffset.ofTotalSeconds(seconds).toString)))
      }
    } else if (ctx.timezone != null) {
      ctx.timezone.getType match {
        case SqlBaseParser.LOCAL =>
          SetCommand(Some(key -> Some(TimeZone.getDefault.getID)))
        case _ =>
          SetCommand(Some(key -> Some(string(ctx.STRING))))
      }
    } else {
      throw new ParseException("Invalid time zone displacement value", ctx)
    }
  }

  /**
   * Create a [[RefreshResource]] logical plan.
   */
  override def visitRefreshResource(ctx: RefreshResourceContext): LogicalPlan = withOrigin(ctx) {
    val path = if (ctx.STRING != null) string(ctx.STRING) else extractUnquotedResourcePath(ctx)
    RefreshResource(path)
  }

  private def extractUnquotedResourcePath(ctx: RefreshResourceContext): String = withOrigin(ctx) {
    val unquotedPath = remainder(ctx.REFRESH.getSymbol).trim
    validate(
      unquotedPath != null && !unquotedPath.isEmpty,
      "Resource paths cannot be empty in REFRESH statements. Use / to match everything",
      ctx)
    val forbiddenSymbols = Seq(" ", "\n", "\r", "\t")
    validate(
      !forbiddenSymbols.exists(unquotedPath.contains(_)),
      "REFRESH statements cannot contain ' ', '\\n', '\\r', '\\t' inside unquoted resource paths",
      ctx)
    unquotedPath
  }

  /**
   * Create a [[CacheTableCommand]].
   *
   * For example:
   * {{{
   *   CACHE [LAZY] TABLE multi_part_name
   *   [OPTIONS tablePropertyList] [[AS] query]
   * }}}
   */
  override def visitCacheTable(ctx: CacheTableContext): LogicalPlan = withOrigin(ctx) {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val query = Option(ctx.query).map(plan)
    val tableName = visitMultipartIdentifier(ctx.multipartIdentifier)
    if (query.isDefined && tableName.length > 1) {
      val catalogAndNamespace = tableName.init
      throw new ParseException("It is not allowed to add catalog/namespace " +
        s"prefix ${catalogAndNamespace.quoted} to " +
        "the table name in CACHE TABLE AS SELECT", ctx)
    }
    val queryText = Option(ctx.query).map(source(_))
    val options = Option(ctx.options).map(visitPropertyKeyValues).getOrElse(Map.empty)
    CacheTableCommand(tableName, query, queryText, ctx.LAZY != null, options)
  }


  /**
   * Create an [[UncacheTableCommand]] logical plan.
   */
  override def visitUncacheTable(ctx: UncacheTableContext): LogicalPlan = withOrigin(ctx) {
    UncacheTableCommand(
      visitMultipartIdentifier(ctx.multipartIdentifier),
      ctx.EXISTS != null)
  }

  /**
   * Create a [[ClearCacheCommand]] logical plan.
   */
  override def visitClearCache(ctx: ClearCacheContext): LogicalPlan = withOrigin(ctx) {
    ClearCacheCommand
  }

  /**
   * Create an [[ExplainCommand]] logical plan.
   * The syntax of using this command in SQL is:
   * {{{
   *   EXPLAIN (EXTENDED | CODEGEN | COST | FORMATTED) SELECT * FROM ...
   * }}}
   */
  override def visitExplain(ctx: ExplainContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.LOGICAL != null) {
      operationNotAllowed("EXPLAIN LOGICAL", ctx)
    }

    val statement = plan(ctx.statement)
    if (statement == null) {
      null  // This is enough since ParseException will raise later.
    } else {
      ExplainCommand(
        logicalPlan = statement,
        mode = {
          if (ctx.EXTENDED != null) ExtendedMode
          else if (ctx.CODEGEN != null) CodegenMode
          else if (ctx.COST != null) CostMode
          else if (ctx.FORMATTED != null) FormattedMode
          else SimpleMode
        })
    }
  }

  /**
   * Create a [[DescribeQueryCommand]] logical command.
   */
  override def visitDescribeQuery(ctx: DescribeQueryContext): LogicalPlan = withOrigin(ctx) {
    DescribeQueryCommand(source(ctx.query), visitQuery(ctx.query))
  }

  /**
   * Converts a multi-part identifier to a TableIdentifier.
   *
   * If the multi-part identifier has too many parts, this will throw a ParseException.
   */
  def tableIdentifier(
      multipart: Seq[String],
      command: String,
      ctx: ParserRuleContext): TableIdentifier = {
    multipart match {
      case Seq(tableName) =>
        TableIdentifier(tableName)
      case Seq(database, tableName) =>
        TableIdentifier(tableName, Some(database))
      case _ =>
        operationNotAllowed(s"$command does not support multi-part identifiers", ctx)
    }
  }

  /**
   * Create a table, returning a [[CreateTable]] logical plan.
   *
   * This is used to produce CreateTempViewUsing from CREATE TEMPORARY TABLE.
   *
   * TODO: Remove this. It is used because CreateTempViewUsing is not a Catalyst plan.
   * Either move CreateTempViewUsing into catalyst as a parsed logical plan, or remove it because
   * it is deprecated.
   */
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = withOrigin(ctx) {
    val (ident, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)

    if (!temp || ctx.query != null) {
      super.visitCreateTable(ctx)
    } else {
      if (external) {
        operationNotAllowed("CREATE EXTERNAL TABLE ... USING", ctx)
      }
      if (ifNotExists) {
        // Unlike CREATE TEMPORARY VIEW USING, CREATE TEMPORARY TABLE USING does not support
        // IF NOT EXISTS. Users are not allowed to replace the existing temp table.
        operationNotAllowed("CREATE TEMPORARY TABLE IF NOT EXISTS", ctx)
      }

      val (_, _, _, _, options, location, _, _) = visitCreateTableClauses(ctx.createTableClauses())
      val provider = Option(ctx.tableProvider).map(_.multipartIdentifier.getText).getOrElse(
        throw new ParseException("CREATE TEMPORARY TABLE without a provider is not allowed.", ctx))
      val schema = Option(ctx.colTypeList()).map(createSchema)

      logWarning(s"CREATE TEMPORARY TABLE ... USING ... is deprecated, please use " +
          "CREATE TEMPORARY VIEW ... USING ... instead")

      val table = tableIdentifier(ident, "CREATE TEMPORARY VIEW", ctx)
      val optionsWithLocation = location.map(l => options + ("path" -> l)).getOrElse(options)
      CreateTempViewUsing(table, schema, replace = false, global = false, provider,
        optionsWithLocation)
    }
  }

  /**
   * Creates a [[CreateTempViewUsing]] logical plan.
   */
  override def visitCreateTempViewUsing(
      ctx: CreateTempViewUsingContext): LogicalPlan = withOrigin(ctx) {
    CreateTempViewUsing(
      tableIdent = visitTableIdentifier(ctx.tableIdentifier()),
      userSpecifiedSchema = Option(ctx.colTypeList()).map(createSchema),
      replace = ctx.REPLACE != null,
      global = ctx.GLOBAL != null,
      provider = ctx.tableProvider.multipartIdentifier.getText,
      options = Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Convert a nested constants list into a sequence of string sequences.
   */
  override def visitNestedConstantList(
      ctx: NestedConstantListContext): Seq[Seq[String]] = withOrigin(ctx) {
    ctx.constantList.asScala.map(visitConstantList).toSeq
  }

  /**
   * Convert a constants list into a String sequence.
   */
  override def visitConstantList(ctx: ConstantListContext): Seq[String] = withOrigin(ctx) {
    ctx.constant.asScala.map(v => visitStringConstant(v, legacyNullAsString = false)).toSeq
  }

  /**
   * Fail an unsupported Hive native command.
   */
  override def visitFailNativeCommand(
    ctx: FailNativeCommandContext): LogicalPlan = withOrigin(ctx) {
    val keywords = if (ctx.unsupportedHiveNativeCommands != null) {
      ctx.unsupportedHiveNativeCommands.children.asScala.collect {
        case n: TerminalNode => n.getText
      }.mkString(" ")
    } else {
      // SET ROLE is the exception to the rule, because we handle this before other SET commands.
      "SET ROLE"
    }
    operationNotAllowed(keywords, ctx)
  }

  /**
   * Create a [[AddFileCommand]], [[AddJarCommand]], [[ListFilesCommand]] or [[ListJarsCommand]]
   * command depending on the requested operation on resources.
   * Expected format:
   * {{{
   *   ADD (FILE[S] <filepath ...> | JAR[S] <jarpath ...>)
   *   LIST (FILE[S] [filepath ...] | JAR[S] [jarpath ...])
   * }}}
   *
   * Note that filepath/jarpath can be given as follows;
   *  - /path/to/fileOrJar
   *  - "/path/to/fileOrJar"
   *  - '/path/to/fileOrJar'
   */
  override def visitManageResource(ctx: ManageResourceContext): LogicalPlan = withOrigin(ctx) {
    val maybePaths = if (ctx.STRING != null) string(ctx.STRING) else remainder(ctx.identifier).trim
    ctx.op.getType match {
      case SqlBaseParser.ADD =>
        ctx.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "file" => AddFileCommand(maybePaths)
          case "jar" => AddJarCommand(maybePaths)
          case other => operationNotAllowed(s"ADD with resource type '$other'", ctx)
        }
      case SqlBaseParser.LIST =>
        ctx.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "files" | "file" =>
            if (maybePaths.length > 0) {
              ListFilesCommand(maybePaths.split("\\s+"))
            } else {
              ListFilesCommand()
            }
          case "jars" | "jar" =>
            if (maybePaths.length > 0) {
              ListJarsCommand(maybePaths.split("\\s+"))
            } else {
              ListJarsCommand()
            }
          case other => operationNotAllowed(s"LIST with resource type '$other'", ctx)
        }
      case _ => operationNotAllowed(s"Other types of operation on resources", ctx)
    }
  }

  private def toStorageFormat(
      location: Option[String],
      maybeSerdeInfo: Option[SerdeInfo],
      ctx: ParserRuleContext): CatalogStorageFormat = {
    if (maybeSerdeInfo.isEmpty) {
      CatalogStorageFormat.empty.copy(locationUri = location.map(CatalogUtils.stringToURI))
    } else {
      val serdeInfo = maybeSerdeInfo.get
      if (serdeInfo.storedAs.isEmpty) {
        CatalogStorageFormat.empty.copy(
          locationUri = location.map(CatalogUtils.stringToURI),
          inputFormat = serdeInfo.formatClasses.map(_.input),
          outputFormat = serdeInfo.formatClasses.map(_.output),
          serde = serdeInfo.serde,
          properties = serdeInfo.serdeProperties)
      } else {
        HiveSerDe.sourceToSerDe(serdeInfo.storedAs.get) match {
          case Some(hiveSerde) =>
            CatalogStorageFormat.empty.copy(
              locationUri = location.map(CatalogUtils.stringToURI),
              inputFormat = hiveSerde.inputFormat,
              outputFormat = hiveSerde.outputFormat,
              serde = serdeInfo.serde.orElse(hiveSerde.serde),
              properties = serdeInfo.serdeProperties)
          case _ =>
            operationNotAllowed(s"STORED AS with file format '${serdeInfo.storedAs.get}'", ctx)
        }
      }
    }
  }

  /**
   * Create a [[CreateTableLikeCommand]] command.
   *
   * For example:
   * {{{
   *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
   *   LIKE [other_db_name.]existing_table_name
   *   [USING provider |
   *    [
   *     [ROW FORMAT row_format]
   *     [STORED AS file_format] [WITH SERDEPROPERTIES (...)]
   *    ]
   *   ]
   *   [locationSpec]
   *   [TBLPROPERTIES (property_name=property_value, ...)]
   * }}}
   */
  override def visitCreateTableLike(ctx: CreateTableLikeContext): LogicalPlan = withOrigin(ctx) {
    val targetTable = visitTableIdentifier(ctx.target)
    val sourceTable = visitTableIdentifier(ctx.source)
    checkDuplicateClauses(ctx.tableProvider, "PROVIDER", ctx)
    checkDuplicateClauses(ctx.createFileFormat, "STORED AS/BY", ctx)
    checkDuplicateClauses(ctx.rowFormat, "ROW FORMAT", ctx)
    checkDuplicateClauses(ctx.locationSpec, "LOCATION", ctx)
    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)
    val provider = ctx.tableProvider.asScala.headOption.map(_.multipartIdentifier.getText)
    val location = visitLocationSpecList(ctx.locationSpec())
    // TODO: Do not skip serde check for CREATE TABLE LIKE.
    val serdeInfo = getSerdeInfo(
      ctx.rowFormat.asScala.toSeq, ctx.createFileFormat.asScala.toSeq, ctx, skipCheck = true)
    if (provider.isDefined && serdeInfo.isDefined) {
      operationNotAllowed(s"CREATE TABLE LIKE ... USING ... ${serdeInfo.get.describe}", ctx)
    }

    // TODO: remove this restriction as it seems unnecessary.
    serdeInfo match {
      case Some(SerdeInfo(storedAs, formatClasses, serde, _)) =>
        if (storedAs.isEmpty && formatClasses.isEmpty && serde.isDefined) {
          throw new ParseException("'ROW FORMAT' must be used with 'STORED AS'", ctx)
        }
      case _ =>
    }

    // TODO: also look at `HiveSerDe.getDefaultStorage`.
    val storage = toStorageFormat(location, serdeInfo, ctx)
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    CreateTableLikeCommand(
      targetTable, sourceTable, storage, provider, properties, ctx.EXISTS != null)
  }

  /**
   * Create a [[ScriptInputOutputSchema]].
   */
  override protected def withScriptIOSchema(
      ctx: ParserRuleContext,
      inRowFormat: RowFormatContext,
      recordWriter: Token,
      outRowFormat: RowFormatContext,
      recordReader: Token,
      schemaLess: Boolean): ScriptInputOutputSchema = {
    if (recordWriter != null || recordReader != null) {
      // TODO: what does this message mean?
      throw new ParseException(
        "Unsupported operation: Used defined record reader/writer classes.", ctx)
    }

    // Decode and input/output format.
    type Format = (Seq[(String, String)], Option[String], Seq[(String, String)], Option[String])
    def format(
        fmt: RowFormatContext,
        configKey: String,
        defaultConfigValue: String): Format = fmt match {
      case c: RowFormatDelimitedContext =>
        // TODO we should use the visitRowFormatDelimited function here. However HiveScriptIOSchema
        // expects a seq of pairs in which the old parsers' token names are used as keys.
        // Transforming the result of visitRowFormatDelimited would be quite a bit messier than
        // retrieving the key value pairs ourselves.
        val entries = entry("TOK_TABLEROWFORMATFIELD", c.fieldsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATCOLLITEMS", c.collectionItemsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATMAPKEYS", c.keysTerminatedBy) ++
          entry("TOK_TABLEROWFORMATNULL", c.nullDefinedAs) ++
          Option(c.linesSeparatedBy).toSeq.map { token =>
            val value = string(token)
            validate(
              value == "\n",
              s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
              c)
            "TOK_TABLEROWFORMATLINES" -> value
          }

        (entries, None, Seq.empty, None)

      case c: RowFormatSerdeContext =>
        // Use a serde format.
        val SerdeInfo(None, None, Some(name), props) = visitRowFormatSerde(c)

        // SPARK-10310: Special cases LazySimpleSerDe
        val recordHandler = if (name == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
          Option(conf.getConfString(configKey, defaultConfigValue))
        } else {
          None
        }
        (Seq.empty, Option(name), props.toSeq, recordHandler)

      case null =>
        // Use default (serde) format.
        val name = conf.getConfString("hive.script.serde",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        val props = Seq(
          "field.delim" -> "\t",
          "serialization.last.column.takes.rest" -> "true")
        val recordHandler = Option(conf.getConfString(configKey, defaultConfigValue))
        (Nil, Option(name), props, recordHandler)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) =
      format(
        inRowFormat, "hive.script.recordreader", "org.apache.hadoop.hive.ql.exec.TextRecordReader")

    val (outFormat, outSerdeClass, outSerdeProps, writer) =
      format(
        outRowFormat, "hive.script.recordwriter",
        "org.apache.hadoop.hive.ql.exec.TextRecordWriter")

    ScriptInputOutputSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }

  /**
   * Create a clause for DISTRIBUTE BY.
   */
  override protected def withRepartitionByExpression(
      ctx: QueryOrganizationContext,
      expressions: Seq[Expression],
      query: LogicalPlan): LogicalPlan = {
    RepartitionByExpression(expressions, query, None)
  }

  /**
   * Return the parameters for [[InsertIntoDir]] logical plan.
   *
   * Expected format:
   * {{{
   *   INSERT OVERWRITE [LOCAL] DIRECTORY
   *   [path]
   *   [OPTIONS table_property_list]
   *   select_statement;
   * }}}
   */
  override def visitInsertOverwriteDir(
      ctx: InsertOverwriteDirContext): InsertDirParams = withOrigin(ctx) {
    val options = Option(ctx.options).map(visitPropertyKeyValues).getOrElse(Map.empty)
    var storage = DataSource.buildStorageFormatFromOptions(options)

    val path = Option(ctx.path).map(string).getOrElse("")

    if (!(path.isEmpty ^ storage.locationUri.isEmpty)) {
      throw new ParseException(
        "Directory path and 'path' in OPTIONS should be specified one, but not both", ctx)
    }

    if (!path.isEmpty) {
      val customLocation = Some(CatalogUtils.stringToURI(path))
      storage = storage.copy(locationUri = customLocation)
    }

    if (ctx.LOCAL() != null) {
      // assert if directory is local when LOCAL keyword is mentioned
      val scheme = Option(storage.locationUri.get.getScheme)
      scheme match {
        case Some(pathScheme) if (!pathScheme.equals("file")) =>
          throw new ParseException("LOCAL is supported only with file: scheme", ctx)
        case _ =>
          // force scheme to be file rather than fs.default.name
          val loc = Some(UriBuilder.fromUri(CatalogUtils.stringToURI(path)).scheme("file").build())
          storage = storage.copy(locationUri = loc)
      }
    }

    val provider = ctx.tableProvider.multipartIdentifier.getText

    (false, storage, Some(provider))
  }

  /**
   * Return the parameters for [[InsertIntoDir]] logical plan.
   *
   * Expected format:
   * {{{
   *   INSERT OVERWRITE [LOCAL] DIRECTORY
   *   path
   *   [ROW FORMAT row_format]
   *   [STORED AS file_format]
   *   select_statement;
   * }}}
   */
  override def visitInsertOverwriteHiveDir(
      ctx: InsertOverwriteHiveDirContext): InsertDirParams = withOrigin(ctx) {
    val serdeInfo = getSerdeInfo(
      Option(ctx.rowFormat).toSeq, Option(ctx.createFileFormat).toSeq, ctx)
    val path = string(ctx.path)
    // The path field is required
    if (path.isEmpty) {
      operationNotAllowed("INSERT OVERWRITE DIRECTORY must be accompanied by path", ctx)
    }

    val default = HiveSerDe.getDefaultStorage(conf)
    val storage = toStorageFormat(Some(path), serdeInfo, ctx)
    val finalStorage = storage.copy(
      inputFormat = storage.inputFormat.orElse(default.inputFormat),
      outputFormat = storage.outputFormat.orElse(default.outputFormat),
      serde = storage.serde.orElse(default.serde))

    (ctx.LOCAL != null, finalStorage, Some(DDLUtils.HIVE_PROVIDER))
  }
}
