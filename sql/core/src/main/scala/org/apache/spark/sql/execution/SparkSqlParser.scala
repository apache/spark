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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, PersistedView, UnresolvedDBObjectName, UnresolvedFunc}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

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
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private val configKeyValueDef = """([a-zA-Z_\d\\.:]+)\s*=([^;]*);*""".r
  private val configKeyDef = """([a-zA-Z_\d\\.:]+)$""".r
  private val configValueDef = """([^;]*);*""".r
  private val strLiteralDef = """(".*?[^\\]"|'.*?[^\\]'|[^ \n\r\t"']+)""".r

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
          case other => throw QueryParsingErrors.invalidPropertyValueForSetQuotedConfigurationError(
            other, keyStr, ctx)
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
        case _ => throw QueryParsingErrors.unexpectedFormatForSetConfigurationError(ctx)
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
        case _ => throw QueryParsingErrors.invalidPropertyKeyForSetQuotedConfigurationError(
          keyCandidate, valueStr, ctx)
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
      case _ => throw QueryParsingErrors.unexpectedFormatForResetConfigurationError(ctx)
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
        throw QueryParsingErrors.intervalValueOutOfRangeError(ctx.interval())
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
      throw QueryParsingErrors.invalidTimeZoneDisplacementValueError(ctx)
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
   * Create a [[ShowCurrentNamespaceCommand]] logical command.
   */
  override def visitShowCurrentNamespace(
      ctx: ShowCurrentNamespaceContext) : LogicalPlan = withOrigin(ctx) {
    ShowCurrentNamespaceCommand()
  }

  /**
   * Create a [[SetNamespaceCommand]] logical command.
   */
  override def visitUseNamespace(ctx: UseNamespaceContext): LogicalPlan = withOrigin(ctx) {
    val nameParts = visitMultipartIdentifier(ctx.multipartIdentifier)
    SetNamespaceCommand(nameParts)
  }

  /**
   * Create a [[SetCatalogCommand]] logical command.
   */
  override def visitSetCatalog(ctx: SetCatalogContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.identifier() != null) {
      SetCatalogCommand(ctx.identifier().getText)
    } else if (ctx.STRING() != null) {
      SetCatalogCommand(string(ctx.STRING()))
    } else {
      throw new IllegalStateException("Invalid catalog name")
    }
  }

  /**
   * Create a [[ShowCatalogsCommand]] logical command.
   */
  override def visitShowCatalogs(ctx: ShowCatalogsContext) : LogicalPlan = withOrigin(ctx) {
    ShowCatalogsCommand(Option(ctx.pattern).map(string))
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
        throw QueryParsingErrors.createTempTableNotSpecifyProviderError(ctx))
      val schema = Option(ctx.createOrReplaceTableColTypeList()).map(createSchema)

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
      options = Option(ctx.propertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
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
   * Create a [[AddFilesCommand]], [[AddJarsCommand]], [[AddArchivesCommand]],
   * [[ListFilesCommand]], [[ListJarsCommand]] or [[ListArchivesCommand]]
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
    val rawArg = remainder(ctx.identifier).trim
    val maybePaths = strLiteralDef.findAllIn(rawArg).toSeq.map {
      case p if p.startsWith("\"") || p.startsWith("'") => unescapeSQLString(p)
      case p => p
    }

    ctx.op.getType match {
      case SqlBaseParser.ADD =>
        ctx.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "files" | "file" => AddFilesCommand(maybePaths)
          case "jars" | "jar" => AddJarsCommand(maybePaths)
          case "archives" | "archive" => AddArchivesCommand(maybePaths)
          case other => operationNotAllowed(s"ADD with resource type '$other'", ctx)
        }
      case SqlBaseParser.LIST =>
        ctx.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "files" | "file" =>
            if (maybePaths.length > 0) {
              ListFilesCommand(maybePaths)
            } else {
              ListFilesCommand()
            }
          case "jars" | "jar" =>
            if (maybePaths.length > 0) {
              ListJarsCommand(maybePaths)
            } else {
              ListJarsCommand()
            }
          case "archives" | "archive" =>
            if (maybePaths.length > 0) {
              ListArchivesCommand(maybePaths)
            } else {
              ListArchivesCommand()
            }
          case other => operationNotAllowed(s"LIST with resource type '$other'", ctx)
        }
      case _ => operationNotAllowed(s"Other types of operation on resources", ctx)
    }
  }


  /**
   * Create or replace a view. This creates a [[CreateViewCommand]].
   *
   * For example:
   * {{{
   *   CREATE [OR REPLACE] [[GLOBAL] TEMPORARY] VIEW [IF NOT EXISTS] multi_part_name
   *   [(column_name [COMMENT column_comment], ...) ]
   *   create_view_clauses
   *
   *   AS SELECT ...;
   *
   *   create_view_clauses (order insensitive):
   *     [COMMENT view_comment]
   *     [TBLPROPERTIES (property_name = property_value, ...)]
   * }}}
   */
  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    if (!ctx.identifierList.isEmpty) {
      operationNotAllowed("CREATE VIEW ... PARTITIONED ON", ctx)
    }

    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.PARTITIONED, "PARTITIONED ON", ctx)
    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)

    val userSpecifiedColumns = Option(ctx.identifierCommentList).toSeq.flatMap { icl =>
      icl.identifierComment.asScala.map { ic =>
        ic.identifier.getText -> Option(ic.commentSpec()).map(visitCommentSpec)
      }
    }

    if (ctx.EXISTS != null && ctx.REPLACE != null) {
      throw QueryParsingErrors.createViewWithBothIfNotExistsAndReplaceError(ctx)
    }

    val properties = ctx.propertyList.asScala.headOption.map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
    if (ctx.TEMPORARY != null && !properties.isEmpty) {
      operationNotAllowed("TBLPROPERTIES can't coexist with CREATE TEMPORARY VIEW", ctx)
    }

    val viewType = if (ctx.TEMPORARY == null) {
      PersistedView
    } else if (ctx.GLOBAL != null) {
      GlobalTempView
    } else {
      LocalTempView
    }
    if (viewType == PersistedView) {
      val originalText = source(ctx.query)
      assert(Option(originalText).isDefined,
        "'originalText' must be provided to create permanent view")
      CreateView(
        UnresolvedDBObjectName(
          visitMultipartIdentifier(ctx.multipartIdentifier),
          false),
        userSpecifiedColumns,
        visitCommentSpecList(ctx.commentSpec()),
        properties,
        Some(originalText),
        plan(ctx.query),
        ctx.EXISTS != null,
        ctx.REPLACE != null)
    } else {
      // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with
      // 'CREATE TEMPORARY TABLE'
      if (ctx.EXISTS != null) {
        throw QueryParsingErrors.defineTempViewWithIfNotExistsError(ctx)
      }

      val tableIdentifier = visitMultipartIdentifier(ctx.multipartIdentifier).asTableIdentifier
      if (tableIdentifier.database.isDefined) {
        // Temporary view names should NOT contain database prefix like "database.table"
        throw QueryParsingErrors
          .notAllowedToAddDBPrefixForTempViewError(tableIdentifier.database.get, ctx)
      }

      CreateViewCommand(
        tableIdentifier,
        userSpecifiedColumns,
        visitCommentSpecList(ctx.commentSpec()),
        properties,
        Option(source(ctx.query)),
        plan(ctx.query),
        ctx.EXISTS != null,
        ctx.REPLACE != null,
        viewType = viewType)
    }
  }

  /**
   * Create a [[CreateFunctionCommand]].
   *
   * For example:
   * {{{
   *   CREATE [OR REPLACE] [TEMPORARY] FUNCTION [IF NOT EXISTS] [db_name.]function_name
   *   AS class_name [USING JAR|FILE|ARCHIVE 'file_uri' [, JAR|FILE|ARCHIVE 'file_uri']];
   * }}}
   */
  override def visitCreateFunction(ctx: CreateFunctionContext): LogicalPlan = withOrigin(ctx) {
    val resources = ctx.resource.asScala.map { resource =>
      val resourceType = resource.identifier.getText.toLowerCase(Locale.ROOT)
      resourceType match {
        case "jar" | "file" | "archive" =>
          FunctionResource(FunctionResourceType.fromString(resourceType), string(resource.STRING))
        case other =>
          operationNotAllowed(s"CREATE FUNCTION with resource type '$resourceType'", ctx)
      }
    }

    if (ctx.EXISTS != null && ctx.REPLACE != null) {
      throw QueryParsingErrors.createFuncWithBothIfNotExistsAndReplaceError(ctx)
    }

    val functionIdentifier = visitMultipartIdentifier(ctx.multipartIdentifier)
    if (ctx.TEMPORARY == null) {
      CreateFunction(
        UnresolvedDBObjectName(
          functionIdentifier,
          isNamespace = false),
        string(ctx.className),
        resources.toSeq,
        ctx.EXISTS != null,
        ctx.REPLACE != null)
    } else {
      // Disallow to define a temporary function with `IF NOT EXISTS`
      if (ctx.EXISTS != null) {
        throw QueryParsingErrors.defineTempFuncWithIfNotExistsError(ctx)
      }

      if (functionIdentifier.length > 2) {
        throw QueryParsingErrors.unsupportedFunctionNameError(functionIdentifier, ctx)
      } else if (functionIdentifier.length == 2) {
        // Temporary function names should not contain database prefix like "database.function"
        throw QueryParsingErrors.specifyingDBInCreateTempFuncError(functionIdentifier.head, ctx)
      }
      CreateFunctionCommand(
        FunctionIdentifier(functionIdentifier.last),
        string(ctx.className),
        resources.toSeq,
        true,
        ctx.EXISTS != null,
        ctx.REPLACE != null)
    }
  }

  /**
   * Create a DROP FUNCTION statement.
   *
   * For example:
   * {{{
   *   DROP [TEMPORARY] FUNCTION [IF EXISTS] function;
   * }}}
   */
  override def visitDropFunction(ctx: DropFunctionContext): LogicalPlan = withOrigin(ctx) {
    val functionName = visitMultipartIdentifier(ctx.multipartIdentifier)
    val isTemp = ctx.TEMPORARY != null
    if (isTemp) {
      if (functionName.length > 1) {
        throw QueryParsingErrors.invalidNameForDropTempFunc(functionName, ctx)
      }
      DropFunctionCommand(
        identifier = FunctionIdentifier(functionName.head),
        ifExists = ctx.EXISTS != null,
        isTemp = true)
    } else {
      val hintStr = "Please use fully qualified identifier to drop the persistent function."
      DropFunction(
        UnresolvedFunc(
          functionName,
          "DROP FUNCTION",
          requirePersistent = true,
          funcTypeMismatchHint = Some(hintStr)),
        ctx.EXISTS != null)
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
    val serdeInfo = getSerdeInfo(
      ctx.rowFormat.asScala.toSeq, ctx.createFileFormat.asScala.toSeq, ctx)
    if (provider.isDefined && serdeInfo.isDefined) {
      operationNotAllowed(s"CREATE TABLE LIKE ... USING ... ${serdeInfo.get.describe}", ctx)
    }

    // For "CREATE TABLE dst LIKE src ROW FORMAT SERDE xxx" which doesn't specify the file format,
    // it's a bit weird to use the default file format, but it's also weird to get file format
    // from the source table while the serde class is user-specified.
    // Here we require both serde and format to be specified, to avoid confusion.
    serdeInfo match {
      case Some(SerdeInfo(storedAs, formatClasses, serde, _)) =>
        if (storedAs.isEmpty && formatClasses.isEmpty && serde.isDefined) {
          throw QueryParsingErrors.rowFormatNotUsedWithStoredAsError(ctx)
        }
      case _ =>
    }

    val storage = toStorageFormat(location, serdeInfo, ctx)
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val cleanedProperties = cleanTableProperties(ctx, properties)
    CreateTableLikeCommand(
      targetTable, sourceTable, storage, provider, cleanedProperties, ctx.EXISTS != null)
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
      throw QueryParsingErrors.useDefinedRecordReaderOrWriterClassesError(ctx)
    }

    if (!conf.getConf(CATALOG_IMPLEMENTATION).equals("hive")) {
      super.withScriptIOSchema(
        ctx,
        inRowFormat,
        recordWriter,
        outRowFormat,
        recordReader,
        schemaLess)
    } else {
      def format(
          fmt: RowFormatContext,
          configKey: String,
          defaultConfigValue: String): ScriptIOFormat = fmt match {
        case c: RowFormatDelimitedContext =>
          getRowFormatDelimited(c)

        case c: RowFormatSerdeContext =>
          // Use a serde format.
          val SerdeInfo(None, None, Some(name), props) = visitRowFormatSerde(c)

          // SPARK-10310: Special cases LazySimpleSerDe
          val recordHandler = if (name == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
            Option(conf.getConfString(configKey, defaultConfigValue))
          } else {
            None
          }
          val finalProps = props ++ Seq("field.delim" -> props.getOrElse("field.delim", "\t"))
          (Seq.empty, Option(name), finalProps.toSeq, recordHandler)

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
          inRowFormat, "hive.script.recordreader",
          "org.apache.hadoop.hive.ql.exec.TextRecordReader")

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
      throw QueryParsingErrors.directoryPathAndOptionsPathBothSpecifiedError(ctx)
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
          throw QueryParsingErrors.unsupportedLocalFileSchemeError(ctx)
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
