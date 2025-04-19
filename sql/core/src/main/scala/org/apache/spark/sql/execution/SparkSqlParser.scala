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

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, GlobalTempView, LocalTempView, PersistedView, PlanWithUnresolvedIdentifier, SchemaEvolution, SchemaTypeEvolution, UnresolvedAttribute, UnresolvedFunctionName, UnresolvedIdentifier, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryParsingErrors}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils.getUriBuilder

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
  private val configKeyDef = """([a-zA-Z_\d\\.:]+)\s*$""".r
  private val configValueDef = """([^;]*);*""".r
  private val strLiteralDef = """(".*?[^\\]"|'.*?[^\\]'|[^ \n\r\t"']+)""".r

  private def withCatalogIdentClause(
      ctx: CatalogIdentifierReferenceContext,
      builder: Seq[String] => LogicalPlan): LogicalPlan = {
    val exprCtx = ctx.expression
    if (exprCtx != null) {
      // resolve later in analyzer
      PlanWithUnresolvedIdentifier(withOrigin(exprCtx) { expression(exprCtx) }, Nil,
        (ident, _) => builder(ident))
    } else if (ctx.errorCapturingIdentifier() != null) {
      // resolve immediately
      builder.apply(Seq(ctx.errorCapturingIdentifier().getText))
    } else if (ctx.stringLit() != null) {
      // resolve immediately
      builder.apply(Seq(string(visitStringLit(ctx.stringLit()))))
    } else {
      throw SparkException.internalError("Invalid catalog name")
    }
  }

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
      remainder(ctx.SET.getSymbol).trim.replaceAll(";+$", "") match {
        case configKeyValueDef(key, value) =>
          SetCommand(Some(key -> Option(value.trim)))
        case configKeyDef(key) =>
          SetCommand(Some(key -> None))
        case s if s.trim == "-v" =>
          SetCommand(Some("-v" -> None))
        case s if s.trim.isEmpty =>
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
    remainder(ctx.RESET.getSymbol).trim.replaceAll(";+$", "") match {
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
      if (interval.months != 0) {
        throw QueryParsingErrors.intervalValueOutOfRangeError(
          toSQLValue(interval.months),
          ctx.interval()
        )
      }
      else if (interval.days != 0) {
        throw QueryParsingErrors.intervalValueOutOfRangeError(
          toSQLValue(interval.days),
          ctx.interval()
        )
      }
      else if (math.abs(interval.microseconds) > 18 * DateTimeConstants.MICROS_PER_HOUR) {
        throw QueryParsingErrors.intervalValueOutOfRangeError(
          toSQLValue((math.abs(interval.microseconds) / DateTimeConstants.MICROS_PER_HOUR).toInt),
          ctx.interval()
        )
      }
      else if (interval.microseconds % DateTimeConstants.MICROS_PER_SECOND != 0) {
        throw QueryParsingErrors.intervalValueOutOfRangeError(
          toSQLValue((interval.microseconds / DateTimeConstants.MICROS_PER_SECOND).toInt),
          ctx.interval()
        )
      } else {
        val seconds = (interval.microseconds / DateTimeConstants.MICROS_PER_SECOND).toInt
        SetCommand(Some(key -> Some(ZoneOffset.ofTotalSeconds(seconds).toString)))
      }
    } else if (ctx.timezone != null) {
      SetCommand(Some(key -> Some(visitTimezone(ctx.timezone()))))
    } else {
      throw QueryParsingErrors.invalidTimeZoneDisplacementValueError(ctx)
    }
  }

  override def visitTimezone (ctx: TimezoneContext): String = {
    if (ctx.stringLit() != null) {
      string(visitStringLit(ctx.stringLit()))
    } else {
      TimeZone.getDefault.getID
    }
  }

  /**
   * Create a [[RefreshResource]] logical plan.
   */
  override def visitRefreshResource(ctx: RefreshResourceContext): LogicalPlan = withOrigin(ctx) {
    val path = if (ctx.stringLit != null) {
      string(visitStringLit(ctx.stringLit))
    } else {
      extractUnquotedResourcePath(ctx)
    }
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
      invalidStatement("EXPLAIN LOGICAL", ctx)
    }

    val statement = plan(Option(ctx.statement()).getOrElse(ctx.setResetStatement()))
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
    withIdentClause(ctx.identifierReference, SetNamespaceCommand(_))
  }

  /**
   * Create a [[SetCatalogCommand]] logical command.
   */
  override def visitSetCatalog(ctx: SetCatalogContext): LogicalPlan = withOrigin(ctx) {
    withCatalogIdentClause(ctx.catalogIdentifierReference, identifiers => {
      if (identifiers.size > 1) {
        // can occur when user put multipart string in IDENTIFIER(...) clause
        throw QueryParsingErrors.invalidNameForSetCatalog(identifiers, ctx)
      }
      SetCatalogCommand(identifiers.head)
    })
  }

  /**
   * Create a [[ShowCatalogsCommand]] logical command.
   */
  override def visitShowCatalogs(ctx: ShowCatalogsContext) : LogicalPlan = withOrigin(ctx) {
    ShowCatalogsCommand(Option(ctx.pattern).map(x => string(visitStringLit(x))))
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
    val (identCtx, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)

    if (!temp || ctx.query != null) {
      super.visitCreateTable(ctx)
    } else {
      if (external) {
        invalidStatement("CREATE EXTERNAL TABLE ... USING", ctx)
      }
      if (ifNotExists) {
        // Unlike CREATE TEMPORARY VIEW USING, CREATE TEMPORARY TABLE USING does not support
        // IF NOT EXISTS. Users are not allowed to replace the existing temp table.
        invalidStatement("CREATE TEMPORARY TABLE IF NOT EXISTS", ctx)
      }

      val (_, _, _, _, options, location, _, _, _, _) =
        visitCreateTableClauses(ctx.createTableClauses())
      val provider = Option(ctx.tableProvider).map(_.multipartIdentifier.getText).getOrElse(
        throw QueryParsingErrors.createTempTableNotSpecifyProviderError(ctx))
      val schema = Option(ctx.tableElementList()).map(createSchema)

      logWarning(s"CREATE TEMPORARY TABLE ... USING ... is deprecated, please use " +
          "CREATE TEMPORARY VIEW ... USING ... instead")

      withIdentClause(identCtx, ident => {
        val table = tableIdentifier(ident, "CREATE TEMPORARY VIEW", ctx)
        val optionsList: Map[String, String] =
          options.options.map { case (key, value) =>
            val newValue: String =
              if (value == null) {
                null
              } else value match {
                case Literal(_, _: StringType) => value.toString
                case _ => throw QueryCompilationErrors.optionMustBeLiteralString(key)
              }
            (key, newValue)
          }.toMap
        val optionsWithLocation =
          location.map(l => optionsList + ("path" -> l)).getOrElse(optionsList)
        CreateTempViewUsing(table, schema, replace = false, global = false, provider,
          optionsWithLocation)
      })
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
    ctx.constant.asScala.map(v => visitStringConstant(v)).toSeq
  }

  /**
   * Fail an unsupported Hive native command (SET ROLE is handled separately).
   */
  override def visitFailNativeCommand(
      ctx: FailNativeCommandContext): LogicalPlan = withOrigin(ctx) {
    val keywords = ctx.unsupportedHiveNativeCommands.children.asScala.collect {
      case n: TerminalNode => n.getText
    }.mkString(" ")
    invalidStatement(keywords, ctx)
  }

  override def visitFailSetRole(ctx: FailSetRoleContext): LogicalPlan = withOrigin(ctx) {
    invalidStatement("SET ROLE", ctx);
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
   *     [DEFAULT COLLATION collation_name]
   *     [TBLPROPERTIES (property_name = property_value, ...)]
   * }}}
   */
  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    if (!ctx.identifierList.isEmpty) {
      invalidStatement("CREATE VIEW ... PARTITIONED ON", ctx)
    }

    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.collationSpec(), "DEFAULT COLLATION", ctx)
    checkDuplicateClauses(ctx.schemaBinding(), "WITH SCHEMA", ctx)
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

    if (ctx.TEMPORARY != null && ctx.schemaBinding(0) != null) {
      throw QueryParsingErrors.temporaryViewWithSchemaBindingMode(ctx)
    }

    val viewType = if (ctx.TEMPORARY == null) {
      PersistedView
    } else if (ctx.GLOBAL != null) {
      GlobalTempView
    } else {
      LocalTempView
    }
    val qPlan: LogicalPlan = plan(ctx.query)

    // Disallow parameter markers in the query of the view.
    // We need this limitation because we store the original query text, pre substitution.
    // To lift this we would need to reconstitute the query with parameter markers replaced with the
    // values given at CREATE VIEW time, or we would need to store the parameter values alongside
    // the text.
    // The same rule can be found in CACHE TABLE builder.
    checkInvalidParameter(qPlan, "the query of CREATE VIEW")
    if (viewType == PersistedView) {
      val originalText = source(ctx.query)
      assert(Option(originalText).isDefined,
        "'originalText' must be provided to create permanent view")
      val schemaBinding = visitSchemaBinding(ctx.schemaBinding(0))
      val finalSchemaBinding =
        if (schemaBinding == SchemaEvolution && userSpecifiedColumns.nonEmpty) {
          SchemaTypeEvolution
        } else {
          schemaBinding
        }
      CreateView(
        withIdentClause(ctx.identifierReference(), UnresolvedIdentifier(_)),
        userSpecifiedColumns,
        visitCommentSpecList(ctx.commentSpec()),
        visitCollationSpecList(ctx.collationSpec()),
        properties,
        Some(originalText),
        qPlan,
        ctx.EXISTS != null,
        ctx.REPLACE != null,
        finalSchemaBinding)
    } else {
      // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with
      // 'CREATE TEMPORARY TABLE'
      if (ctx.EXISTS != null) {
        throw QueryParsingErrors.defineTempViewWithIfNotExistsError(ctx)
      }

      withIdentClause(ctx.identifierReference(), Seq(qPlan), (ident, otherPlans) => {
        val tableIdentifier = ident.asTableIdentifier
        if (tableIdentifier.database.isDefined) {
          // Temporary view names should NOT contain database prefix like "database.table"
          throw QueryParsingErrors
            .notAllowedToAddDBPrefixForTempViewError(tableIdentifier.nameParts, ctx)
        }

        CreateViewCommand(
          tableIdentifier,
          userSpecifiedColumns,
          visitCommentSpecList(ctx.commentSpec()),
          visitCollationSpecList(ctx.collationSpec()),
          properties,
          Option(source(ctx.query)),
          otherPlans.head,
          ctx.EXISTS != null,
          ctx.REPLACE != null,
          viewType = viewType)
      })
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
          FunctionResource(FunctionResourceType.fromString(resourceType),
            string(visitStringLit(resource.stringLit())))
        case other =>
          operationNotAllowed(s"CREATE FUNCTION with resource type '$resourceType'", ctx)
      }
    }

    if (ctx.EXISTS != null && ctx.REPLACE != null) {
      throw QueryParsingErrors.createFuncWithBothIfNotExistsAndReplaceError(ctx)
    }

    withIdentClause(ctx.identifierReference(), functionIdentifier => {
      if (ctx.TEMPORARY == null) {
        CreateFunction(
          UnresolvedIdentifier(functionIdentifier),
          string(visitStringLit(ctx.className)),
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
          string(visitStringLit(ctx.className)),
          resources.toSeq,
          true,
          ctx.EXISTS != null,
          ctx.REPLACE != null)
      }
    })
  }

  /**
   * Create a [[CreateUserDefinedFunctionCommand]].
   *
   * For example:
   * {{{
   *   CREATE [OR REPLACE] [TEMPORARY] FUNCTION [IF NOT EXISTS] [db_name.]function_name
   *   ([param_name param_type [COMMENT param_comment], ...])
   *   RETURNS {ret_type | TABLE (ret_name ret_type [COMMENT ret_comment], ...])}
   *   [routine_characteristic]
   *   RETURN {expression | query };
   *
   *   routine_characteristic
   *   { LANGUAGE {SQL | IDENTIFIER} |
   *     [NOT] DETERMINISTIC |
   *     COMMENT function_comment |
   *     [CONTAINS SQL | READS SQL DATA] }
   * }}}
   */
  override def visitCreateUserDefinedFunction(ctx: CreateUserDefinedFunctionContext): LogicalPlan =
    withOrigin(ctx) {
      assert(ctx.expression != null || ctx.query != null)

      if (ctx.EXISTS != null && ctx.REPLACE != null) {
        throw QueryParsingErrors.createFuncWithBothIfNotExistsAndReplaceError(ctx)
      }

      // Reject invalid options
      for {
        parameters <- Option(ctx.parameters)
        colDefinition <- parameters.colDefinition().asScala
        option <- colDefinition.colDefinitionOption().asScala
      } {
        if (option.generationExpression() != null) {
          throw QueryParsingErrors.createFuncWithGeneratedColumnsError(ctx.parameters)
        }
        if (option.columnConstraintDefinition() != null) {
          throw QueryParsingErrors.createFuncWithConstraintError(ctx.parameters)
        }
      }

      val inputParamText = Option(ctx.parameters).map(source)
      val returnTypeText: String =
        if (ctx.RETURNS != null &&
          (Option(ctx.dataType).nonEmpty || Option(ctx.returnParams).nonEmpty)) {
          source(Option(ctx.dataType).getOrElse(ctx.returnParams))
        } else {
          ""
        }
      val exprText = Option(ctx.expression()).map(source)
      val queryText = Option(ctx.query()).map(source)

      val (containsSQL, deterministic, comment, optionalLanguage) =
        visitRoutineCharacteristics(ctx.routineCharacteristics())
      val language: RoutineLanguage = optionalLanguage.getOrElse(LanguageSQL)
      val isTableFunc = ctx.TABLE() != null || returnTypeText.equalsIgnoreCase("table")

      withIdentClause(ctx.identifierReference(), functionIdentifier => {
        if (ctx.TEMPORARY == null) {
          CreateUserDefinedFunction(
            UnresolvedIdentifier(functionIdentifier),
            inputParamText,
            returnTypeText,
            exprText,
            queryText,
            comment,
            deterministic,
            containsSQL,
            language,
            isTableFunc,
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

          CreateUserDefinedFunctionCommand(
            functionIdentifier.asFunctionIdentifier,
            inputParamText,
            returnTypeText,
            exprText,
            queryText,
            comment,
            deterministic,
            containsSQL,
            language,
            isTableFunc,
            isTemp = true,
            ctx.EXISTS != null,
            ctx.REPLACE != null
          )
        }
      })
    }

  /**
   * SQL function routine characteristics.
   * Currently only deterministic clause and comment clause are used.
   *
   * routine language: [LANGUAGE SQL | IDENTIFIER]
   * specific name: [SPECIFIC specific_name]
   * routine data access: [NO SQL | CONTAINS SQL | READS SQL DATA | MODIFIES SQL DATA]
   * routine null call: [RETURNS NULL ON NULL INPUT | CALLED ON NULL INPUT]
   * routine determinism: [DETERMINISTIC | NOT DETERMINISTIC]
   * comment: [COMMENT function_comment]
   * rights: [SQL SECURITY INVOKER | SQL SECURITY DEFINER]
   */
  override def visitRoutineCharacteristics(ctx: RoutineCharacteristicsContext)
  : (Option[Boolean], Option[Boolean], Option[String], Option[RoutineLanguage]) =
    withOrigin(ctx) {
      checkDuplicateClauses(ctx.routineLanguage(), "LANGUAGE", ctx)
      checkDuplicateClauses(ctx.specificName(), "SPECIFIC", ctx)
      checkDuplicateClauses(ctx.sqlDataAccess(), "SQL DATA ACCESS", ctx)
      checkDuplicateClauses(ctx.nullCall(), "NULL CALL", ctx)
      checkDuplicateClauses(ctx.deterministic(), "DETERMINISTIC", ctx)
      checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
      checkDuplicateClauses(ctx.rightsClause(), "SQL SECURITY RIGHTS", ctx)

      val language: Option[RoutineLanguage] = ctx
        .routineLanguage()
        .asScala
        .headOption
        .map(x => {
          if (x.SQL() != null) {
            LanguageSQL
          } else {
            val name: String = x.IDENTIFIER().getText()
            operationNotAllowed(s"Unsupported language for user defined functions: $name", x)
          }
        })

      val deterministic = ctx.deterministic().asScala.headOption.map(visitDeterminism)
      val comment = visitCommentSpecList(ctx.commentSpec())

      ctx.specificName().asScala.headOption.foreach(checkSpecificName)
      ctx.nullCall().asScala.headOption.foreach(checkNullCall)
      ctx.rightsClause().asScala.headOption.foreach(checkRightsClause)
      val containsSQL: Option[Boolean] =
        ctx.sqlDataAccess().asScala.headOption.map(visitDataAccess)
      (containsSQL, deterministic, comment, language)
    }

  /**
   * Check if the function has a SPECIFIC name,
   * which is a way to provide an alternative name for the function.
   * This check applies for all user defined functions.
   * Use functionName to specify the function that is currently checked.
   */
  private def checkSpecificName(ctx: SpecificNameContext): Unit =
    withOrigin(ctx) {
      operationNotAllowed(s"SQL function with SPECIFIC name is not supported", ctx)
    }

  private def checkNullCall(ctx: NullCallContext): Unit = withOrigin(ctx) {
    if (ctx.RETURNS() != null) {
      operationNotAllowed("SQL function with RETURNS NULL ON NULL INPUT is not supported", ctx)
    }
  }

  /**
   * Check SQL function data access clause. Currently only READS SQL DATA and CONTAINS SQL
   * are supported. Return true if the data access routine is CONTAINS SQL.
   */
  private def visitDataAccess(ctx: SqlDataAccessContext): Boolean = withOrigin(ctx) {
    if (ctx.NO() != null) {
      operationNotAllowed("SQL function with NO SQL is not supported", ctx)
    }
    if (ctx.MODIFIES() != null) {
      operationNotAllowed("SQL function with MODIFIES SQL DATA is not supported", ctx)
    }
    return ctx.READS() == null
  }

  private def checkRightsClause(ctx: RightsClauseContext): Unit = withOrigin(ctx) {
    if (ctx.INVOKER() != null) {
      operationNotAllowed("SQL function with SQL SECURITY INVOKER is not supported", ctx)
    }
  }

  private def visitDeterminism(ctx: DeterministicContext): Boolean = withOrigin(ctx) {
    blockBang(ctx.errorCapturingNot())
    ctx.errorCapturingNot() == null
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
    withIdentClause(ctx.identifierReference(), functionName => {
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
          UnresolvedFunctionName(
            functionName,
            "DROP FUNCTION",
            requirePersistent = true,
            funcTypeMismatchHint = Some(hintStr)),
          ctx.EXISTS != null)
      }
    })
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
      invalidStatement(s"CREATE TABLE LIKE ... USING ... ${serdeInfo.get.describe}", ctx)
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

      // The Writer uses inFormat to feed input data into the running script and
      // the reader uses outFormat to read the output from the running script,
      // this behavior is same with hive.
      val (inFormat, inSerdeClass, inSerdeProps, writer) =
        format(
          inRowFormat, "hive.script.recordwriter",
          "org.apache.hadoop.hive.ql.exec.TextRecordWriter")

      val (outFormat, outSerdeClass, outSerdeProps, reader) =
        format(
          outRowFormat, "hive.script.recordreader",
          "org.apache.hadoop.hive.ql.exec.TextRecordReader")

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

    val path = Option(ctx.path).map(x => string(visitStringLit(x))).getOrElse("")

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
          throw QueryParsingErrors.unsupportedLocalFileSchemeError(ctx, pathScheme)
        case _ =>
          // force scheme to be file rather than fs.default.name
          val loc = Some(getUriBuilder(CatalogUtils.stringToURI(path)).scheme("file").build())
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
    val path = string(visitStringLit(ctx.path))
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

  /**
   * Create a [[UnsetNamespacePropertiesCommand]] command.
   *
   * Expected format:
   * {{{
   *   ALTER (DATABASE|SCHEMA|NAMESPACE) database
   *   UNSET (DBPROPERTIES | PROPERTIES) ('key1', 'key2');
   * }}}
   */
  override def visitUnsetNamespaceProperties(
      ctx: UnsetNamespacePropertiesContext): LogicalPlan = withOrigin(ctx) {
    val properties = visitPropertyKeys(ctx.propertyList)
    val cleanedProperties = cleanNamespaceProperties(properties.map(_ -> "").toMap, ctx).keys.toSeq
    UnsetNamespacePropertiesCommand(
      withIdentClause(ctx.identifierReference(), UnresolvedNamespace(_)),
      cleanedProperties)
  }

  /**
   * Create a [[DescribeColumn]] or [[DescribeRelation]] or [[DescribeRelationAsJsonCommand]]
   * command.
   */
  override def visitDescribeRelation(ctx: DescribeRelationContext): LogicalPlan = withOrigin(ctx) {
    val isExtended = ctx.EXTENDED != null || ctx.FORMATTED != null
    val asJson = ctx.JSON != null
    if (asJson && !isExtended) {
      val tableName = ctx.identifierReference.getText.split("\\.").lastOption.getOrElse("table")
      throw QueryCompilationErrors.describeJsonNotExtendedError(tableName)
    }
    val relation = createUnresolvedTableOrView(ctx.identifierReference, "DESCRIBE TABLE")
    if (ctx.describeColName != null) {
      if (ctx.partitionSpec != null) {
        throw QueryParsingErrors.descColumnForPartitionUnsupportedError(ctx)
      } else if (asJson) {
        throw QueryCompilationErrors.describeColJsonUnsupportedError()
      } else {
        DescribeColumn(
          relation,
          UnresolvedAttribute(ctx.describeColName.nameParts.asScala.map(_.getText).toSeq),
          isExtended)
      }
    } else {
      val partitionSpec = if (ctx.partitionSpec != null) {
        // According to the syntax, visitPartitionSpec returns `Map[String, Option[String]]`.
        visitPartitionSpec(ctx.partitionSpec).map {
          case (key, Some(value)) => key -> value
          case (key, _) =>
            throw QueryParsingErrors.emptyPartitionKeyError(key, ctx.partitionSpec)
        }
      } else {
        Map.empty[String, String]
      }
      if (asJson) {
        DescribeRelationJsonCommand(relation, partitionSpec, isExtended)
      } else {
        DescribeRelation(relation, partitionSpec, isExtended)
      }
    }
  }

  override def visitShowProcedures(ctx: ShowProceduresContext): LogicalPlan = withOrigin(ctx) {
    val ns = if (ctx.identifierReference != null) {
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_))
    } else {
      CurrentNamespace
    }
    ShowProceduresCommand(ns)
  }

  override def visitShowNamespaces(ctx: ShowNamespacesContext): LogicalPlan = withOrigin(ctx) {
    val multiPart = Option(ctx.multipartIdentifier).map(visitMultipartIdentifier)
    ShowNamespacesCommand(
      UnresolvedNamespace(multiPart.getOrElse(Seq.empty[String])),
      Option(ctx.pattern).map(x => string(visitStringLit(x))))
  }

  override def visitDescribeProcedure(
      ctx: DescribeProcedureContext): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx.identifierReference(), procIdentifier =>
      DescribeProcedureCommand(UnresolvedIdentifier(procIdentifier)))
  }
}
