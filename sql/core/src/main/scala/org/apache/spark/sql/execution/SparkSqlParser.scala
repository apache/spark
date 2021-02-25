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

import java.util.Locale

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.StructType

/**
 * Concrete parser for Spark SQL statements.
 */
class SparkSqlParser(conf: SQLConf) extends AbstractSqlParser(conf) {
  val astBuilder = new SparkSqlAstBuilder(conf)

  private val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class SparkSqlAstBuilder(conf: SQLConf) extends AstBuilder(conf) {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  /**
   * Create a [[SetCommand]] logical plan.
   *
   * Note that we assume that everything after the SET keyword is assumed to be a part of the
   * key-value pair. The split between key and value is made by searching for the first `=`
   * character in the raw string.
   */
  override def visitSetConfiguration(ctx: SetConfigurationContext): LogicalPlan = withOrigin(ctx) {
    // Construct the command.
    val raw = remainder(ctx.SET.getSymbol)
    val keyValueSeparatorIndex = raw.indexOf('=')
    if (keyValueSeparatorIndex >= 0) {
      val key = raw.substring(0, keyValueSeparatorIndex).trim
      val value = raw.substring(keyValueSeparatorIndex + 1).trim
      SetCommand(Some(key -> Option(value)))
    } else if (raw.nonEmpty) {
      SetCommand(Some(raw.trim -> None))
    } else {
      SetCommand(None)
    }
  }

  /**
   * Create a [[ResetCommand]] logical plan.
   * Example SQL :
   * {{{
   *   RESET;
   * }}}
   */
  override def visitResetConfiguration(
      ctx: ResetConfigurationContext): LogicalPlan = withOrigin(ctx) {
    ResetCommand
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

      val (_, _, _, options, location, _) = visitCreateTableClauses(ctx.createTableClauses())
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
    ctx.constantList.asScala.map(visitConstantList)
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
    val mayebePaths = if (ctx.STRING != null) string(ctx.STRING) else remainder(ctx.identifier).trim
    ctx.op.getType match {
      case SqlBaseParser.ADD =>
        ctx.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "file" => AddFileCommand(mayebePaths)
          case "jar" => AddJarCommand(mayebePaths)
          case other => operationNotAllowed(s"ADD with resource type '$other'", ctx)
        }
      case SqlBaseParser.LIST =>
        ctx.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "files" | "file" =>
            if (mayebePaths.length > 0) {
              ListFilesCommand(mayebePaths.split("\\s+"))
            } else {
              ListFilesCommand()
            }
          case "jars" | "jar" =>
            if (mayebePaths.length > 0) {
              ListJarsCommand(mayebePaths.split("\\s+"))
            } else {
              ListJarsCommand()
            }
          case other => operationNotAllowed(s"LIST with resource type '$other'", ctx)
        }
      case _ => operationNotAllowed(s"Other types of operation on resources", ctx)
    }
  }

  /**
   * Create a Hive serde table, returning a [[CreateTable]] logical plan.
   *
   * This is a legacy syntax for Hive compatibility, we recommend users to use the Spark SQL
   * CREATE TABLE syntax to create Hive serde table, e.g. "CREATE TABLE ... USING hive ..."
   *
   * Note: several features are currently not supported - temporary tables, bucketing,
   * skewed columns and storage handlers (STORED BY).
   *
   * Expected format:
   * {{{
   *   CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
   *   [(col1[:] data_type [COMMENT col_comment], ...)]
   *   create_table_clauses
   *   [AS select_statement];
   *
   *   create_table_clauses (order insensitive):
   *     [COMMENT table_comment]
   *     [PARTITIONED BY (col2[:] data_type [COMMENT col_comment], ...)]
   *     [ROW FORMAT row_format]
   *     [STORED AS file_format]
   *     [LOCATION path]
   *     [TBLPROPERTIES (property_name=property_value, ...)]
   * }}}
   */
  override def visitCreateHiveTable(ctx: CreateHiveTableContext): LogicalPlan = withOrigin(ctx) {
    val (ident, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
    // TODO: implement temporary tables
    if (temp) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
          "Please use CREATE TEMPORARY VIEW as an alternative.", ctx)
    }
    if (ctx.skewSpec.size > 0) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
    }

    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)
    checkDuplicateClauses(ctx.PARTITIONED, "PARTITIONED BY", ctx)
    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.bucketSpec(), "CLUSTERED BY", ctx)
    checkDuplicateClauses(ctx.createFileFormat, "STORED AS/BY", ctx)
    checkDuplicateClauses(ctx.rowFormat, "ROW FORMAT", ctx)
    checkDuplicateClauses(ctx.locationSpec, "LOCATION", ctx)

    val dataCols = Option(ctx.columns).map(visitColTypeList).getOrElse(Nil)
    val partitionCols = Option(ctx.partitionColumns).map(visitColTypeList).getOrElse(Nil)
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val selectQuery = Option(ctx.query).map(plan)
    val bucketSpec = ctx.bucketSpec().asScala.headOption.map(visitBucketSpec)

    // Note: Hive requires partition columns to be distinct from the schema, so we need
    // to include the partition columns here explicitly
    val schema = StructType(dataCols ++ partitionCols)

    // Storage format
    val defaultStorage = HiveSerDe.getDefaultStorage(conf)
    validateRowFormatFileFormat(ctx.rowFormat.asScala, ctx.createFileFormat.asScala, ctx)
    val fileStorage = ctx.createFileFormat.asScala.headOption.map(visitCreateFileFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val rowStorage = ctx.rowFormat.asScala.headOption.map(visitRowFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val location = visitLocationSpecList(ctx.locationSpec())
    // If we are creating an EXTERNAL table, then the LOCATION field is required
    if (external && location.isEmpty) {
      operationNotAllowed("CREATE EXTERNAL TABLE must be accompanied by LOCATION", ctx)
    }

    val locUri = location.map(CatalogUtils.stringToURI(_))
    val storage = CatalogStorageFormat(
      locationUri = locUri,
      inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
      outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
      serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
      compressed = false,
      properties = rowStorage.properties ++ fileStorage.properties)
    // If location is defined, we'll assume this is an external table.
    // Otherwise, we may accidentally delete existing data.
    val tableType = if (external || location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    val name = tableIdentifier(ident, "CREATE TABLE ... STORED AS ...", ctx)

    // TODO support the sql text - have a proper location for this!
    val tableDesc = CatalogTable(
      identifier = name,
      tableType = tableType,
      storage = storage,
      schema = schema,
      bucketSpec = bucketSpec,
      provider = Some(DDLUtils.HIVE_PROVIDER),
      partitionColumnNames = partitionCols.map(_.name),
      properties = properties,
      comment = visitCommentSpecList(ctx.commentSpec()))

    val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

    selectQuery match {
      case Some(q) =>
        // Don't allow explicit specification of schema for CTAS.
        if (dataCols.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement",
            ctx)
        }

        // When creating partitioned table with CTAS statement, we can't specify data type for the
        // partition columns.
        if (partitionCols.nonEmpty) {
          val errorMessage = "Create Partitioned Table As Select cannot specify data type for " +
            "the partition columns of the target table."
          operationNotAllowed(errorMessage, ctx)
        }

        // Hive CTAS supports dynamic partition by specifying partition column names.
        val partitionColumnNames =
          Option(ctx.partitionColumnNames)
            .map(visitIdentifierList(_).toArray)
            .getOrElse(Array.empty[String])

        val tableDescWithPartitionColNames =
          tableDesc.copy(partitionColumnNames = partitionColumnNames)

        val hasStorageProperties = (ctx.createFileFormat.size != 0) || (ctx.rowFormat.size != 0)
        if (conf.convertCTAS && !hasStorageProperties) {
          // At here, both rowStorage.serdeProperties and fileStorage.serdeProperties
          // are empty Maps.
          val newTableDesc = tableDescWithPartitionColNames.copy(
            storage = CatalogStorageFormat.empty.copy(locationUri = locUri),
            provider = Some(conf.defaultDataSourceName))
          CreateTable(newTableDesc, mode, Some(q))
        } else {
          CreateTable(tableDescWithPartitionColNames, mode, Some(q))
        }
      case None =>
        // When creating partitioned table, we must specify data type for the partition columns.
        if (Option(ctx.partitionColumnNames).isDefined) {
          val errorMessage = "Must specify a data type for each partition column while creating " +
            "Hive partitioned table."
          operationNotAllowed(errorMessage, ctx)
        }

        CreateTable(tableDesc, mode, None)
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
    // rowStorage used to determine CatalogStorageFormat.serde and
    // CatalogStorageFormat.properties in STORED AS clause.
    val rowStorage = ctx.rowFormat.asScala.headOption.map(visitRowFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val fileFormat = ctx.createFileFormat.asScala.headOption.map(visitCreateFileFormat) match {
      case Some(f) =>
        if (provider.isDefined) {
          throw new ParseException("'STORED AS hiveFormats' and 'USING provider' " +
            "should not be specified both", ctx)
        }
        f.copy(
          locationUri = location.map(CatalogUtils.stringToURI),
          serde = rowStorage.serde.orElse(f.serde),
          properties = rowStorage.properties ++ f.properties)
      case None =>
        if (rowStorage.serde.isDefined) {
          throw new ParseException("'ROW FORMAT' must be used with 'STORED AS'", ctx)
        }
        CatalogStorageFormat.empty.copy(locationUri = location.map(CatalogUtils.stringToURI))
    }
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    CreateTableLikeCommand(
      targetTable, sourceTable, fileFormat, provider, properties, ctx.EXISTS != null)
  }

  /**
   * Create a [[CatalogStorageFormat]] for creating tables.
   *
   * Format: STORED AS ...
   */
  override def visitCreateFileFormat(
      ctx: CreateFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    (ctx.fileFormat, ctx.storageHandler) match {
      // Expected format: INPUTFORMAT input_format OUTPUTFORMAT output_format
      case (c: TableFileFormatContext, null) =>
        visitTableFileFormat(c)
      // Expected format: SEQUENCEFILE | TEXTFILE | RCFILE | ORC | PARQUET | AVRO
      case (c: GenericFileFormatContext, null) =>
        visitGenericFileFormat(c)
      case (null, storageHandler) =>
        operationNotAllowed("STORED BY", ctx)
      case _ =>
        throw new ParseException("Expected either STORED AS or STORED BY, not both", ctx)
    }
  }

  /**
   * Create a [[CatalogStorageFormat]].
   */
  override def visitTableFileFormat(
      ctx: TableFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    CatalogStorageFormat.empty.copy(
      inputFormat = Option(string(ctx.inFmt)),
      outputFormat = Option(string(ctx.outFmt)))
  }

  /**
   * Resolve a [[HiveSerDe]] based on the name given and return it as a [[CatalogStorageFormat]].
   */
  override def visitGenericFileFormat(
      ctx: GenericFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    val source = ctx.identifier.getText
    HiveSerDe.sourceToSerDe(source) match {
      case Some(s) =>
        CatalogStorageFormat.empty.copy(
          inputFormat = s.inputFormat,
          outputFormat = s.outputFormat,
          serde = s.serde)
      case None =>
        operationNotAllowed(s"STORED AS with file format '$source'", ctx)
    }
  }

  /**
   * Create a [[CatalogStorageFormat]] used for creating tables.
   *
   * Example format:
   * {{{
   *   SERDE serde_name [WITH SERDEPROPERTIES (k1=v1, k2=v2, ...)]
   * }}}
   *
   * OR
   *
   * {{{
   *   DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
   *   [COLLECTION ITEMS TERMINATED BY char]
   *   [MAP KEYS TERMINATED BY char]
   *   [LINES TERMINATED BY char]
   *   [NULL DEFINED AS char]
   * }}}
   */
  private def visitRowFormat(ctx: RowFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    ctx match {
      case serde: RowFormatSerdeContext => visitRowFormatSerde(serde)
      case delimited: RowFormatDelimitedContext => visitRowFormatDelimited(delimited)
    }
  }

  /**
   * Create SERDE row format name and properties pair.
   */
  override def visitRowFormatSerde(
      ctx: RowFormatSerdeContext): CatalogStorageFormat = withOrigin(ctx) {
    import ctx._
    CatalogStorageFormat.empty.copy(
      serde = Option(string(name)),
      properties = Option(tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Create a delimited row format properties object.
   */
  override def visitRowFormatDelimited(
      ctx: RowFormatDelimitedContext): CatalogStorageFormat = withOrigin(ctx) {
    // TODO we need proper support for the NULL format.
    val entries =
      entry("field.delim", ctx.fieldsTerminatedBy) ++
        entry("serialization.format", ctx.fieldsTerminatedBy) ++
        entry("escape.delim", ctx.escapedBy) ++
        // The following typo is inherited from Hive...
        entry("colelction.delim", ctx.collectionItemsTerminatedBy) ++
        entry("mapkey.delim", ctx.keysTerminatedBy) ++
        Option(ctx.linesSeparatedBy).toSeq.map { token =>
          val value = string(token)
          validate(
            value == "\n",
            s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
            ctx)
          "line.delim" -> value
        }
    CatalogStorageFormat.empty.copy(properties = entries.toMap)
  }

  /**
   * Throw a [[ParseException]] if the user specified incompatible SerDes through ROW FORMAT
   * and STORED AS.
   *
   * The following are allowed. Anything else is not:
   *   ROW FORMAT SERDE ... STORED AS [SEQUENCEFILE | RCFILE | TEXTFILE]
   *   ROW FORMAT DELIMITED ... STORED AS TEXTFILE
   *   ROW FORMAT ... STORED AS INPUTFORMAT ... OUTPUTFORMAT ...
   */
  private def validateRowFormatFileFormat(
      rowFormatCtx: RowFormatContext,
      createFileFormatCtx: CreateFileFormatContext,
      parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx == null || createFileFormatCtx == null) {
      return
    }
    (rowFormatCtx, createFileFormatCtx.fileFormat) match {
      case (_, ffTable: TableFileFormatContext) => // OK
      case (rfSerde: RowFormatSerdeContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
          case ("sequencefile" | "textfile" | "rcfile") => // OK
          case fmt =>
            operationNotAllowed(
              s"ROW FORMAT SERDE is incompatible with format '$fmt', which also specifies a serde",
              parentCtx)
        }
      case (rfDelimited: RowFormatDelimitedContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
          case "textfile" => // OK
          case fmt => operationNotAllowed(
            s"ROW FORMAT DELIMITED is only compatible with 'textfile', not '$fmt'", parentCtx)
        }
      case _ =>
        // should never happen
        def str(ctx: ParserRuleContext): String = {
          (0 until ctx.getChildCount).map { i => ctx.getChild(i).getText }.mkString(" ")
        }
        operationNotAllowed(
          s"Unexpected combination of ${str(rowFormatCtx)} and ${str(createFileFormatCtx)}",
          parentCtx)
    }
  }

  private def validateRowFormatFileFormat(
      rowFormatCtx: Seq[RowFormatContext],
      createFileFormatCtx: Seq[CreateFileFormatContext],
      parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx.size == 1 && createFileFormatCtx.size == 1) {
      validateRowFormatFileFormat(rowFormatCtx.head, createFileFormatCtx.head, parentCtx)
    }
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
          entry("TOK_TABLEROWFORMATLINES", c.linesSeparatedBy) ++
          entry("TOK_TABLEROWFORMATNULL", c.nullDefinedAs)

        (entries, None, Seq.empty, None)

      case c: RowFormatSerdeContext =>
        // Use a serde format.
        val CatalogStorageFormat(None, None, None, Some(name), _, props) = visitRowFormatSerde(c)

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
        val props = Seq("field.delim" -> "\t")
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
    RepartitionByExpression(expressions, query, conf.numShufflePartitions)
  }

  /**
   * Return the parameters for [[InsertIntoDir]] logical plan.
   *
   * Expected format:
   * {{{
   *   INSERT OVERWRITE DIRECTORY
   *   [path]
   *   [OPTIONS table_property_list]
   *   select_statement;
   * }}}
   */
  override def visitInsertOverwriteDir(
      ctx: InsertOverwriteDirContext): InsertDirParams = withOrigin(ctx) {
    if (ctx.LOCAL != null) {
      throw new ParseException(
        "LOCAL is not supported in INSERT OVERWRITE DIRECTORY to data source", ctx)
    }

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
    validateRowFormatFileFormat(ctx.rowFormat, ctx.createFileFormat, ctx)
    val rowStorage = Option(ctx.rowFormat).map(visitRowFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val fileStorage = Option(ctx.createFileFormat).map(visitCreateFileFormat)
      .getOrElse(CatalogStorageFormat.empty)

    val path = string(ctx.path)
    // The path field is required
    if (path.isEmpty) {
      operationNotAllowed("INSERT OVERWRITE DIRECTORY must be accompanied by path", ctx)
    }

    val defaultStorage = HiveSerDe.getDefaultStorage(conf)

    val storage = CatalogStorageFormat(
      locationUri = Some(CatalogUtils.stringToURI(path)),
      inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
      outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
      serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
      compressed = false,
      properties = rowStorage.properties ++ fileStorage.properties)

    (ctx.LOCAL != null, storage, Some(DDLUtils.HIVE_PROVIDER))
  }
}
