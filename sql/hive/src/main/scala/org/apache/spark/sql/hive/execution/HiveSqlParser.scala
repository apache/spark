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
package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.parse.EximUtil
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{CreateTable, CreateTableLike}
import org.apache.spark.sql.hive.{CreateTableAsSelect => CTAS, CreateViewAsSelect => CreateView, HiveSerDe}
import org.apache.spark.sql.hive.{HiveGenericUDTF, HiveMetastoreTypes, HiveSerDe}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper

/**
 * Concrete parser for HiveQl statements.
 */
class HiveSqlParser(hiveConf: HiveConf) extends AbstractSqlParser {
  val astBuilder = new HiveSqlAstBuilder(hiveConf)

  override protected def nativeCommand(sqlText: String): LogicalPlan = {
    HiveNativeCommand(sqlText)
  }
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class HiveSqlAstBuilder(hiveConf: HiveConf) extends SparkSqlAstBuilder {
  import ParserUtils._

  /**
   * Pass a command to Hive using a [[HiveNativeCommand]].
   */
  override def visitExecuteNativeCommand(
      ctx: ExecuteNativeCommandContext): LogicalPlan = withOrigin(ctx) {
    HiveNativeCommand(command(ctx))
  }

  /**
   * Fail an unsupported Hive native command.
   */
  override def visitFailNativeCommand(
      ctx: FailNativeCommandContext): LogicalPlan = withOrigin(ctx) {
    val keywords = if (ctx.kws != null) {
      Seq(ctx.kws.kw1, ctx.kws.kw2, ctx.kws.kw3).filter(_ != null).map(_.getText).mkString(" ")
    } else {
      // SET ROLE is the exception to the rule, because we handle this before other SET commands.
      "SET ROLE"
    }
    throw new ParseException(s"Unsupported operation: $keywords", ctx)
  }

  /**
   * Create an [[AddJar]] or [[AddFile]] command depending on the requested resource.
   */
  override def visitAddResource(ctx: AddResourceContext): LogicalPlan = withOrigin(ctx) {
    ctx.identifier.getText.toLowerCase match {
      case "file" => AddFile(remainder(ctx.identifier).trim)
      case "jar" => AddJar(remainder(ctx.identifier).trim)
      case other => throw new ParseException(s"Unsupported resource type '$other'.", ctx)
    }
  }

  /**
   * Create an [[AnalyzeTable]] command. This currently only implements the NOSCAN option (other
   * options are passed on to Hive) e.g.:
   * {{{
   *   ANALYZE TABLE table COMPUTE STATISTICS NOSCAN;
   * }}}
   */
  override def visitAnalyze(ctx: AnalyzeContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.partitionSpec == null &&
      ctx.identifier != null &&
      ctx.identifier.getText.toLowerCase == "noscan") {
      AnalyzeTable(visitTableIdentifier(ctx.tableIdentifier).toString)
    } else {
      HiveNativeCommand(command(ctx))
    }
  }

  /**
   * Create a [[CatalogStorageFormat]] for creating tables.
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
        throw new ParseException("Operation not allowed: ... STORED BY storage_handler ...", ctx)
      case _ =>
        throw new ParseException("expected either STORED AS or STORED BY, not both", ctx)
    }
  }

  /**
   * Create a table, returning either a [[CreateTable]] or a [[CreateTableAsSelect]].
   *
   * This is not used to create datasource tables, which is handled through
   * "CREATE TABLE ... USING ...".
   *
   * Note: several features are currently not supported - temporary tables, bucketing,
   * skewed columns and storage handlers (STORED BY).
   *
   * Expected format:
   * {{{
   *   CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
   *   [(col1 data_type [COMMENT col_comment], ...)]
   *   [COMMENT table_comment]
   *   [PARTITIONED BY (col3 data_type [COMMENT col_comment], ...)]
   *   [CLUSTERED BY (col1, ...) [SORTED BY (col1 [ASC|DESC], ...)] INTO num_buckets BUCKETS]
   *   [SKEWED BY (col1, col2, ...) ON ((col_value, col_value, ...), ...) [STORED AS DIRECTORIES]]
   *   [ROW FORMAT row_format]
   *   [STORED AS file_format | STORED BY storage_handler_class [WITH SERDEPROPERTIES (...)]]
   *   [LOCATION path]
   *   [TBLPROPERTIES (property_name=property_value, ...)]
   *   [AS select_statement];
   * }}}
   */
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = withOrigin(ctx) {
    val (name, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
    // TODO: implement temporary tables
    if (temp) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
        "Please use registerTempTable as an alternative.", ctx)
    }
    if (ctx.skewSpec != null) {
      throw new ParseException("Operation not allowed: CREATE TABLE ... SKEWED BY ...", ctx)
    }
    if (ctx.bucketSpec != null) {
      throw new ParseException("Operation not allowed: CREATE TABLE ... CLUSTERED BY ...", ctx)
    }
    val tableType = if (external) {
      CatalogTableType.EXTERNAL_TABLE
    } else {
      CatalogTableType.MANAGED_TABLE
    }
    val comment = Option(ctx.STRING).map(string)
    val partitionCols = Option(ctx.partitionColumns).toSeq.flatMap(visitCatalogColumns)
    val cols = Option(ctx.columns).toSeq.flatMap(visitCatalogColumns)
    val properties = Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty)
    val selectQuery = Option(ctx.query).map(plan)

    // Note: Hive requires partition columns to be distinct from the schema, so we need
    // to include the partition columns here explicitly
    val schema = cols ++ partitionCols

    // Storage format
    val defaultStorage: CatalogStorageFormat = {
      val defaultStorageType = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT)
      val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType, hiveConf)
      CatalogStorageFormat(
        locationUri = None,
        inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
          .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
        outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
          .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
        // Note: Keep this unspecified because we use the presence of the serde to decide
        // whether to convert a table created by CTAS to a datasource table.
        serde = None,
        serdeProperties = Map())
    }
    val fileStorage = Option(ctx.createFileFormat).map(visitCreateFileFormat)
      .getOrElse(EmptyStorageFormat)
    val rowStorage = Option(ctx.rowFormat).map(visitRowFormat).getOrElse(EmptyStorageFormat)
    val location = Option(ctx.locationSpec).map(visitLocationSpec)
    val storage = CatalogStorageFormat(
      locationUri = location,
      inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
      outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
      serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
      serdeProperties = rowStorage.serdeProperties ++ fileStorage.serdeProperties)

    // TODO support the sql text - have a proper location for this!
    val tableDesc = CatalogTable(
      identifier = name,
      tableType = tableType,
      storage = storage,
      schema = schema,
      partitionColumnNames = partitionCols.map(_.name),
      properties = properties,
      comment = comment)

    selectQuery match {
      case Some(q) => CTAS(tableDesc, q, ifNotExists)
      case None => CreateTable(tableDesc, ifNotExists)
    }
  }

  /**
   * Create a [[CreateTableLike]] command.
   */
  override def visitCreateTableLike(ctx: CreateTableLikeContext): LogicalPlan = withOrigin(ctx) {
    val targetTable = visitTableIdentifier(ctx.target)
    val sourceTable = visitTableIdentifier(ctx.source)
    CreateTableLike(targetTable, sourceTable, ctx.EXISTS != null)
  }

  /**
   * Create or replace a view. This creates a [[CreateViewAsSelect]] command.
   *
   * For example:
   * {{{
   *   CREATE VIEW [IF NOT EXISTS] [db_name.]view_name
   *   [(column_name [COMMENT column_comment], ...) ]
   *   [COMMENT view_comment]
   *   [TBLPROPERTIES (property_name = property_value, ...)]
   *   AS SELECT ...;
   * }}}
   */
  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.identifierList != null) {
      throw new ParseException(s"Operation not allowed: partitioned views", ctx)
    } else {
      val identifiers = Option(ctx.identifierCommentList).toSeq.flatMap(_.identifierComment.asScala)
      val schema = identifiers.map { ic =>
        CatalogColumn(ic.identifier.getText, null, nullable = true, Option(ic.STRING).map(string))
      }
      createView(
        ctx,
        ctx.tableIdentifier,
        comment = Option(ctx.STRING).map(string),
        schema,
        ctx.query,
        Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty),
        ctx.EXISTS != null,
        ctx.REPLACE != null
      )
    }
  }

  /**
   * Alter the query of a view. This creates a [[CreateViewAsSelect]] command.
   */
  override def visitAlterViewQuery(ctx: AlterViewQueryContext): LogicalPlan = withOrigin(ctx) {
    createView(
      ctx,
      ctx.tableIdentifier,
      comment = None,
      Seq.empty,
      ctx.query,
      Map.empty,
      allowExist = false,
      replace = true)
  }

  /**
   * Create a [[CreateViewAsSelect]] command.
   */
  private def createView(
      ctx: ParserRuleContext,
      name: TableIdentifierContext,
      comment: Option[String],
      schema: Seq[CatalogColumn],
      query: QueryContext,
      properties: Map[String, String],
      allowExist: Boolean,
      replace: Boolean): LogicalPlan = {
    val sql = Option(source(query))
    val tableDesc = CatalogTable(
      identifier = visitTableIdentifier(name),
      tableType = CatalogTableType.VIRTUAL_VIEW,
      schema = schema,
      storage = EmptyStorageFormat,
      properties = properties,
      viewOriginalText = sql,
      viewText = sql,
      comment = comment)
    CreateView(tableDesc, plan(query), allowExist, replace, command(ctx))
  }

  /**
   * Create a [[HiveScriptIOSchema]].
   */
  override protected def withScriptIOSchema(
      ctx: QuerySpecificationContext,
      inRowFormat: RowFormatContext,
      recordWriter: Token,
      outRowFormat: RowFormatContext,
      recordReader: Token,
      schemaLess: Boolean): HiveScriptIOSchema = {
    if (recordWriter != null || recordReader != null) {
      throw new ParseException(
        "Unsupported operation: Used defined record reader/writer classes.", ctx)
    }

    // Decode and input/output format.
    type Format = (Seq[(String, String)], Option[String], Seq[(String, String)], Option[String])
    def format(fmt: RowFormatContext, confVar: ConfVars): Format = fmt match {
      case c: RowFormatDelimitedContext =>
        // TODO we should use the visitRowFormatDelimited function here. However HiveScriptIOSchema
        // expects a seq of pairs in which the old parsers' token names are used as keys.
        // Transforming the result of visitRowFormatDelimited would be quite a bit messier than
        // retrieving the key value pairs ourselves.
        def entry(key: String, value: Token): Seq[(String, String)] = {
          Option(value).map(t => key -> t.getText).toSeq
        }
        val entries = entry("TOK_TABLEROWFORMATFIELD", c.fieldsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATCOLLITEMS", c.collectionItemsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATMAPKEYS", c.keysTerminatedBy) ++
          entry("TOK_TABLEROWFORMATLINES", c.linesSeparatedBy) ++
          entry("TOK_TABLEROWFORMATNULL", c.nullDefinedAs)

        (entries, None, Seq.empty, None)

      case c: RowFormatSerdeContext =>
        // Use a serde format.
        val CatalogStorageFormat(None, None, None, Some(name), props) = visitRowFormatSerde(c)

        // SPARK-10310: Special cases LazySimpleSerDe
        val recordHandler = if (name == classOf[LazySimpleSerDe].getCanonicalName) {
          Option(hiveConf.getVar(confVar))
        } else {
          None
        }
        (Seq.empty, Option(name), props.toSeq, recordHandler)

      case null =>
        // Use default (serde) format.
        val name = hiveConf.getVar(ConfVars.HIVESCRIPTSERDE)
        val props = Seq(serdeConstants.FIELD_DELIM -> "\t")
        val recordHandler = Option(hiveConf.getVar(confVar))
        (Nil, Option(name), props, recordHandler)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) =
      format(inRowFormat, ConfVars.HIVESCRIPTRECORDREADER)

    val (outFormat, outSerdeClass, outSerdeProps, writer) =
      format(inRowFormat, ConfVars.HIVESCRIPTRECORDWRITER)

    HiveScriptIOSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }

  /**
   * Create location string.
   */
  override def visitLocationSpec(ctx: LocationSpecContext): String = {
    EximUtil.relativeToAbsolutePath(hiveConf, super.visitLocationSpec(ctx))
  }

  /** Empty storage format for default values and copies. */
  private val EmptyStorageFormat = CatalogStorageFormat(None, None, None, None, Map.empty)

  /**
   * Create a [[CatalogStorageFormat]].
   */
  override def visitTableFileFormat(
      ctx: TableFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    EmptyStorageFormat.copy(
      inputFormat = Option(string(ctx.inFmt)),
      outputFormat = Option(string(ctx.outFmt)),
      serde = Option(ctx.serdeCls).map(string)
    )
  }

  /**
   * Resolve a [[HiveSerDe]] based on the name given and return it as a [[CatalogStorageFormat]].
   */
  override def visitGenericFileFormat(
      ctx: GenericFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    val source = ctx.identifier.getText
    HiveSerDe.sourceToSerDe(source, hiveConf) match {
      case Some(s) =>
        EmptyStorageFormat.copy(
          inputFormat = s.inputFormat,
          outputFormat = s.outputFormat,
          serde = s.serde)
      case None =>
        throw new ParseException(s"Unrecognized file format in STORED AS clause: $source", ctx)
    }
  }

  /**
   * Create a [[RowFormat]] used for creating tables.
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
    EmptyStorageFormat.copy(
      serde = Option(string(name)),
      serdeProperties = Option(tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty))
  }

  /**
   * Create a delimited row format properties object.
   */
  override def visitRowFormatDelimited(
      ctx: RowFormatDelimitedContext): CatalogStorageFormat = withOrigin(ctx) {
    // Collect the entries if any.
    def entry(key: String, value: Token): Seq[(String, String)] = {
      Option(value).toSeq.map(x => key -> string(x))
    }
    // TODO we need proper support for the NULL format.
    val entries = entry(serdeConstants.FIELD_DELIM, ctx.fieldsTerminatedBy) ++
      entry(serdeConstants.SERIALIZATION_FORMAT, ctx.fieldsTerminatedBy) ++
      entry(serdeConstants.ESCAPE_CHAR, ctx.escapedBy) ++
      entry(serdeConstants.COLLECTION_DELIM, ctx.collectionItemsTerminatedBy) ++
      entry(serdeConstants.MAPKEY_DELIM, ctx.keysTerminatedBy) ++
      Option(ctx.linesSeparatedBy).toSeq.map { token =>
        val value = string(token)
        assert(
          value == "\n",
          s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
          ctx)
        serdeConstants.LINE_DELIM -> value
      }
    EmptyStorageFormat.copy(serdeProperties = entries.toMap)
  }

  /**
   * Create a sequence of [[CatalogColumn]]s from a column list
   */
  private def visitCatalogColumns(ctx: ColTypeListContext): Seq[CatalogColumn] = withOrigin(ctx) {
    ctx.colType.asScala.map { col =>
      CatalogColumn(
        col.identifier.getText.toLowerCase,
        // Note: for types like "STRUCT<myFirstName: STRING, myLastName: STRING>" we can't
        // just convert the whole type string to lower case, otherwise the struct field names
        // will no longer be case sensitive. Instead, we rely on our parser to get the proper
        // case before passing it to Hive.
        CatalystSqlParser.parseDataType(col.dataType.getText).simpleString,
        nullable = true,
        Option(col.STRING).map(string))
    }
  }
}
