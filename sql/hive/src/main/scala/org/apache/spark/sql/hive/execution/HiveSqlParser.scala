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

import org.apache.spark.sql.catalyst.analysis.UnresolvedGenerator
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.hive.{CreateTableAsSelect => CTAS, CreateViewAsSelect => CreateView}
import org.apache.spark.sql.hive.{HiveGenericUDTF, HiveSerDe}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper

/**
 * Concrete parser for HiveQl statements.
 */
object HiveSqlParser extends AbstractSqlParser {
  val astBuilder = new HiveSqlAstBuilder

  override protected def nativeCommand(sqlText: String): LogicalPlan = {
    HiveNativeCommand(sqlText)
  }
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class HiveSqlAstBuilder extends SparkSqlAstBuilder {
  import ParserUtils._

  /**
   * Get the current Hive Configuration.
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
   * Create a [[DropTable]] command.
   */
  override def visitDropTable(ctx: DropTableContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.PURGE != null) {
      logWarning("PURGE option is ignored.")
    }
    if (ctx.REPLICATION != null) {
      logWarning("REPLICATION clause is ignored.")
    }
    DropTable(visitTableIdentifier(ctx.tableIdentifier).toString, ctx.EXISTS != null)
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
   * Create a [[CatalogStorageFormat]]. This is part of the [[CreateTableAsSelect]] command.
   */
  override def visitCreateFileFormat(
      ctx: CreateFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    if (ctx.storageHandler == null) {
      typedVisit[CatalogStorageFormat](ctx.fileFormat)
    } else {
      visitStorageHandler(ctx.storageHandler)
    }
  }

  /**
   * Create a [[CreateTableAsSelect]] command.
   */
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    if (ctx.query == null) {
      HiveNativeCommand(command(ctx))
    } else {
      // Get the table header.
      val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
      val tableType = if (external) {
        CatalogTableType.EXTERNAL_TABLE
      } else {
        CatalogTableType.MANAGED_TABLE
      }

      // Unsupported clauses.
      if (temp) {
        logWarning("TEMPORARY clause is ignored.")
      }
      if (ctx.bucketSpec != null) {
        // TODO add this - we need cluster columns in the CatalogTable for this to work.
        logWarning("CLUSTERED BY ... [ORDERED BY ...] INTO ... BUCKETS clause is ignored.")
      }
      if (ctx.skewSpec != null) {
        logWarning("SKEWED BY ... ON ... [STORED AS DIRECTORIES] clause is ignored.")
      }

      // Create the schema.
      val schema = Option(ctx.columns).toSeq.flatMap(visitCatalogColumns(_, _.toLowerCase))

      // Get the column by which the table is partitioned.
      val partitionCols = Option(ctx.partitionColumns).toSeq.flatMap(visitCatalogColumns(_))

      // Create the storage.
      def format(fmt: ParserRuleContext): CatalogStorageFormat = {
        Option(fmt).map(typedVisit[CatalogStorageFormat]).getOrElse(EmptyStorageFormat)
      }
      // Default storage.
      val defaultStorageType = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT)
      val hiveSerDe = HiveSerDe.sourceToSerDe(defaultStorageType, hiveConf).getOrElse {
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.mapred.TextInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"))
      }
      // Defined storage.
      val fileStorage = format(ctx.createFileFormat)
      val rowStorage = format(ctx.rowFormat)
      val storage = CatalogStorageFormat(
        Option(ctx.locationSpec).map(visitLocationSpec),
        fileStorage.inputFormat.orElse(hiveSerDe.inputFormat),
        fileStorage.outputFormat.orElse(hiveSerDe.outputFormat),
        rowStorage.serde.orElse(hiveSerDe.serde).orElse(fileStorage.serde),
        rowStorage.serdeProperties ++ fileStorage.serdeProperties
      )

      val tableDesc = CatalogTable(
        identifier = table,
        tableType = tableType,
        schema = schema,
        partitionColumns = partitionCols,
        storage = storage,
        properties = Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty),
        // TODO support the sql text - have a proper location for this!
        viewText = Option(ctx.STRING).map(string))
      CTAS(tableDesc, plan(ctx.query), ifNotExists)
    }
  }

  /**
   * Create or replace a view. This creates a [[CreateViewAsSelect]] command.
   */
  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    // Pass a partitioned view on to hive.
    if (ctx.identifierList != null) {
      HiveNativeCommand(command(ctx))
    } else {
      if (ctx.STRING != null) {
        logWarning("COMMENT clause is ignored.")
      }
      val identifiers = Option(ctx.identifierCommentList).toSeq.flatMap(_.identifierComment.asScala)
      val schema = identifiers.map { ic =>
        CatalogColumn(ic.identifier.getText, null, nullable = true, Option(ic.STRING).map(string))
      }
      createView(
        ctx,
        ctx.tableIdentifier,
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
      viewText = sql)
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
      logWarning("Used defined record reader/writer classes are currently ignored.")
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
   * Create a [[CatalogStorageFormat]]. The INPUTDRIVER and OUTPUTDRIVER clauses are currently
   * ignored.
   */
  override def visitTableFileFormat(
      ctx: TableFileFormatContext): CatalogStorageFormat = withOrigin(ctx) {
    import ctx._
    if (inDriver != null || outDriver != null) {
      logWarning("INPUTDRIVER ... OUTPUTDRIVER ... clauses are ignored.")
    }
    EmptyStorageFormat.copy(
      inputFormat = Option(string(inFmt)),
      outputFormat = Option(string(outFmt)),
      serde = Option(serdeCls).map(string)
    )
  }

  /**
   * Resolve a [[HiveSerDe]] based on the format name given.
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
   * Storage Handlers are currently not supported in the statements we support (CTAS).
   */
  override def visitStorageHandler(
      ctx: StorageHandlerContext): CatalogStorageFormat = withOrigin(ctx) {
    throw new ParseException("Storage Handlers are currently unsupported.", ctx)
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
  private def visitCatalogColumns(
      ctx: ColTypeListContext,
      formatter: String => String = identity): Seq[CatalogColumn] = withOrigin(ctx) {
    ctx.colType.asScala.map { col =>
      CatalogColumn(
        formatter(col.identifier.getText),
        col.dataType.getText.toLowerCase, // TODO validate this?
        nullable = true,
        Option(col.STRING).map(string))
    }
  }
}
