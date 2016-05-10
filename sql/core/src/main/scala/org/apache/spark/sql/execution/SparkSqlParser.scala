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

import scala.collection.JavaConverters._
import scala.util.Try

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, ScriptInputOutputSchema}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.DataType

/**
 * Concrete parser for Spark SQL statements.
 */
class SparkSqlParser(conf: SQLConf) extends AbstractSqlParser {
  val astBuilder = new SparkSqlAstBuilder(conf)

  private val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class SparkSqlAstBuilder(conf: SQLConf) extends AstBuilder {
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
      // Always just run the no scan analyze. We should fix this and implement full analyze
      // command in the future.
      AnalyzeTable(visitTableIdentifier(ctx.tableIdentifier).toString)
    }
  }

  /**
   * Create a [[SetDatabaseCommand]] logical plan.
   */
  override def visitUse(ctx: UseContext): LogicalPlan = withOrigin(ctx) {
    SetDatabaseCommand(ctx.db.getText)
  }

  /**
   * Create a [[ShowTablesCommand]] logical plan.
   * Example SQL :
   * {{{
   *   SHOW TABLES [(IN|FROM) database_name] [[LIKE] 'identifier_with_wildcards'];
   * }}}
   */
  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = withOrigin(ctx) {
    ShowTablesCommand(
      Option(ctx.db).map(_.getText),
      Option(ctx.pattern).map(string))
  }

  /**
   * Create a [[ShowDatabasesCommand]] logical plan.
   * Example SQL:
   * {{{
   *   SHOW (DATABASES|SCHEMAS) [LIKE 'identifier_with_wildcards'];
   * }}}
   */
  override def visitShowDatabases(ctx: ShowDatabasesContext): LogicalPlan = withOrigin(ctx) {
    ShowDatabasesCommand(Option(ctx.pattern).map(string))
  }

  /**
   * A command for users to list the properties for a table. If propertyKey is specified, the value
   * for the propertyKey is returned. If propertyKey is not specified, all the keys and their
   * corresponding values are returned.
   * The syntax of using this command in SQL is:
   * {{{
   *   SHOW TBLPROPERTIES table_name[('propertyKey')];
   * }}}
   */
  override def visitShowTblProperties(
      ctx: ShowTblPropertiesContext): LogicalPlan = withOrigin(ctx) {
    ShowTablePropertiesCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.key).map(visitTablePropertyKey))
  }

  /**
   * A command for users to list the column names for a table.
   * This function creates a [[ShowColumnsCommand]] logical plan.
   *
   * The syntax of using this command in SQL is:
   * {{{
   *   SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
   * }}}
   */
  override def visitShowColumns(ctx: ShowColumnsContext): LogicalPlan = withOrigin(ctx) {
    val table = visitTableIdentifier(ctx.tableIdentifier)

    val lookupTable = Option(ctx.db) match {
      case None => table
      case Some(db) if table.database.exists(_ != db) =>
        throw operationNotAllowed(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${table.database.get}'",
          ctx)
      case Some(db) => TableIdentifier(table.identifier, Some(db.getText))
    }
    ShowColumnsCommand(lookupTable)
  }

  /**
   * A command for users to list the partition names of a table. If partition spec is specified,
   * partitions that match the spec are returned. Otherwise an empty result set is returned.
   *
   * This function creates a [[ShowPartitionsCommand]] logical plan
   *
   * The syntax of using this command in SQL is:
   * {{{
   *   SHOW PARTITIONS table_identifier [partition_spec];
   * }}}
   */
  override def visitShowPartitions(ctx: ShowPartitionsContext): LogicalPlan = withOrigin(ctx) {
    val table = visitTableIdentifier(ctx.tableIdentifier)
    val partitionKeys = Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec)
    ShowPartitionsCommand(table, partitionKeys)
  }

  /**
   * Create a [[RefreshTable]] logical plan.
   */
  override def visitRefreshTable(ctx: RefreshTableContext): LogicalPlan = withOrigin(ctx) {
    RefreshTable(visitTableIdentifier(ctx.tableIdentifier))
  }

  /**
   * Create a [[CacheTableCommand]] logical plan.
   */
  override def visitCacheTable(ctx: CacheTableContext): LogicalPlan = withOrigin(ctx) {
    val query = Option(ctx.query).map(plan)
    CacheTableCommand(ctx.identifier.getText, query, ctx.LAZY != null)
  }

  /**
   * Create an [[UncacheTableCommand]] logical plan.
   */
  override def visitUncacheTable(ctx: UncacheTableContext): LogicalPlan = withOrigin(ctx) {
    UncacheTableCommand(ctx.identifier.getText)
  }

  /**
   * Create a [[ClearCacheCommand]] logical plan.
   */
  override def visitClearCache(ctx: ClearCacheContext): LogicalPlan = withOrigin(ctx) {
    ClearCacheCommand
  }

  /**
   * Create an [[ExplainCommand]] logical plan.
   */
  override def visitExplain(ctx: ExplainContext): LogicalPlan = withOrigin(ctx) {
    val options = ctx.explainOption.asScala
    if (options.exists(_.FORMATTED != null)) {
      throw operationNotAllowed("EXPLAIN FORMATTED", ctx)
    }

    // Create the explain comment.
    val statement = plan(ctx.statement)
    if (isExplainableStatement(statement)) {
      ExplainCommand(statement, extended = options.exists(_.EXTENDED != null),
        codegen = options.exists(_.CODEGEN != null))
    } else {
      ExplainCommand(OneRowRelation)
    }
  }

  /**
   * Determine if a plan should be explained at all.
   */
  protected def isExplainableStatement(plan: LogicalPlan): Boolean = plan match {
    case _: DescribeTableCommand => false
    case _ => true
  }

  /**
   * Create a [[DescribeTableCommand]] logical plan.
   */
  override def visitDescribeTable(ctx: DescribeTableContext): LogicalPlan = withOrigin(ctx) {
    // FORMATTED and columns are not supported. Return null and let the parser decide what to do
    // with this (create an exception or pass it on to a different system).
    if (ctx.describeColName != null || ctx.partitionSpec != null) {
      null
    } else {
      DescribeTableCommand(
        visitTableIdentifier(ctx.tableIdentifier),
        ctx.EXTENDED != null,
        ctx.FORMATTED() != null)
    }
  }

  /**
   * Type to keep track of a table header: (identifier, isTemporary, ifNotExists, isExternal).
   */
  type TableHeader = (TableIdentifier, Boolean, Boolean, Boolean)

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
      ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    if (temporary && ifNotExists) {
      throw operationNotAllowed("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    (visitTableIdentifier(ctx.tableIdentifier), temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
   * Create a [[CreateTableUsing]] or a [[CreateTableUsingAsSelect]] logical plan.
   *
   * TODO add bucketing and partitioning.
   */
  override def visitCreateTableUsing(ctx: CreateTableUsingContext): LogicalPlan = withOrigin(ctx) {
    val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
    if (external) {
      throw operationNotAllowed("CREATE EXTERNAL TABLE ... USING", ctx)
    }
    val options = Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty)
    val provider = ctx.tableProvider.qualifiedName.getText
    val bucketSpec = Option(ctx.bucketSpec()).map(visitBucketSpec)

    if (ctx.query != null) {
      // Get the backing query.
      val query = plan(ctx.query)

      // Determine the storage mode.
      val mode = if (ifNotExists) {
        SaveMode.Ignore
      } else if (temp) {
        SaveMode.Overwrite
      } else {
        SaveMode.ErrorIfExists
      }

      val partitionColumnNames =
        Option(ctx.partitionColumnNames)
          .map(visitIdentifierList(_).toArray)
          .getOrElse(Array.empty[String])

      CreateTableUsingAsSelect(
        table, provider, temp, partitionColumnNames, bucketSpec, mode, options, query)
    } else {
      val struct = Option(ctx.colTypeList()).map(createStructType)
      CreateTableUsing(table, struct, provider, temp, options, ifNotExists, managedIfNoPath = true)
    }
  }

  /**
   * Create a [[LoadData]] command.
   *
   * For example:
   * {{{
   *   LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
   *   [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitLoadData(ctx: LoadDataContext): LogicalPlan = withOrigin(ctx) {
    LoadData(
      table = visitTableIdentifier(ctx.tableIdentifier),
      path = string(ctx.path),
      isLocal = ctx.LOCAL != null,
      isOverwrite = ctx.OVERWRITE != null,
      partition = Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec)
    )
  }

  /**
   * Convert a table property list into a key-value map.
   */
  override def visitTablePropertyList(
      ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    ctx.tableProperty.asScala.map { property =>
      val key = visitTablePropertyKey(property.key)
      val value = Option(property.value).map(string).orNull
      key -> value
    }.toMap
  }

  /**
   * A table property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a table property
   * identifier.
   */
  override def visitTablePropertyKey(key: TablePropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
   * Create a [[CreateDatabase]] command.
   *
   * For example:
   * {{{
   *   CREATE DATABASE [IF NOT EXISTS] database_name [COMMENT database_comment]
   *    [LOCATION path] [WITH DBPROPERTIES (key1=val1, key2=val2, ...)]
   * }}}
   */
  override def visitCreateDatabase(ctx: CreateDatabaseContext): LogicalPlan = withOrigin(ctx) {
    CreateDatabase(
      ctx.identifier.getText,
      ctx.EXISTS != null,
      Option(ctx.locationSpec).map(visitLocationSpec),
      Option(ctx.comment).map(string),
      Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty))
  }

  /**
   * Create an [[AlterDatabaseProperties]] command.
   *
   * For example:
   * {{{
   *   ALTER (DATABASE|SCHEMA) database SET DBPROPERTIES (property_name=property_value, ...);
   * }}}
   */
  override def visitSetDatabaseProperties(
      ctx: SetDatabasePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterDatabaseProperties(
      ctx.identifier.getText,
      visitTablePropertyList(ctx.tablePropertyList))
  }

  /**
   * Create a [[DropDatabase]] command.
   *
   * For example:
   * {{{
   *   DROP (DATABASE|SCHEMA) [IF EXISTS] database [RESTRICT|CASCADE];
   * }}}
   */
  override def visitDropDatabase(ctx: DropDatabaseContext): LogicalPlan = withOrigin(ctx) {
    DropDatabase(ctx.identifier.getText, ctx.EXISTS != null, ctx.CASCADE != null)
  }

  /**
   * Create a [[DescribeDatabase]] command.
   *
   * For example:
   * {{{
   *   DESCRIBE DATABASE [EXTENDED] database;
   * }}}
   */
  override def visitDescribeDatabase(ctx: DescribeDatabaseContext): LogicalPlan = withOrigin(ctx) {
    DescribeDatabase(ctx.identifier.getText, ctx.EXTENDED != null)
  }

  /**
   * Create a [[CreateFunction]] command.
   *
   * For example:
   * {{{
   *   CREATE [TEMPORARY] FUNCTION [db_name.]function_name AS class_name
   *    [USING JAR|FILE|ARCHIVE 'file_uri' [, JAR|FILE|ARCHIVE 'file_uri']];
   * }}}
   */
  override def visitCreateFunction(ctx: CreateFunctionContext): LogicalPlan = withOrigin(ctx) {
    val resources = ctx.resource.asScala.map { resource =>
      val resourceType = resource.identifier.getText.toLowerCase
      resourceType match {
        case "jar" | "file" | "archive" =>
          resourceType -> string(resource.STRING)
        case other =>
          throw operationNotAllowed(s"CREATE FUNCTION with resource type '$resourceType'", ctx)
      }
    }

    // Extract database, name & alias.
    val functionIdentifier = visitFunctionName(ctx.qualifiedName)
    CreateFunction(
      functionIdentifier.database,
      functionIdentifier.funcName,
      string(ctx.className),
      resources,
      ctx.TEMPORARY != null)
  }

  /**
   * Create a [[DropFunction]] command.
   *
   * For example:
   * {{{
   *   DROP [TEMPORARY] FUNCTION [IF EXISTS] function;
   * }}}
   */
  override def visitDropFunction(ctx: DropFunctionContext): LogicalPlan = withOrigin(ctx) {
    val functionIdentifier = visitFunctionName(ctx.qualifiedName)
    DropFunction(
      functionIdentifier.database,
      functionIdentifier.funcName,
      ctx.EXISTS != null,
      ctx.TEMPORARY != null)
  }

  /**
   * Create a [[DropTable]] command.
   */
  override def visitDropTable(ctx: DropTableContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.PURGE != null) {
      throw operationNotAllowed("DROP TABLE ... PURGE", ctx)
    }
    DropTable(
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.EXISTS != null,
      ctx.VIEW != null)
  }

  /**
   * Create a [[AlterTableRename]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 RENAME TO table2;
   *   ALTER VIEW view1 RENAME TO view2;
   * }}}
   */
  override def visitRenameTable(ctx: RenameTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableRename(
      visitTableIdentifier(ctx.from),
      visitTableIdentifier(ctx.to),
      ctx.VIEW != null)
  }

  /**
   * Create an [[AlterTableSetProperties]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table SET TBLPROPERTIES ('comment' = new_comment);
   *   ALTER VIEW view SET TBLPROPERTIES ('comment' = new_comment);
   * }}}
   */
  override def visitSetTableProperties(
      ctx: SetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterTableSetProperties(
      visitTableIdentifier(ctx.tableIdentifier),
      visitTablePropertyList(ctx.tablePropertyList),
      ctx.VIEW != null)
  }

  /**
   * Create an [[AlterTableUnsetProperties]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
   *   ALTER VIEW view UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
   * }}}
   */
  override def visitUnsetTableProperties(
      ctx: UnsetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterTableUnsetProperties(
      visitTableIdentifier(ctx.tableIdentifier),
      visitTablePropertyList(ctx.tablePropertyList).keys.toSeq,
      ctx.EXISTS != null,
      ctx.VIEW != null)
  }

  /**
   * Create an [[AlterTableSerDeProperties]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
   *   ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
   * }}}
   */
  override def visitSetTableSerDe(ctx: SetTableSerDeContext): LogicalPlan = withOrigin(ctx) {
    AlterTableSerDeProperties(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.STRING).map(string),
      Option(ctx.tablePropertyList).map(visitTablePropertyList),
      // TODO a partition spec is allowed to have optional values. This is currently violated.
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
  }

  /**
   * Create an [[AlterTableAddPartition]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table ADD [IF NOT EXISTS] PARTITION spec [LOCATION 'loc1']
   *   ALTER VIEW view ADD [IF NOT EXISTS] PARTITION spec
   * }}}
   *
   * ALTER VIEW ... ADD PARTITION ... is not supported because the concept of partitioning
   * is associated with physical tables
   */
  override def visitAddTablePartition(
      ctx: AddTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.VIEW != null) {
      throw operationNotAllowed("ALTER VIEW ... ADD PARTITION", ctx)
    }
    // Create partition spec to location mapping.
    val specsAndLocs = if (ctx.partitionSpec.isEmpty) {
      ctx.partitionSpecLocation.asScala.map {
        splCtx =>
          val spec = visitNonOptionalPartitionSpec(splCtx.partitionSpec)
          val location = Option(splCtx.locationSpec).map(visitLocationSpec)
          spec -> location
      }
    } else {
      // Alter View: the location clauses are not allowed.
      ctx.partitionSpec.asScala.map(visitNonOptionalPartitionSpec(_) -> None)
    }
    AlterTableAddPartition(
      visitTableIdentifier(ctx.tableIdentifier),
      specsAndLocs,
      ctx.EXISTS != null)
  }

  /**
   * Create an [[AlterTableRenamePartition]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2;
   * }}}
   */
  override def visitRenameTablePartition(
      ctx: RenameTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    AlterTableRenamePartition(
      visitTableIdentifier(ctx.tableIdentifier),
      visitNonOptionalPartitionSpec(ctx.from),
      visitNonOptionalPartitionSpec(ctx.to))
  }

  /**
   * Create an [[AlterTableDropPartition]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
   *   ALTER VIEW view DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...];
   * }}}
   *
   * ALTER VIEW ... DROP PARTITION ... is not supported because the concept of partitioning
   * is associated with physical tables
   */
  override def visitDropTablePartitions(
      ctx: DropTablePartitionsContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.VIEW != null) {
      throw operationNotAllowed("ALTER VIEW ... DROP PARTITION", ctx)
    }
    if (ctx.PURGE != null) {
      throw operationNotAllowed("ALTER TABLE ... DROP PARTITION ... PURGE", ctx)
    }
    AlterTableDropPartition(
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.partitionSpec.asScala.map(visitNonOptionalPartitionSpec),
      ctx.EXISTS != null)
  }

  /**
   * Create an [[AlterTableSetLocation]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] SET LOCATION "loc";
   * }}}
   */
  override def visitSetTableLocation(ctx: SetTableLocationContext): LogicalPlan = withOrigin(ctx) {
    AlterTableSetLocation(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      visitLocationSpec(ctx.locationSpec))
  }

  /**
   * Create location string.
   */
  override def visitLocationSpec(ctx: LocationSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  /**
   * Create a [[BucketSpec]].
   */
  override def visitBucketSpec(ctx: BucketSpecContext): BucketSpec = withOrigin(ctx) {
    BucketSpec(
      ctx.INTEGER_VALUE.getText.toInt,
      visitIdentifierList(ctx.identifierList),
      Option(ctx.orderedIdentifierList)
        .toSeq
        .flatMap(_.orderedIdentifier.asScala)
        .map { orderedIdCtx =>
          Option(orderedIdCtx.ordering).map(_.getText).foreach { dir =>
            if (dir.toLowerCase != "asc") {
              throw operationNotAllowed(s"Column ordering must be ASC, was '$dir'", ctx)
            }
          }

          orderedIdCtx.identifier.getText
        })
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
    ctx.constant.asScala.map(visitStringConstant)
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
    throw operationNotAllowed(keywords, ctx)
  }

  /**
   * Create an [[AddJar]] or [[AddFile]] command depending on the requested resource.
   */
  override def visitAddResource(ctx: AddResourceContext): LogicalPlan = withOrigin(ctx) {
    ctx.identifier.getText.toLowerCase match {
      case "file" => AddFile(remainder(ctx.identifier).trim)
      case "jar" => AddJar(remainder(ctx.identifier).trim)
      case other => throw operationNotAllowed(s"ADD with resource type '$other'", ctx)
    }
  }

  /**
   * Create a table, returning either a [[CreateTable]] or a [[CreateTableAsSelectLogicalPlan]].
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
      throw operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
    }
    if (ctx.bucketSpec != null) {
      throw operationNotAllowed("CREATE TABLE ... CLUSTERED BY", ctx)
    }
    val tableType = if (external) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
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
      val defaultStorageType = conf.getConfString("hive.default.fileformat", "textfile")
      val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType, conf)
      CatalogStorageFormat(
        locationUri = None,
        inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
          .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
        outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
          .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
        // Note: Keep this unspecified because we use the presence of the serde to decide
        // whether to convert a table created by CTAS to a datasource table.
        serde = None,
        compressed = false,
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
      compressed = false,
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
      case Some(q) => CreateTableAsSelectLogicalPlan(tableDesc, q, ifNotExists)
      case None => CreateTable(tableDesc, ifNotExists)
    }
  }

  /**
   * Create a [[CreateTableLike]] command.
   *
   * For example:
   * {{{
   *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
   *   LIKE [other_db_name.]existing_table_name
   * }}}
   */
  override def visitCreateTableLike(ctx: CreateTableLikeContext): LogicalPlan = withOrigin(ctx) {
    val targetTable = visitTableIdentifier(ctx.target)
    val sourceTable = visitTableIdentifier(ctx.source)
    CreateTableLike(targetTable, sourceTable, ctx.EXISTS != null)
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
        throw operationNotAllowed("STORED BY", ctx)
      case _ =>
        throw new ParseException("Expected either STORED AS or STORED BY, not both", ctx)
    }
  }

  /** Empty storage format for default values and copies. */
  private val EmptyStorageFormat = CatalogStorageFormat(None, None, None, None, false, Map.empty)

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
    HiveSerDe.sourceToSerDe(source, conf) match {
      case Some(s) =>
        EmptyStorageFormat.copy(
          inputFormat = s.inputFormat,
          outputFormat = s.outputFormat,
          serde = s.serde)
      case None =>
        throw operationNotAllowed(s"STORED AS with file format '$source'", ctx)
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
    val entries =
      entry("field.delim", ctx.fieldsTerminatedBy) ++
        entry("serialization.format", ctx.fieldsTerminatedBy) ++
        entry("escape.delim", ctx.escapedBy) ++
        // The following typo is inherited from Hive...
        entry("colelction.delim", ctx.collectionItemsTerminatedBy) ++
        entry("mapkey.delim", ctx.keysTerminatedBy) ++
        Option(ctx.linesSeparatedBy).toSeq.map { token =>
          val value = string(token)
          assert(
            value == "\n",
            s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
            ctx)
          "line.delim" -> value
        }
    EmptyStorageFormat.copy(serdeProperties = entries.toMap)
  }

  /**
   * Create or replace a view. This creates a [[CreateViewCommand]] command.
   *
   * For example:
   * {{{
   *   CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [db_name.]view_name
   *   [(column_name [COMMENT column_comment], ...) ]
   *   [COMMENT view_comment]
   *   [TBLPROPERTIES (property_name = property_value, ...)]
   *   AS SELECT ...;
   * }}}
   */
  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.identifierList != null) {
      throw operationNotAllowed("CREATE VIEW ... PARTITIONED ON", ctx)
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
        ctx.REPLACE != null,
        ctx.TEMPORARY != null
      )
    }
  }

  /**
   * Alter the query of a view. This creates a [[CreateViewCommand]] command.
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
      replace = true,
      isTemporary = false)
  }

  /**
   * Create a [[CreateViewCommand]] command.
   */
  private def createView(
      ctx: ParserRuleContext,
      name: TableIdentifierContext,
      comment: Option[String],
      schema: Seq[CatalogColumn],
      query: QueryContext,
      properties: Map[String, String],
      allowExist: Boolean,
      replace: Boolean,
      isTemporary: Boolean): LogicalPlan = {
    val sql = Option(source(query))
    val tableDesc = CatalogTable(
      identifier = visitTableIdentifier(name),
      tableType = CatalogTableType.VIEW,
      schema = schema,
      storage = EmptyStorageFormat,
      properties = properties,
      viewOriginalText = sql,
      viewText = sql,
      comment = comment)
    CreateViewCommand(tableDesc, plan(query), allowExist, replace, isTemporary, command(ctx))
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
        typedVisit[DataType](col.dataType).catalogString,
        nullable = true,
        Option(col.STRING).map(string))
    }
  }

  /**
   * Create a [[ScriptInputOutputSchema]].
   */
  override protected def withScriptIOSchema(
      ctx: QuerySpecificationContext,
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
    def format(fmt: RowFormatContext, configKey: String): Format = fmt match {
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
        val CatalogStorageFormat(None, None, None, Some(name), _, props) = visitRowFormatSerde(c)

        // SPARK-10310: Special cases LazySimpleSerDe
        val recordHandler = if (name == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
          Try(conf.getConfString(configKey)).toOption
        } else {
          None
        }
        (Seq.empty, Option(name), props.toSeq, recordHandler)

      case null =>
        // Use default (serde) format.
        val name = conf.getConfString("hive.script.serde",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        val props = Seq("field.delim" -> "\t")
        val recordHandler = Try(conf.getConfString(configKey)).toOption
        (Nil, Option(name), props, recordHandler)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) =
      format(inRowFormat, "hive.script.recordreader")

    val (outFormat, outSerdeClass, outSerdeProps, writer) =
      format(outRowFormat, "hive.script.recordwriter")

    ScriptInputOutputSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }
}
