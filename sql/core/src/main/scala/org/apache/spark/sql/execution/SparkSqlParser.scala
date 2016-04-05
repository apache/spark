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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder, ParseException}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.command.{DescribeCommand => _, _}
import org.apache.spark.sql.execution.datasources._

/**
 * Concrete parser for Spark SQL statements.
 */
object SparkSqlParser extends AbstractSqlParser{
  val astBuilder = new SparkSqlAstBuilder
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class SparkSqlAstBuilder extends AstBuilder {
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
      logWarning("EXPLAIN FORMATTED option is ignored.")
    }
    if (options.exists(_.LOGICAL != null)) {
      logWarning("EXPLAIN LOGICAL option is ignored.")
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
    case _: datasources.DescribeCommand => false
    case _ => true
  }

  /**
   * Create a [[DescribeCommand]] logical plan.
   */
  override def visitDescribeTable(ctx: DescribeTableContext): LogicalPlan = withOrigin(ctx) {
    // FORMATTED and columns are not supported. Return null and let the parser decide what to do
    // with this (create an exception or pass it on to a different system).
    if (ctx.describeColName != null || ctx.FORMATTED != null || ctx.partitionSpec != null) {
      null
    } else {
      datasources.DescribeCommand(
        visitTableIdentifier(ctx.tableIdentifier),
        ctx.EXTENDED != null)
    }
  }

  /** Type to keep track of a table header. */
  type TableHeader = (TableIdentifier, Boolean, Boolean, Boolean)

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
      ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    assert(!temporary || !ifNotExists,
      "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.",
      ctx)
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
      logWarning("EXTERNAL option is not supported.")
    }
    val options = Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty)
    val provider = ctx.tableProvider.qualifiedName.getText

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
      CreateTableUsingAsSelect(table, provider, temp, Array.empty, None, mode, options, query)
    } else {
      val struct = Option(ctx.colTypeList).map(createStructType)
      CreateTableUsing(table, struct, provider, temp, options, ifNotExists, managedIfNoPath = false)
    }
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
          throw new ParseException(s"Resource Type '$resourceType' is not supported.", ctx)
      }
    }

    // Extract database, name & alias.
    val (database, function) = visitFunctionName(ctx.qualifiedName)
    CreateFunction(
      database,
      function,
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
    val (database, function) = visitFunctionName(ctx.qualifiedName)
    DropFunction(database, function, ctx.EXISTS != null, ctx.TEMPORARY != null)
  }

  /**
   * Create a function database (optional) and name pair.
   */
  private def visitFunctionName(ctx: QualifiedNameContext): (Option[String], String) = {
    ctx.identifier().asScala.map(_.getText) match {
      case Seq(db, fn) => (Option(db), fn)
      case Seq(fn) => (None, fn)
      case other => throw new ParseException(s"Unsupported function name '${ctx.getText}'", ctx)
    }
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
      visitTableIdentifier(ctx.to))(
      command(ctx))
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
      visitTablePropertyList(ctx.tablePropertyList))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableUnsetProperties]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table UNSET TBLPROPERTIES IF EXISTS ('comment', 'key');
   *   ALTER VIEW view UNSET TBLPROPERTIES IF EXISTS ('comment', 'key');
   * }}}
   */
  override def visitUnsetTableProperties(
      ctx: UnsetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterTableUnsetProperties(
      visitTableIdentifier(ctx.tableIdentifier),
      visitTablePropertyList(ctx.tablePropertyList),
      ctx.EXISTS != null)(
      command(ctx))
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
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableStorageProperties]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table CLUSTERED BY (col, ...) [SORTED BY (col, ...)] INTO n BUCKETS;
   * }}}
   */
  override def visitBucketTable(ctx: BucketTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableStorageProperties(
      visitTableIdentifier(ctx.tableIdentifier),
      visitBucketSpec(ctx.bucketSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableNotClustered]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table NOT CLUSTERED;
   * }}}
   */
  override def visitUnclusterTable(ctx: UnclusterTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableNotClustered(visitTableIdentifier(ctx.tableIdentifier))(command(ctx))
  }

  /**
   * Create an [[AlterTableNotSorted]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table NOT SORTED;
   * }}}
   */
  override def visitUnsortTable(ctx: UnsortTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableNotSorted(visitTableIdentifier(ctx.tableIdentifier))(command(ctx))
  }

  /**
   * Create an [[AlterTableSkewed]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table SKEWED BY (col1, col2)
   *   ON ((col1_value, col2_value) [, (col1_value, col2_value), ...])
   *   [STORED AS DIRECTORIES];
   * }}}
   */
  override def visitSkewTable(ctx: SkewTableContext): LogicalPlan = withOrigin(ctx) {
    val table = visitTableIdentifier(ctx.tableIdentifier)
    val (cols, values, storedAsDirs) = visitSkewSpec(ctx.skewSpec)
    AlterTableSkewed(table, cols, values, storedAsDirs)(command(ctx))
  }

  /**
   * Create an [[AlterTableNotSorted]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table NOT SKEWED;
   * }}}
   */
  override def visitUnskewTable(ctx: UnskewTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableNotSkewed(visitTableIdentifier(ctx.tableIdentifier))(command(ctx))
  }

  /**
   * Create an [[AlterTableNotStoredAsDirs]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table NOT STORED AS DIRECTORIES
   * }}}
   */
  override def visitUnstoreTable(ctx: UnstoreTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableNotStoredAsDirs(visitTableIdentifier(ctx.tableIdentifier))(command(ctx))
  }

  /**
   * Create an [[AlterTableSkewedLocation]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table SET SKEWED LOCATION (col1="loc1" [, (col2, col3)="loc2", ...] );
   * }}}
   */
  override def visitSetTableSkewLocations(
      ctx: SetTableSkewLocationsContext): LogicalPlan = withOrigin(ctx) {
    val skewedMap = ctx.skewedLocationList.skewedLocation.asScala.flatMap {
      slCtx =>
        val location = string(slCtx.STRING)
        if (slCtx.constant != null) {
          Seq(visitStringConstant(slCtx.constant) -> location)
        } else {
          // TODO this is similar to what was in the original implementation. However this does not
          // make to much sense to me since we should be storing a tuple of values (not column
          // names) for which we want a dedicated storage location.
          visitConstantList(slCtx.constantList).map(_ -> location)
        }
    }.toMap

    AlterTableSkewedLocation(
      visitTableIdentifier(ctx.tableIdentifier),
      skewedMap)(
      command(ctx))
  }

  /**
   * Create an [[AlterTableAddPartition]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table ADD [IF NOT EXISTS] PARTITION spec [LOCATION 'loc1']
   *   ALTER VIEW view ADD [IF NOT EXISTS] PARTITION spec
   * }}}
   */
  override def visitAddTablePartition(
      ctx: AddTablePartitionContext): LogicalPlan = withOrigin(ctx) {
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
      ctx.EXISTS != null)(
      command(ctx))
  }

  /**
   * Create an [[AlterTableExchangePartition]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 EXCHANGE PARTITION spec WITH TABLE table2;
   * }}}
   */
  override def visitExchangeTablePartition(
      ctx: ExchangeTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    AlterTableExchangePartition(
      visitTableIdentifier(ctx.from),
      visitTableIdentifier(ctx.to),
      visitNonOptionalPartitionSpec(ctx.partitionSpec))(
      command(ctx))
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
      visitNonOptionalPartitionSpec(ctx.to))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableDropPartition]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
   *   ALTER VIEW view DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...];
   * }}}
   */
  override def visitDropTablePartitions(
      ctx: DropTablePartitionsContext): LogicalPlan = withOrigin(ctx) {
    AlterTableDropPartition(
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.partitionSpec.asScala.map(visitNonOptionalPartitionSpec),
      ctx.EXISTS != null,
      ctx.PURGE != null)(
      command(ctx))
  }

  /**
   * Create an [[AlterTableArchivePartition]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table ARCHIVE PARTITION spec;
   * }}}
   */
  override def visitArchiveTablePartition(
      ctx: ArchiveTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    AlterTableArchivePartition(
      visitTableIdentifier(ctx.tableIdentifier),
      visitNonOptionalPartitionSpec(ctx.partitionSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableUnarchivePartition]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table UNARCHIVE PARTITION spec;
   * }}}
   */
  override def visitUnarchiveTablePartition(
      ctx: UnarchiveTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    AlterTableUnarchivePartition(
      visitTableIdentifier(ctx.tableIdentifier),
      visitNonOptionalPartitionSpec(ctx.partitionSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableSetFileFormat]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] SET FILEFORMAT file_format;
   * }}}
   */
  override def visitSetTableFileFormat(
      ctx: SetTableFileFormatContext): LogicalPlan = withOrigin(ctx) {
    // AlterTableSetFileFormat currently takes both a GenericFileFormat and a
    // TableFileFormatContext. This is a bit weird because it should only take one. It also should
    // use a CatalogFileFormat instead of either a String or a Sequence of Strings. We will address
    // this in a follow-up PR.
    val (fileFormat, genericFormat) = ctx.fileFormat match {
      case s: GenericFileFormatContext =>
        (Seq.empty[String], Option(s.identifier.getText))
      case s: TableFileFormatContext =>
        val elements = Seq(s.inFmt, s.outFmt) ++
          Option(s.serdeCls).toSeq ++
          Option(s.inDriver).toSeq ++
          Option(s.outDriver).toSeq
        (elements.map(string), None)
    }
    AlterTableSetFileFormat(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      fileFormat,
      genericFormat)(
      command(ctx))
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
      visitLocationSpec(ctx.locationSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableTouch]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table TOUCH [PARTITION spec];
   * }}}
   */
  override def visitTouchTable(ctx: TouchTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableTouch(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableCompact]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] COMPACT 'compaction_type';
   * }}}
   */
  override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableCompact(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      string(ctx.STRING))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableMerge]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] CONCATENATE;
   * }}}
   */
  override def visitConcatenateTable(ctx: ConcatenateTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableMerge(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))(
      command(ctx))
  }

  /**
   * Create an [[AlterTableChangeCol]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE tableIdentifier [PARTITION spec]
   *    CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment]
   *    [FIRST|AFTER column_name] [CASCADE|RESTRICT];
   * }}}
   */
  override def visitChangeColumn(ctx: ChangeColumnContext): LogicalPlan = withOrigin(ctx) {
    val col = visitColType(ctx.colType())
    val comment = if (col.metadata.contains("comment")) {
      Option(col.metadata.getString("comment"))
    } else {
      None
    }

    AlterTableChangeCol(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      ctx.oldName.getText,
      // We could also pass in a struct field - seems easier.
      col.name,
      col.dataType,
      comment,
      Option(ctx.after).map(_.getText),
      // Note that Restrict and Cascade are mutually exclusive.
      ctx.RESTRICT != null,
      ctx.CASCADE != null)(
      command(ctx))
  }

  /**
   * Create an [[AlterTableAddCol]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE tableIdentifier [PARTITION spec]
   *    ADD COLUMNS (name type [COMMENT comment], ...) [CASCADE|RESTRICT]
   * }}}
   */
  override def visitAddColumns(ctx: AddColumnsContext): LogicalPlan = withOrigin(ctx) {
    AlterTableAddCol(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      createStructType(ctx.colTypeList),
      // Note that Restrict and Cascade are mutually exclusive.
      ctx.RESTRICT != null,
      ctx.CASCADE != null)(
      command(ctx))
  }

  /**
   * Create an [[AlterTableReplaceCol]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE tableIdentifier [PARTITION spec]
   *    REPLACE COLUMNS (name type [COMMENT comment], ...) [CASCADE|RESTRICT]
   * }}}
   */
  override def visitReplaceColumns(ctx: ReplaceColumnsContext): LogicalPlan = withOrigin(ctx) {
    AlterTableReplaceCol(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec),
      createStructType(ctx.colTypeList),
      // Note that Restrict and Cascade are mutually exclusive.
      ctx.RESTRICT != null,
      ctx.CASCADE != null)(
      command(ctx))
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
      Option(ctx.orderedIdentifierList).toSeq
        .flatMap(_.orderedIdentifier.asScala)
        .map(_.identifier.getText))
  }

  /**
   * Create a skew specification. This contains three components:
   * - The Skewed Columns
   * - Values for which are skewed. The size of each entry must match the number of skewed columns.
   * - A store in directory flag.
   */
  override def visitSkewSpec(
      ctx: SkewSpecContext): (Seq[String], Seq[Seq[String]], Boolean) = withOrigin(ctx) {
    val skewedValues = if (ctx.constantList != null) {
      Seq(visitConstantList(ctx.constantList))
    } else {
      visitNestedConstantList(ctx.nestedConstantList)
    }
    (visitIdentifierList(ctx.identifierList), skewedValues, ctx.DIRECTORIES != null)
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
}
