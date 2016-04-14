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

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser._
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
      logWarning("Unsupported operation: EXPLAIN FORMATTED option")
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
      throw new ParseException("Unsupported operation: EXTERNAL option", ctx)
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
   * Create a [[DropTable]] command.
   */
  override def visitDropTable(ctx: DropTableContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.PURGE != null) {
      throw new ParseException("Unsupported operation: PURGE option", ctx)
    }
    if (ctx.REPLICATION != null) {
      throw new ParseException("Unsupported operation: REPLICATION clause", ctx)
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

  // TODO: don't even bother parsing alter table commands related to bucketing and skewing

  override def visitBucketTable(ctx: BucketTableContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException(
      "Operation not allowed: ALTER TABLE ... CLUSTERED BY ... INTO N BUCKETS")
  }

  override def visitUnclusterTable(ctx: UnclusterTableContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... NOT CLUSTERED")
  }

  override def visitUnsortTable(ctx: UnsortTableContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... NOT SORTED")
  }

  override def visitSkewTable(ctx: SkewTableContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... SKEWED BY ...")
  }

  override def visitUnskewTable(ctx: UnskewTableContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... NOT SKEWED")
  }

  override def visitUnstoreTable(ctx: UnstoreTableContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException(
      "Operation not allowed: ALTER TABLE ... NOT STORED AS DIRECTORIES")
  }

  override def visitSetTableSkewLocations(
      ctx: SetTableSkewLocationsContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException(
      "Operation not allowed: ALTER TABLE ... SET SKEWED LOCATION ...")
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
      throw new AnalysisException(s"Operation not allowed: partitioned views")
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
   * Create an [[AlterTableExchangePartition]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 EXCHANGE PARTITION spec WITH TABLE table2;
   * }}}
   */
  override def visitExchangeTablePartition(
      ctx: ExchangeTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    throw new AnalysisException(
      "Operation not allowed: ALTER TABLE ... EXCHANGE PARTITION ...")
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
      throw new AnalysisException(s"Operation not allowed: partitioned views")
    }
    if (ctx.PURGE != null) {
      throw new AnalysisException(s"Operation not allowed: PURGE")
    }
    AlterTableDropPartition(
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.partitionSpec.asScala.map(visitNonOptionalPartitionSpec),
      ctx.EXISTS != null)
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
    throw new AnalysisException(
      "Operation not allowed: ALTER TABLE ... ARCHIVE PARTITION ...")
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
    throw new AnalysisException(
      "Operation not allowed: ALTER TABLE ... UNARCHIVE PARTITION ...")
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
        val elements = Seq(s.inFmt, s.outFmt) ++ Option(s.serdeCls).toSeq
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
      visitLocationSpec(ctx.locationSpec))
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
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... TOUCH ...")
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
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... COMPACT ...")
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
    throw new AnalysisException("Operation not allowed: ALTER TABLE ... CONCATENATE")
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
