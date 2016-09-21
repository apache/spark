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

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, ScriptInputOutputSchema}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, _}
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.StructType

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
   * Create an [[AnalyzeTableCommand]] command or an [[AnalyzeColumnCommand]] command.
   * Example SQL for analyzing table :
   * {{{
   *   ANALYZE TABLE table COMPUTE STATISTICS [NOSCAN];
   * }}}
   * Example SQL for analyzing columns :
   * {{{
   *   ANALYZE TABLE table COMPUTE STATISTICS FOR COLUMNS column1, column2;
   * }}}
   */
  override def visitAnalyze(ctx: AnalyzeContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.partitionSpec == null &&
      ctx.identifier != null &&
      ctx.identifier.getText.toLowerCase == "noscan") {
      AnalyzeTableCommand(visitTableIdentifier(ctx.tableIdentifier))
    } else if (ctx.identifierSeq() == null) {
      AnalyzeTableCommand(visitTableIdentifier(ctx.tableIdentifier), noscan = false)
    } else {
      AnalyzeColumnCommand(
        visitTableIdentifier(ctx.tableIdentifier),
        visitIdentifierSeq(ctx.identifierSeq()))
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
        operationNotAllowed(
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
   * Creates a [[ShowCreateTableCommand]]
   */
  override def visitShowCreateTable(ctx: ShowCreateTableContext): LogicalPlan = withOrigin(ctx) {
    val table = visitTableIdentifier(ctx.tableIdentifier())
    ShowCreateTableCommand(table)
  }

  /**
   * Create a [[RefreshTable]] logical plan.
   */
  override def visitRefreshTable(ctx: RefreshTableContext): LogicalPlan = withOrigin(ctx) {
    RefreshTable(visitTableIdentifier(ctx.tableIdentifier))
  }

  /**
   * Create a [[RefreshTable]] logical plan.
   */
  override def visitRefreshResource(ctx: RefreshResourceContext): LogicalPlan = withOrigin(ctx) {
    val resourcePath = remainder(ctx.REFRESH.getSymbol).trim
    RefreshResource(resourcePath)
  }

  /**
   * Create a [[CacheTableCommand]] logical plan.
   */
  override def visitCacheTable(ctx: CacheTableContext): LogicalPlan = withOrigin(ctx) {
    val query = Option(ctx.query).map(plan)
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    if (query.isDefined && tableIdent.database.isDefined) {
      val database = tableIdent.database.get
      throw new ParseException(s"It is not allowed to add database prefix `$database` to " +
        s"the table name in CACHE TABLE AS SELECT", ctx)
    }
    CacheTableCommand(tableIdent, query, ctx.LAZY != null)
  }

  /**
   * Create an [[UncacheTableCommand]] logical plan.
   */
  override def visitUncacheTable(ctx: UncacheTableContext): LogicalPlan = withOrigin(ctx) {
    UncacheTableCommand(visitTableIdentifier(ctx.tableIdentifier))
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
   *   EXPLAIN (EXTENDED | CODEGEN) SELECT * FROM ...
   * }}}
   */
  override def visitExplain(ctx: ExplainContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.FORMATTED != null) {
      operationNotAllowed("EXPLAIN FORMATTED", ctx)
    }
    if (ctx.LOGICAL != null) {
      operationNotAllowed("EXPLAIN LOGICAL", ctx)
    }

    val statement = plan(ctx.statement)
    if (statement == null) {
      null  // This is enough since ParseException will raise later.
    } else if (isExplainableStatement(statement)) {
      ExplainCommand(statement, extended = ctx.EXTENDED != null, codegen = ctx.CODEGEN != null)
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
    // Describe column are not supported yet. Return null and let the parser decide
    // what to do with this (create an exception or pass it on to a different system).
    if (ctx.describeColName != null) {
      null
    } else {
      val partitionSpec = if (ctx.partitionSpec != null) {
        // According to the syntax, visitPartitionSpec returns `Map[String, Option[String]]`.
        visitPartitionSpec(ctx.partitionSpec).map {
          case (key, Some(value)) => key -> value
          case (key, _) =>
            throw new ParseException(s"PARTITION specification is incomplete: `$key`", ctx)
        }
      } else {
        Map.empty[String, String]
      }
      DescribeTableCommand(
        visitTableIdentifier(ctx.tableIdentifier),
        partitionSpec,
        ctx.EXTENDED != null,
        ctx.FORMATTED != null)
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
      operationNotAllowed("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    (visitTableIdentifier(ctx.tableIdentifier), temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
   * Create a [[CreateTable]] logical plan.
   */
  override def visitCreateTableUsing(ctx: CreateTableUsingContext): LogicalPlan = withOrigin(ctx) {
    val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
    if (external) {
      operationNotAllowed("CREATE EXTERNAL TABLE ... USING", ctx)
    }
    val options = Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val provider = ctx.tableProvider.qualifiedName.getText
    if (provider.toLowerCase == "hive") {
      throw new AnalysisException("Cannot create hive serde table with CREATE TABLE USING")
    }
    val schema = Option(ctx.colTypeList()).map(createSchema)
    val partitionColumnNames =
      Option(ctx.partitionColumnNames)
        .map(visitIdentifierList(_).toArray)
        .getOrElse(Array.empty[String])
    val bucketSpec = Option(ctx.bucketSpec()).map(visitBucketSpec)

    // TODO: this may be wrong for non file-based data source like JDBC, which should be external
    // even there is no `path` in options. We should consider allow the EXTERNAL keyword.
    val tableType = if (new CaseInsensitiveMap(options).contains("path")) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    val tableDesc = CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = CatalogStorageFormat.empty.copy(properties = options),
      schema = schema.getOrElse(new StructType),
      provider = Some(provider),
      partitionColumnNames = partitionColumnNames,
      bucketSpec = bucketSpec
    )

    // Determine the storage mode.
    val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

    if (ctx.query != null) {
      // Get the backing query.
      val query = plan(ctx.query)

      if (temp) {
        operationNotAllowed("CREATE TEMPORARY TABLE ... USING ... AS query", ctx)
      }

      CreateTable(tableDesc, mode, Some(query))
    } else {
      if (temp) {
        if (ifNotExists) {
          operationNotAllowed("CREATE TEMPORARY TABLE IF NOT EXISTS", ctx)
        }

        logWarning(s"CREATE TEMPORARY TABLE ... USING ... is deprecated, please use " +
          "CREATE TEMPORARY VIEW ... USING ... instead")
        CreateTempViewUsing(table, schema, replace = true, global = false, provider, options)
      } else {
        CreateTable(tableDesc, mode, None)
      }
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
      provider = ctx.tableProvider.qualifiedName.getText,
      options = Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Create a [[LoadDataCommand]] command.
   *
   * For example:
   * {{{
   *   LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
   *   [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitLoadData(ctx: LoadDataContext): LogicalPlan = withOrigin(ctx) {
    LoadDataCommand(
      table = visitTableIdentifier(ctx.tableIdentifier),
      path = string(ctx.path),
      isLocal = ctx.LOCAL != null,
      isOverwrite = ctx.OVERWRITE != null,
      partition = Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec)
    )
  }

  /**
   * Create a [[TruncateTableCommand]] command.
   *
   * For example:
   * {{{
   *   TRUNCATE TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
   * }}}
   */
  override def visitTruncateTable(ctx: TruncateTableContext): LogicalPlan = withOrigin(ctx) {
    TruncateTableCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
  }

  /**
   * Create a [[AlterTableRecoverPartitionsCommand]] command.
   *
   * For example:
   * {{{
   *   MSCK REPAIR TABLE tablename
   * }}}
   */
  override def visitRepairTable(ctx: RepairTableContext): LogicalPlan = withOrigin(ctx) {
    AlterTableRecoverPartitionsCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      "MSCK REPAIR TABLE")
  }

  /**
   * Convert a table property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitTablePropertyList(
      ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.tableProperty.asScala.map { property =>
      val key = visitTablePropertyKey(property.key)
      val value = visitTablePropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  private def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[TablePropertyListContext]], assuming no values are specified.
   */
  private def visitPropertyKeys(ctx: TablePropertyListContext): Seq[String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
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
   * A table property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitTablePropertyValue(value: TablePropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase
    } else {
      value.getText
    }
  }

  /**
   * Create a [[CreateDatabaseCommand]] command.
   *
   * For example:
   * {{{
   *   CREATE DATABASE [IF NOT EXISTS] database_name [COMMENT database_comment]
   *    [LOCATION path] [WITH DBPROPERTIES (key1=val1, key2=val2, ...)]
   * }}}
   */
  override def visitCreateDatabase(ctx: CreateDatabaseContext): LogicalPlan = withOrigin(ctx) {
    CreateDatabaseCommand(
      ctx.identifier.getText,
      ctx.EXISTS != null,
      Option(ctx.locationSpec).map(visitLocationSpec),
      Option(ctx.comment).map(string),
      Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Create an [[AlterDatabasePropertiesCommand]] command.
   *
   * For example:
   * {{{
   *   ALTER (DATABASE|SCHEMA) database SET DBPROPERTIES (property_name=property_value, ...);
   * }}}
   */
  override def visitSetDatabaseProperties(
      ctx: SetDatabasePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterDatabasePropertiesCommand(
      ctx.identifier.getText,
      visitPropertyKeyValues(ctx.tablePropertyList))
  }

  /**
   * Create a [[DropDatabaseCommand]] command.
   *
   * For example:
   * {{{
   *   DROP (DATABASE|SCHEMA) [IF EXISTS] database [RESTRICT|CASCADE];
   * }}}
   */
  override def visitDropDatabase(ctx: DropDatabaseContext): LogicalPlan = withOrigin(ctx) {
    DropDatabaseCommand(ctx.identifier.getText, ctx.EXISTS != null, ctx.CASCADE != null)
  }

  /**
   * Create a [[DescribeDatabaseCommand]] command.
   *
   * For example:
   * {{{
   *   DESCRIBE DATABASE [EXTENDED] database;
   * }}}
   */
  override def visitDescribeDatabase(ctx: DescribeDatabaseContext): LogicalPlan = withOrigin(ctx) {
    DescribeDatabaseCommand(ctx.identifier.getText, ctx.EXTENDED != null)
  }

  /**
   * Create a plan for a DESCRIBE FUNCTION command.
   */
  override def visitDescribeFunction(ctx: DescribeFunctionContext): LogicalPlan = withOrigin(ctx) {
    import ctx._
    val functionName =
      if (describeFuncName.STRING() != null) {
        FunctionIdentifier(string(describeFuncName.STRING()), database = None)
      } else if (describeFuncName.qualifiedName() != null) {
        visitFunctionName(describeFuncName.qualifiedName)
      } else {
        FunctionIdentifier(describeFuncName.getText, database = None)
      }
    DescribeFunctionCommand(functionName, EXTENDED != null)
  }

  /**
   * Create a plan for a SHOW FUNCTIONS command.
   */
  override def visitShowFunctions(ctx: ShowFunctionsContext): LogicalPlan = withOrigin(ctx) {
    import ctx._
    val (user, system) = Option(ctx.identifier).map(_.getText.toLowerCase) match {
      case None | Some("all") => (true, true)
      case Some("system") => (false, true)
      case Some("user") => (true, false)
      case Some(x) => throw new ParseException(s"SHOW $x FUNCTIONS not supported", ctx)
    }

    val (db, pat) = if (qualifiedName != null) {
      val name = visitFunctionName(qualifiedName)
      (name.database, Some(name.funcName))
    } else if (pattern != null) {
      (None, Some(string(pattern)))
    } else {
      (None, None)
    }

    ShowFunctionsCommand(db, pat, user, system)
  }

  /**
   * Create a [[CreateFunctionCommand]] command.
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
          FunctionResource(FunctionResourceType.fromString(resourceType), string(resource.STRING))
        case other =>
          operationNotAllowed(s"CREATE FUNCTION with resource type '$resourceType'", ctx)
      }
    }

    // Extract database, name & alias.
    val functionIdentifier = visitFunctionName(ctx.qualifiedName)
    CreateFunctionCommand(
      functionIdentifier.database,
      functionIdentifier.funcName,
      string(ctx.className),
      resources,
      ctx.TEMPORARY != null)
  }

  /**
   * Create a [[DropFunctionCommand]] command.
   *
   * For example:
   * {{{
   *   DROP [TEMPORARY] FUNCTION [IF EXISTS] function;
   * }}}
   */
  override def visitDropFunction(ctx: DropFunctionContext): LogicalPlan = withOrigin(ctx) {
    val functionIdentifier = visitFunctionName(ctx.qualifiedName)
    DropFunctionCommand(
      functionIdentifier.database,
      functionIdentifier.funcName,
      ctx.EXISTS != null,
      ctx.TEMPORARY != null)
  }

  /**
   * Create a [[DropTableCommand]] command.
   */
  override def visitDropTable(ctx: DropTableContext): LogicalPlan = withOrigin(ctx) {
    DropTableCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.EXISTS != null,
      ctx.VIEW != null,
      ctx.PURGE != null)
  }

  /**
   * Create a [[AlterTableRenameCommand]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table1 RENAME TO table2;
   *   ALTER VIEW view1 RENAME TO view2;
   * }}}
   */
  override def visitRenameTable(ctx: RenameTableContext): LogicalPlan = withOrigin(ctx) {
    val fromName = visitTableIdentifier(ctx.from)
    val toName = visitTableIdentifier(ctx.to)
    if (toName.database.isDefined) {
      operationNotAllowed("Can not specify database in table/view name after RENAME TO", ctx)
    }

    AlterTableRenameCommand(
      fromName,
      toName.table,
      ctx.VIEW != null)
  }

  /**
   * Create an [[AlterTableSetPropertiesCommand]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table SET TBLPROPERTIES ('comment' = new_comment);
   *   ALTER VIEW view SET TBLPROPERTIES ('comment' = new_comment);
   * }}}
   */
  override def visitSetTableProperties(
      ctx: SetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterTableSetPropertiesCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      visitPropertyKeyValues(ctx.tablePropertyList),
      ctx.VIEW != null)
  }

  /**
   * Create an [[AlterTableUnsetPropertiesCommand]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
   *   ALTER VIEW view UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
   * }}}
   */
  override def visitUnsetTableProperties(
      ctx: UnsetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    AlterTableUnsetPropertiesCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      visitPropertyKeys(ctx.tablePropertyList),
      ctx.EXISTS != null,
      ctx.VIEW != null)
  }

  /**
   * Create an [[AlterTableSerDePropertiesCommand]] command.
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
   *   ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
   * }}}
   */
  override def visitSetTableSerDe(ctx: SetTableSerDeContext): LogicalPlan = withOrigin(ctx) {
    AlterTableSerDePropertiesCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.STRING).map(string),
      Option(ctx.tablePropertyList).map(visitPropertyKeyValues),
      // TODO a partition spec is allowed to have optional values. This is currently violated.
      Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec))
  }

  /**
   * Create an [[AlterTableAddPartitionCommand]] command.
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
      operationNotAllowed("ALTER VIEW ... ADD PARTITION", ctx)
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
    AlterTableAddPartitionCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      specsAndLocs,
      ctx.EXISTS != null)
  }

  /**
   * Create an [[AlterTableRenamePartitionCommand]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2;
   * }}}
   */
  override def visitRenameTablePartition(
      ctx: RenameTablePartitionContext): LogicalPlan = withOrigin(ctx) {
    AlterTableRenamePartitionCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      visitNonOptionalPartitionSpec(ctx.from),
      visitNonOptionalPartitionSpec(ctx.to))
  }

  /**
   * Create an [[AlterTableDropPartitionCommand]] command
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
      operationNotAllowed("ALTER VIEW ... DROP PARTITION", ctx)
    }
    AlterTableDropPartitionCommand(
      visitTableIdentifier(ctx.tableIdentifier),
      ctx.partitionSpec.asScala.map(visitNonOptionalPartitionSpec),
      ctx.EXISTS != null,
      ctx.PURGE != null)
  }

  /**
   * Create an [[AlterTableRecoverPartitionsCommand]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table RECOVER PARTITIONS;
   * }}}
   */
  override def visitRecoverPartitions(
      ctx: RecoverPartitionsContext): LogicalPlan = withOrigin(ctx) {
    AlterTableRecoverPartitionsCommand(visitTableIdentifier(ctx.tableIdentifier))
  }

  /**
   * Create an [[AlterTableSetLocationCommand]] command
   *
   * For example:
   * {{{
   *   ALTER TABLE table [PARTITION spec] SET LOCATION "loc";
   * }}}
   */
  override def visitSetTableLocation(ctx: SetTableLocationContext): LogicalPlan = withOrigin(ctx) {
    AlterTableSetLocationCommand(
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
              operationNotAllowed(s"Column ordering must be ASC, was '$dir'", ctx)
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
   */
  override def visitManageResource(ctx: ManageResourceContext): LogicalPlan = withOrigin(ctx) {
    val mayebePaths = remainder(ctx.identifier).trim
    ctx.op.getType match {
      case SqlBaseParser.ADD =>
        ctx.identifier.getText.toLowerCase match {
          case "file" => AddFileCommand(mayebePaths)
          case "jar" => AddJarCommand(mayebePaths)
          case other => operationNotAllowed(s"ADD with resource type '$other'", ctx)
        }
      case SqlBaseParser.LIST =>
        ctx.identifier.getText.toLowerCase match {
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
   * Create a table, returning a [[CreateTable]] logical plan.
   *
   * This is not used to create datasource tables, which is handled through
   * "CREATE TABLE ... USING ...".
   *
   * Note: several features are currently not supported - temporary tables, bucketing,
   * skewed columns and storage handlers (STORED BY).
   *
   * Expected format:
   * {{{
   *   CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
   *   [(col1[:] data_type [COMMENT col_comment], ...)]
   *   [COMMENT table_comment]
   *   [PARTITIONED BY (col2[:] data_type [COMMENT col_comment], ...)]
   *   [ROW FORMAT row_format]
   *   [STORED AS file_format]
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
          "Please use CREATE TEMPORARY VIEW as an alternative.", ctx)
    }
    if (ctx.skewSpec != null) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
    }
    if (ctx.bucketSpec != null) {
      operationNotAllowed("CREATE TABLE ... CLUSTERED BY", ctx)
    }
    val comment = Option(ctx.STRING).map(string)
    val dataCols = Option(ctx.columns).map(visitColTypeList).getOrElse(Nil)
    val partitionCols = Option(ctx.partitionColumns).map(visitColTypeList).getOrElse(Nil)
    val properties = Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val selectQuery = Option(ctx.query).map(plan)

    // Note: Hive requires partition columns to be distinct from the schema, so we need
    // to include the partition columns here explicitly
    val schema = StructType(dataCols ++ partitionCols)

    // Storage format
    val defaultStorage: CatalogStorageFormat = {
      val defaultStorageType = conf.getConfString("hive.default.fileformat", "textfile")
      val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType)
      CatalogStorageFormat(
        locationUri = None,
        inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
          .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
        outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
          .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
        serde = defaultHiveSerde.flatMap(_.serde),
        compressed = false,
        properties = Map())
    }
    validateRowFormatFileFormat(ctx.rowFormat, ctx.createFileFormat, ctx)
    val fileStorage = Option(ctx.createFileFormat).map(visitCreateFileFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val rowStorage = Option(ctx.rowFormat).map(visitRowFormat)
      .getOrElse(CatalogStorageFormat.empty)
    val location = Option(ctx.locationSpec).map(visitLocationSpec)
    // If we are creating an EXTERNAL table, then the LOCATION field is required
    if (external && location.isEmpty) {
      operationNotAllowed("CREATE EXTERNAL TABLE must be accompanied by LOCATION", ctx)
    }
    val storage = CatalogStorageFormat(
      locationUri = location,
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

    // TODO support the sql text - have a proper location for this!
    val tableDesc = CatalogTable(
      identifier = name,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some("hive"),
      partitionColumnNames = partitionCols.map(_.name),
      properties = properties,
      comment = comment)

    val mode = if (ifNotExists) SaveMode.Ignore else SaveMode.ErrorIfExists

    selectQuery match {
      case Some(q) =>
        // Hive does not allow to use a CTAS statement to create a partitioned table.
        if (tableDesc.partitionColumnNames.nonEmpty) {
          val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
            "create a partitioned table using Hive's file formats. " +
            "Please use the syntax of \"CREATE TABLE tableName USING dataSource " +
            "OPTIONS (...) PARTITIONED BY ...\" to create a partitioned table through a " +
            "CTAS statement."
          operationNotAllowed(errorMessage, ctx)
        }
        // Just use whatever is projected in the select statement as our schema
        if (schema.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement",
            ctx)
        }

        val hasStorageProperties = (ctx.createFileFormat != null) || (ctx.rowFormat != null)
        if (conf.convertCTAS && !hasStorageProperties) {
          // At here, both rowStorage.serdeProperties and fileStorage.serdeProperties
          // are empty Maps.
          val optionsWithPath = if (location.isDefined) {
            Map("path" -> location.get)
          } else {
            Map.empty[String, String]
          }

          val newTableDesc = tableDesc.copy(
            storage = CatalogStorageFormat.empty.copy(properties = optionsWithPath),
            provider = Some(conf.defaultDataSourceName)
          )

          CreateTable(newTableDesc, mode, Some(q))
        } else {
          CreateTable(tableDesc, mode, Some(q))
        }
      case None => CreateTable(tableDesc, mode, None)
    }
  }

  /**
   * Create a [[CreateTableLikeCommand]] command.
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
    CreateTableLikeCommand(targetTable, sourceTable, ctx.EXISTS != null)
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
        ffGeneric.identifier.getText.toLowerCase match {
          case ("sequencefile" | "textfile" | "rcfile") => // OK
          case fmt =>
            operationNotAllowed(
              s"ROW FORMAT SERDE is incompatible with format '$fmt', which also specifies a serde",
              parentCtx)
        }
      case (rfDelimited: RowFormatDelimitedContext, ffGeneric: GenericFileFormatContext) =>
        ffGeneric.identifier.getText.toLowerCase match {
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

  /**
   * Create or replace a view. This creates a [[CreateViewCommand]] command.
   *
   * For example:
   * {{{
   *   CREATE [OR REPLACE] [[GLOBAL] TEMPORARY] VIEW [IF NOT EXISTS] [db_name.]view_name
   *   [(column_name [COMMENT column_comment], ...) ]
   *   [COMMENT view_comment]
   *   [TBLPROPERTIES (property_name = property_value, ...)]
   *   AS SELECT ...;
   * }}}
   */
  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.identifierList != null) {
      operationNotAllowed("CREATE VIEW ... PARTITIONED ON", ctx)
    } else {
      val userSpecifiedColumns = Option(ctx.identifierCommentList).toSeq.flatMap { icl =>
        icl.identifierComment.asScala.map { ic =>
          ic.identifier.getText -> Option(ic.STRING).map(string)
        }
      }

      val viewType = if (ctx.TEMPORARY == null) {
        PersistedView
      } else if (ctx.GLOBAL != null) {
        GlobalTempView
      } else {
        LocalTempView
      }

      CreateViewCommand(
        name = visitTableIdentifier(ctx.tableIdentifier),
        userSpecifiedColumns = userSpecifiedColumns,
        comment = Option(ctx.STRING).map(string),
        properties = Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty),
        originalText = Option(source(ctx.query)),
        child = plan(ctx.query),
        allowExisting = ctx.EXISTS != null,
        replace = ctx.REPLACE != null,
        viewType = viewType)
    }
  }

  /**
   * Alter the query of a view. This creates a [[AlterViewAsCommand]] command.
   *
   * For example:
   * {{{
   *   ALTER VIEW [db_name.]view_name AS SELECT ...;
   * }}}
   */
  override def visitAlterViewQuery(ctx: AlterViewQueryContext): LogicalPlan = withOrigin(ctx) {
    AlterViewAsCommand(
      name = visitTableIdentifier(ctx.tableIdentifier),
      originalText = source(ctx.query),
      query = plan(ctx.query))
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
    def format(
        fmt: RowFormatContext,
        configKey: String,
        defaultConfigValue: String): Format = fmt match {
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
}
