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

package org.apache.spark.sql.hive

import java.io.IOException
import java.util.{List => JList}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition => TPartition, Table => TTable}
import org.apache.hadoop.hive.metastore.{TableType, Warehouse}
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.ql.plan.CreateTableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeException}
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, Catalog, OverrideCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.parquet.{ParquetRelation2, Partition => ParquetPartition, PartitionSpec}
import org.apache.spark.sql.sources.{CreateTableUsingAsSelect, DDLParser, LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] class HiveMetastoreCatalog(hive: HiveContext) extends Catalog with Logging {
  import org.apache.spark.sql.hive.HiveMetastoreTypes._

  /** Connection to hive metastore.  Usages should lock on `this`. */
  protected[hive] val client = Hive.get(hive.hiveconf)

  /** Usages should lock on `this`. */
  protected[hive] lazy val hiveWarehouse = new Warehouse(hive.hiveconf)

  // TODO: Use this everywhere instead of tuples or databaseName, tableName,.
  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String) {
    def toLowerCase = QualifiedTableName(database.toLowerCase, name.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = synchronized {
          client.getTable(in.database, in.name)
        }
        val userSpecifiedSchema =
          Option(table.getProperty("spark.sql.sources.schema.numParts")).map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.getProperty(s"spark.sql.sources.schema.part.${index}")
              if (part == null) {
                throw new AnalysisException(
                  s"Could not read schema from the metastore because it is corrupted " +
                  s"(missing part ${index} of the schema).")
              }

              part
            }
            // Stick all parts back to a single schema string in the JSON representation
            // and convert it back to a StructType.
            DataType.fromJson(parts.mkString).asInstanceOf[StructType]
          }

        // It does not appear that the ql client for the metastore has a way to enumerate all the
        // SerDe properties directly...
        val options = table.getTTable.getSd.getSerdeInfo.getParameters.toMap

        val resolvedRelation =
          ResolvedDataSource(
            hive,
            userSpecifiedSchema,
            table.getProperty("spark.sql.sources.provider"),
            options)

        LogicalRelation(resolvedRelation.relation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def refreshTable(databaseName: String, tableName: String): Unit = {
    cachedDataSourceTables.refresh(QualifiedTableName(databaseName, tableName).toLowerCase)
  }

  def invalidateTable(databaseName: String, tableName: String): Unit = {
    cachedDataSourceTables.invalidate(QualifiedTableName(databaseName, tableName).toLowerCase)
  }

  val caseSensitive: Boolean = false

  /**
   * Creates a data source table (a table created with USING clause) in Hive's metastore.
   * Returns true when the table has been created. Otherwise, false.
   */
  def createDataSourceTable(
      tableName: String,
      userSpecifiedSchema: Option[StructType],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName("default", tableName)
    val tbl = new Table(dbName, tblName)

    tbl.setProperty("spark.sql.sources.provider", provider)
    if (userSpecifiedSchema.isDefined) {
      val threshold = hive.conf.schemaStringLengthThreshold
      val schemaJsonString = userSpecifiedSchema.get.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tbl.setProperty("spark.sql.sources.schema.numParts", parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tbl.setProperty(s"spark.sql.sources.schema.part.${index}", part)
      }
    }
    options.foreach { case (key, value) => tbl.setSerdeParam(key, value) }

    if (isExternal) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    } else {
      tbl.setProperty("EXTERNAL", "FALSE")
      tbl.setTableType(TableType.MANAGED_TABLE)
    }

    // create the table
    synchronized {
      client.createTable(tbl, false)
    }
  }

  def hiveDefaultTableFilePath(tableName: String): String = synchronized {
    val currentDatabase = client.getDatabase(hive.sessionState.getCurrentDatabase)

    hiveWarehouse.getTablePath(currentDatabase, tableName).toString
  }

  def tableExists(tableIdentifier: Seq[String]): Boolean = synchronized {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName =
      tableIdent
        .lift(tableIdent.size - 2)
        .getOrElse(hive.sessionState.getCurrentDatabase)
    val tblName = tableIdent.last
    client.getTable(databaseName, tblName, false) != null
  }

  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = synchronized {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
      hive.sessionState.getCurrentDatabase)
    val tblName = tableIdent.last
    val table = try client.getTable(databaseName, tblName) catch {
      case te: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
        throw new NoSuchTableException
    }

    if (table.getProperty("spark.sql.sources.provider") != null) {
      val dataSourceTable =
        cachedDataSourceTables(QualifiedTableName(databaseName, tblName).toLowerCase)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Othersie, wrap the table with a Subquery using the table name.
      val withAlias =
        alias.map(a => Subquery(a, dataSourceTable)).getOrElse(
          Subquery(tableIdent.last, dataSourceTable))

      withAlias
    } else if (table.isView) {
      // if the unresolved relation is from hive view
      // parse the text into logic node.
      HiveQl.createPlanForView(table, alias)
    } else {
      val partitions: Seq[Partition] =
        if (table.isPartitioned) {
          HiveShim.getAllPartitionsOf(client, table).toSeq
        } else {
          Nil
        }

      MetastoreRelation(databaseName, tblName, alias)(
        table.getTTable, partitions.map(part => part.getTPartition))(hive)
    }
  }

  private def convertToParquetRelation(metastoreRelation: MetastoreRelation): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)

    // NOTE: Instead of passing Metastore schema directly to `ParquetRelation2`, we have to
    // serialize the Metastore schema to JSON and pass it as a data source option because of the
    // evil case insensitivity issue, which is reconciled within `ParquetRelation2`.
    if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      val partitions = metastoreRelation.hiveQlPartitions.map { p =>
        val location = p.getLocation
        val values = Row.fromSeq(p.getValues.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        ParquetPartition(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val paths = partitions.map(_.path)
      LogicalRelation(
        ParquetRelation2(
          paths,
          Map(ParquetRelation2.METASTORE_SCHEMA -> metastoreSchema.json),
          None,
          Some(partitionSpec))(hive))
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)
      LogicalRelation(
        ParquetRelation2(
          paths,
          Map(ParquetRelation2.METASTORE_SCHEMA -> metastoreSchema.json))(hive))
    }
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = synchronized {
    val dbName = if (!caseSensitive) {
      if (databaseName.isDefined) Some(databaseName.get.toLowerCase) else None
    } else {
      databaseName
    }
    val db = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

    client.getAllTables(db).map(tableName => (tableName, false))
  }

  /**
   * Create table with specified database, table name, table description and schema
   * @param databaseName Database Name
   * @param tableName Table Name
   * @param schema Schema of the new table, if not specified, will use the schema
   *               specified in crtTbl
   * @param allowExisting if true, ignore AlreadyExistsException
   * @param desc CreateTableDesc object which contains the SerDe info. Currently
   *               we support most of the features except the bucket.
   */
  def createTable(
      databaseName: String,
      tableName: String,
      schema: Seq[Attribute],
      allowExisting: Boolean = false,
      desc: Option[CreateTableDesc] = None) {
    val hconf = hive.hiveconf

    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val tbl = new Table(dbName, tblName)

    val crtTbl: CreateTableDesc = desc.getOrElse(null)

    // We should respect the passed in schema, unless it's not set
    val hiveSchema: JList[FieldSchema] = if (schema == null || schema.isEmpty) {
      crtTbl.getCols
    } else {
      schema.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), null))
    }
    tbl.setFields(hiveSchema)

    // Most of code are similar with the DDLTask.createTable() of Hive,
    if (crtTbl != null && crtTbl.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtTbl.getTblProps())
    }

    if (crtTbl != null && crtTbl.getPartCols() != null) {
      tbl.setPartCols(crtTbl.getPartCols())
    }

    if (crtTbl != null && crtTbl.getStorageHandler() != null) {
      tbl.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
        crtTbl.getStorageHandler())
    }

    /*
     * We use LazySimpleSerDe by default.
     *
     * If the user didn't specify a SerDe, and any of the columns are not simple
     * types, we will have to use DynamicSerDe instead.
     */
    if (crtTbl == null || crtTbl.getSerName() == null) {
      val storageHandler = tbl.getStorageHandler()
      if (storageHandler == null) {
        logInfo(s"Default to LazySimpleSerDe for table $dbName.$tblName")
        tbl.setSerializationLib(classOf[LazySimpleSerDe].getName())

        import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
        import org.apache.hadoop.io.Text
        import org.apache.hadoop.mapred.TextInputFormat

        tbl.setInputFormatClass(classOf[TextInputFormat])
        tbl.setOutputFormatClass(classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]])
        tbl.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
      } else {
        val serDeClassName = storageHandler.getSerDeClass().getName()
        logInfo(s"Use StorageHandler-supplied $serDeClassName for table $dbName.$tblName")
        tbl.setSerializationLib(serDeClassName)
      }
    } else {
      // let's validate that the serde exists
      val serdeName = crtTbl.getSerName()
      try {
        val d = ReflectionUtils.newInstance(hconf.getClassByName(serdeName), hconf)
        if (d != null) {
          logDebug("Found class for $serdeName")
        }
      } catch {
        case e: SerDeException => throw new HiveException("Cannot validate serde: " + serdeName, e)
      }
      tbl.setSerializationLib(serdeName)
    }

    if (crtTbl != null && crtTbl.getFieldDelim() != null) {
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, crtTbl.getFieldDelim())
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, crtTbl.getFieldDelim())
    }
    if (crtTbl != null && crtTbl.getFieldEscape() != null) {
      tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR, crtTbl.getFieldEscape())
    }

    if (crtTbl != null && crtTbl.getCollItemDelim() != null) {
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, crtTbl.getCollItemDelim())
    }
    if (crtTbl != null && crtTbl.getMapKeyDelim() != null) {
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, crtTbl.getMapKeyDelim())
    }
    if (crtTbl != null && crtTbl.getLineDelim() != null) {
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, crtTbl.getLineDelim())
    }
    HiveShim.setTblNullFormat(crtTbl, tbl)

    if (crtTbl != null && crtTbl.getSerdeProps() != null) {
      val iter = crtTbl.getSerdeProps().entrySet().iterator()
      while (iter.hasNext()) {
        val m = iter.next()
        tbl.setSerdeParam(m.getKey(), m.getValue())
      }
    }

    if (crtTbl != null && crtTbl.getComment() != null) {
      tbl.setProperty("comment", crtTbl.getComment())
    }

    if (crtTbl != null && crtTbl.getLocation() != null) {
      HiveShim.setLocation(tbl, crtTbl)
    }

    if (crtTbl != null && crtTbl.getSkewedColNames() != null) {
      tbl.setSkewedColNames(crtTbl.getSkewedColNames())
    }
    if (crtTbl != null && crtTbl.getSkewedColValues() != null) {
      tbl.setSkewedColValues(crtTbl.getSkewedColValues())
    }

    if (crtTbl != null) {
      tbl.setStoredAsSubDirectories(crtTbl.isStoredAsSubDirectories())
      tbl.setInputFormatClass(crtTbl.getInputFormat())
      tbl.setOutputFormatClass(crtTbl.getOutputFormat())
    }

    tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName())
    tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName())

    if (crtTbl != null && crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    }

    // set owner
    try {
      tbl.setOwner(hive.hiveconf.getUser)
    } catch {
      case e: IOException => throw new HiveException("Unable to get current user", e)
    }

    // set create time
    tbl.setCreateTime((System.currentTimeMillis() / 1000).asInstanceOf[Int])

    // TODO add bucket support
    // TODO set more info if Hive upgrade

    // create the table
    synchronized {
      try client.createTable(tbl, allowExisting) catch {
        case e: org.apache.hadoop.hive.metastore.api.AlreadyExistsException
          if allowExisting => // Do nothing
        case e: Throwable => throw e
      }
    }
  }

  protected def processDatabaseAndTableName(
      databaseName: Option[String],
      tableName: String): (Option[String], String) = {
    if (!caseSensitive) {
      (databaseName.map(_.toLowerCase), tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  protected def processDatabaseAndTableName(
      databaseName: String,
      tableName: String): (String, String) = {
    if (!caseSensitive) {
      (databaseName.toLowerCase, tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   *
   * This rule can be considered as [[HiveStrategies.ParquetConversion]] done right.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved) {
        return plan
      }

      // Collects all `MetastoreRelation`s which should be replaced
      val toBeReplaced = plan.collect {
        // Write path
        case InsertIntoTable(relation: MetastoreRelation, _, _, _)
            // Inserting into partitioned table is not supported in Parquet data source (yet).
            if !relation.hiveQlTable.isPartitioned &&
              hive.convertMetastoreParquet &&
              hive.conf.parquetUseDataSourceApi &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Write path
        case InsertIntoHiveTable(relation: MetastoreRelation, _, _, _)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !relation.hiveQlTable.isPartitioned &&
            hive.convertMetastoreParquet &&
            hive.conf.parquetUseDataSourceApi &&
            relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Read path
        case p @ PhysicalOperation(_, _, relation: MetastoreRelation)
            if hive.convertMetastoreParquet &&
              hive.conf.parquetUseDataSourceApi &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)
      }

      val relationMap = toBeReplaced.map(r => (r._1, r._2)).toMap
      val attributedRewrites = AttributeMap(toBeReplaced.map(_._3).fold(Nil)(_ ++: _))

      // Replaces all `MetastoreRelation`s with corresponding `ParquetRelation2`s, and fixes
      // attribute IDs referenced in other nodes.
      plan.transformUp {
        case r: MetastoreRelation if relationMap.contains(r) => {
          val parquetRelation = relationMap(r)
          val withAlias =
            r.alias.map(a => Subquery(a, parquetRelation)).getOrElse(
              Subquery(r.tableName, parquetRelation))

          withAlias
        }
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite)
          if relationMap.contains(r) => {
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite)
        }
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite)
          if relationMap.contains(r) => {
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite)
        }
        case other => other.transformExpressions {
          case a: Attribute if a.resolved => attributedRewrites.getOrElse(a, a)
        }
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    import org.apache.hadoop.hive.ql.Context
    import org.apache.hadoop.hive.ql.parse.{ASTNode, QB, SemanticAnalyzer}

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      // TODO extra is in type of ASTNode which means the logical plan is not resolved
      // Need to think about how to implement the CreateTableAsSelect.resolved
      case CreateTableAsSelect(db, tableName, child, allowExisting, Some(extra: ASTNode)) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

        // Get the CreateTableDesc from Hive SemanticAnalyzer
        val desc: Option[CreateTableDesc] = if (tableExists(Seq(databaseName, tblName))) {
          None
        } else {
          val sa = new SemanticAnalyzer(hive.hiveconf) {
            override def analyzeInternal(ast: ASTNode) {
              // A hack to intercept the SemanticAnalyzer.analyzeInternal,
              // to ignore the SELECT clause of the CTAS
              val method = classOf[SemanticAnalyzer].getDeclaredMethod(
                "analyzeCreateTable", classOf[ASTNode], classOf[QB])
              method.setAccessible(true)
              method.invoke(this, ast, this.getQB)
            }
          }

          sa.analyze(extra, new Context(hive.hiveconf))
          Some(sa.getQB().getTableDesc)
        }

        // Check if the query specifies file format or storage handler.
        val hasStorageSpec = desc match {
          case Some(crtTbl) =>
            crtTbl != null && (crtTbl.getSerName != null || crtTbl.getStorageHandler != null)
          case None => false
        }

        if (hive.convertCTAS && !hasStorageSpec) {
          // Do the conversion when spark.sql.hive.convertCTAS is true and the query
          // does not specify any storage format (file format and storage handler).
          if (dbName.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
              "when spark.sql.hive.convertCTAS is set to true.")
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
            tblName,
            hive.conf.defaultDataSourceName,
            temporary = false,
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
          execution.CreateTableAsSelect(
            databaseName,
            tableName,
            child,
            allowExisting,
            desc)
        }

      case p: LogicalPlan if p.resolved => p

      case p @ CreateTableAsSelect(db, tableName, child, allowExisting, None) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        if (hive.convertCTAS) {
          if (dbName.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
              "when spark.sql.hive.convertCTAS is set to true.")
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
            tblName,
            hive.conf.defaultDataSourceName,
            temporary = false,
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
          val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)
          execution.CreateTableAsSelect(
            databaseName,
            tableName,
            child,
            allowExisting,
            None)
        }
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan) = {
      val childOutputDataTypes = child.output.map(_.dataType)
      val tableOutputDataTypes =
        (table.attributes ++ table.partitionKeys).take(child.output.length).map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        p
      } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
        childOutputDataTypes.zip(tableOutputDataTypes)
          .forall { case (left, right) => left.sameType(right) }) {
        // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
        // use InsertIntoHiveTable instead of InsertIntoTable.
        InsertIntoHiveTable(p.table, p.partition, p.child, p.overwrite)
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = ???

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = ???

  override def unregisterAllTables() = {}
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean)
  extends LogicalPlan {

  override def children = child :: Nil
  override def output = child.output

  override lazy val resolved = childrenResolved && child.output.zip(table.output).forall {
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType)
  }
}

private[hive] case class MetastoreRelation
    (databaseName: String, tableName: String, alias: Option[String])
    (val table: TTable, val partitions: Seq[TPartition])
    (@transient sqlContext: SQLContext)
  extends LeafNode {

  self: Product =>

  // TODO: Can we use org.apache.hadoop.hive.ql.metadata.Table as the type of table and
  // use org.apache.hadoop.hive.ql.metadata.Partition as the type of elements of partitions.
  // Right now, using org.apache.hadoop.hive.ql.metadata.Table and
  // org.apache.hadoop.hive.ql.metadata.Partition will cause a NotSerializableException
  // which indicates the SerDe we used is not Serializable.

  @transient val hiveQlTable = new Table(table)

  @transient val hiveQlPartitions = partitions.map { p =>
    new Partition(hiveQlTable, p)
  }

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      val totalSize = hiveQlTable.getParameters.get(HiveShim.getStatsSetupConstTotalSize)
      val rawDataSize = hiveQlTable.getParameters.get(HiveShim.getStatsSetupConstRawDataSize)
      // TODO: check if this estimate is valid for tables after partition pruning.
      // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
      // relatively cheap if parameters for the table are populated into the metastore.  An
      // alternative would be going through Hadoop's FileSystem API, which can be expensive if a lot
      // of RPCs are involved.  Besides `totalSize`, there are also `numFiles`, `numRows`,
      // `rawDataSize` keys (see StatsSetupConst in Hive) that we can look at in the future.
      BigInt(
        // When table is external,`totalSize` is always zero, which will influence join strategy
        // so when `totalSize` is zero, use `rawDataSize` instead
        // if the size is still less than zero, we use default size
        Option(totalSize).map(_.toLong).filter(_ > 0)
          .getOrElse(Option(rawDataSize).map(_.toLong).filter(_ > 0)
          .getOrElse(sqlContext.conf.defaultSizeInBytes)))
    }
  )

  /** Only compare database and tablename, not alias. */
  override def sameResult(plan: LogicalPlan): Boolean = {
    plan match {
      case mr: MetastoreRelation =>
        mr.databaseName == databaseName && mr.tableName == tableName
      case _ => false
    }
  }

  val tableDesc = HiveShim.getTableDesc(
    Class.forName(
      hiveQlTable.getSerializationLib,
      true,
      Utils.getContextOrSparkClassLoader).asInstanceOf[Class[Deserializer]],
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  implicit class SchemaAttribute(f: FieldSchema) {
    def toAttribute = AttributeReference(
      f.getName,
      HiveMetastoreTypes.toDataType(f.getType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = Seq(alias.getOrElse(tableName)))
  }

  // Must be a stable value since new attributes are born here.
  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = hiveQlTable.getCols.map(_.toAttribute)

  val output = attributes ++ partitionKeys

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o,o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex)
}


private[hive] object HiveMetastoreTypes {
  def toDataType(metastoreType: String): DataType = DataTypeParser(metastoreType)

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DateType => "date"
    case d: DecimalType => HiveShim.decimalMetastoreString(d)
    case TimestampType => "timestamp"
    case NullType => "void"
    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
  }
}
