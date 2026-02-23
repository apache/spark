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

package org.apache.spark.sql.internal

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalog.{Catalog, CatalogMetadata, Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * Single implementation of [[Catalog]] for both Classic and Connect.
 * Uses SQL DDL only (SHOW TABLES, DESCRIBE TABLE, etc.) so behavior is identical everywhere.
 *
 * @param session Spark session (Classic or Connect)
 * @param catalogSupport parse/quote and current database/catalog
 *                       (from SessionState or Connect session)
 */
private[sql] class CatalogImpl(
    session: SparkSession,
    catalogSupport: CatalogSupport) extends Catalog {

  private def sql(str: String): DataFrame = session.sql(str)
  private def parseIdent(name: String): Seq[String] = catalogSupport.parseMultipartIdentifier(name)
  private def quote(name: String): String = catalogSupport.quoteIdentifier(name)
  private def quoted(nameParts: Seq[String]): String = nameParts.map(quote).mkString(".")
  private def escapeSingleQuotes(s: String): String = s.replace("'", "''")

  private def makeDataset[T: org.apache.spark.sql.Encoder](data: Seq[T]): Dataset[T] =
    session.createDataset(data)

  private implicit val databaseEncoder: org.apache.spark.sql.Encoder[Database] =
    ScalaReflection.encoderFor(ScalaReflection.localTypeOf[Database])
      .asInstanceOf[org.apache.spark.sql.Encoder[Database]]
  private implicit val tableEncoder: org.apache.spark.sql.Encoder[Table] =
    ScalaReflection.encoderFor(ScalaReflection.localTypeOf[Table])
      .asInstanceOf[org.apache.spark.sql.Encoder[Table]]
  private implicit val functionEncoder: org.apache.spark.sql.Encoder[Function] =
    ScalaReflection.encoderFor(ScalaReflection.localTypeOf[Function])
      .asInstanceOf[org.apache.spark.sql.Encoder[Function]]
  private implicit val columnEncoder: org.apache.spark.sql.Encoder[Column] =
    ScalaReflection.encoderFor(ScalaReflection.localTypeOf[Column])
      .asInstanceOf[org.apache.spark.sql.Encoder[Column]]
  private implicit val catalogMetadataEncoder: org.apache.spark.sql.Encoder[CatalogMetadata] =
    ScalaReflection.encoderFor(ScalaReflection.localTypeOf[CatalogMetadata])
      .asInstanceOf[org.apache.spark.sql.Encoder[CatalogMetadata]]

  override def currentDatabase: String = catalogSupport.currentDatabase

  override def setCurrentDatabase(dbName: String): Unit = {
    val quotedNs = QuotingUtils.quoteNameParts(parseIdent(dbName))
    sql(s"USE NAMESPACE $quotedNs")
  }

  override def listDatabases(): Dataset[Database] = listDatabasesInternal(None)

  override def listDatabases(pattern: String): Dataset[Database] =
    listDatabasesInternal(Some(pattern))

  private def listDatabasesInternal(patternOpt: Option[String]): Dataset[Database] = {
    val catalogName = catalogSupport.currentCatalog()
    val sqlStr = patternOpt match {
      case Some(p) => s"SHOW NAMESPACES LIKE '${escapeSingleQuotes(p)}'"
      case None => "SHOW NAMESPACES"
    }
    val rows = sql(sqlStr).collect()
    val databases = rows.map { row =>
      val rawName = row.getString(0)
      val name = if (rawName.startsWith("`")) rawName else QuotingUtils.quoteIfNeeded(rawName)
      new Database(
        name = name,
        catalog = catalogName,
        description = null,
        locationUri = null
      )
    }
    makeDataset(databases.toSeq)
  }

  override def listTables(): Dataset[Table] = listTables(catalogSupport.currentDatabase)

  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String): Dataset[Table] =
    listTablesInternal(dbName, None)

  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String, pattern: String): Dataset[Table] =
    listTablesInternal(dbName, Some(pattern))

  private def listTablesInternal(dbName: String, pattern: Option[String]): Dataset[Table] = {
    val dbNameParts = parseIdent(dbName)
    val catalogName = if (dbNameParts.length > 1) dbNameParts.head
      else catalogSupport.currentCatalog()
    // When user asks for database "default" (single part), use the session catalog's default
    // database. Otherwise "default" is resolved as a catalog name when a catalog named "default"
    // exists, yielding an empty namespace and no tables.
    val sessionCatalogName = "spark_catalog"
    val (quotedNs, effectiveCatalogName) =
      if (dbNameParts.length == 1 && dbNameParts.head.equalsIgnoreCase("default")) {
        val sessionDefault = s"${quote(sessionCatalogName)}.${quote("default")}"
        (sessionDefault, sessionCatalogName)
      } else {
        (dbNameParts.map(quote).mkString("."), catalogName)
      }
    val currentNs = catalogSupport.currentDatabase
    val sqlStr = pattern match {
      case Some(p) => s"SHOW TABLES IN $quotedNs LIKE '${escapeSingleQuotes(p)}'"
      case None if quotedNs == currentNs => "SHOW TABLES"
      case None => s"SHOW TABLES IN $quotedNs"
    }
    val rows = sql(sqlStr).collect()
    val tables = rows.map { row =>
      if (row.size < 3) {
        val tableName = if (row.size >= 2) row.getString(1) else row.getString(0)
        new Table(
          name = tableName,
          catalog = effectiveCatalogName,
          namespace = dbNameParts.drop(1).toArray,
          description = null,
          tableType = null,
          isTemporary = false
        )
      } else {
        val namespaceName = row.getString(0)
        val tableName = row.getString(1)
        val isTempView = row.getBoolean(2)
        val ns = if (isTempView) {
          if (namespaceName.isEmpty) Array.empty[String] else Array(namespaceName)
        } else {
          parseIdent(namespaceName).toArray
        }
        val nsInCatalog = if (isTempView) Array.empty[String] else {
          if (dbNameParts.length > 1 && ns.nonEmpty && ns.head == effectiveCatalogName) {
            ns.tail
          } else {
            ns
          }
        }
        val nameParts =
          if (isTempView) ns :+ tableName else effectiveCatalogName +: nsInCatalog :+ tableName
        try {
          val t = getTable(nameParts.mkString("."))
          // Prefer SHOW TABLES namespace and isTemporary; for temp views always use empty namespace
          if (row.size >= 3) {
            new Table(
              name = t.name,
              catalog = t.catalog,
              namespace = nsInCatalog,
              description = t.description,
              tableType = t.tableType,
              isTemporary = row.getBoolean(2))
          } else {
            t
          }
        } catch {
          case _: Exception =>
            new Table(
              name = tableName,
              catalog = if (isTempView) null else effectiveCatalogName,
              namespace = nsInCatalog,
              description = null,
              tableType = null,
              isTemporary = isTempView
            )
        }
      }
    }
    makeDataset(tables.toSeq)
  }

  override def listFunctions(): Dataset[Function] = listFunctions(catalogSupport.currentDatabase)

  @throws[AnalysisException]("database does not exist")
  override def listFunctions(dbName: String): Dataset[Function] =
    listFunctionsInternal(dbName, None)

  @throws[AnalysisException]("database does not exist")
  override def listFunctions(dbName: String, pattern: String): Dataset[Function] =
    listFunctionsInternal(dbName, Some(pattern))

  private def listFunctionsInternal(dbName: String, pattern: Option[String]): Dataset[Function] = {
    val catalogName = catalogSupport.currentCatalog()
    val currentNs = catalogSupport.currentDatabase
    val quotedNs = parseIdent(dbName).map(quote).mkString(".")
    val sqlStr = pattern match {
      case Some(p) => s"SHOW FUNCTIONS IN $quotedNs LIKE '${escapeSingleQuotes(p)}'"
      case None if quotedNs == currentNs => "SHOW FUNCTIONS"
      case None => s"SHOW FUNCTIONS IN $quotedNs"
    }
    val rows = sql(sqlStr).collect()
    val nsArray = parseIdent(dbName).toArray
    val qualifiedNs = (catalogName +: nsArray).mkString(".")
    val functions = rows.map { row =>
      val funcName = row.getString(0)
      val parts = try parseIdent(funcName) catch { case NonFatal(_) => Seq(funcName) }
      val name = if (parts.nonEmpty) parts.last else funcName
      val (namespace, isTemp, describeRows) = resolveFunctionNamespaceWithDescribe(
        qualifiedNs, name, nsArray)
      val (description, className) = describeRows
        .map(parseDescribeFunctionOutput)
        .getOrElse((null: String, null: String))
      new Function(
        name = name,
        catalog = catalogName,
        namespace = namespace,
        description = description,
        className = className,
        isTemporary = isTemp
      )
    }
    makeDataset(functions.toSeq)
  }

  /** Returns (namespace, isTemp, optional DESCRIBE rows for description/className). */
  private def resolveFunctionNamespaceWithDescribe(
      qualifiedNs: String,
      funcName: String,
      nsArray: Array[String]): (Array[String], Boolean, Option[Array[Row]]) = {
    val qualifiedFunc = s"$qualifiedNs.${quote(funcName)}"
    try {
      val describeRows = sql(s"DESCRIBE FUNCTION EXTENDED $qualifiedFunc").collect()
      (nsArray, false, Some(describeRows))
    } catch {
      case _: AnalysisException =>
        try {
          val describeRows = sql(s"DESCRIBE FUNCTION EXTENDED ${quote(funcName)}").collect()
          (Array.empty, true, Some(describeRows))
        } catch {
          case _: Exception => (nsArray, false, None)
        }
    }
  }

  private def resolveFunctionNamespace(
      qualifiedNs: String,
      funcName: String,
      nsArray: Array[String]): (Array[String], Boolean) = {
    val (namespace, isTemp, _) = resolveFunctionNamespaceWithDescribe(
      qualifiedNs, funcName, nsArray)
    (namespace, isTemp)
  }

  @throws[AnalysisException]("table does not exist")
  override def listColumns(tableName: String): Dataset[Column] = {
    listColumns(parseIdent(tableName))
  }

  @throws[AnalysisException]("database or table does not exist")
  override def listColumns(dbName: String, tableName: String): Dataset[Column] = {
    listColumns(Seq("spark_catalog", dbName, tableName))
  }

  private def listColumns(nameParts: Seq[String]): Dataset[Column] = {
    val quotedTable = quoted(nameParts)
    val rows = try {
      sql(s"DESCRIBE TABLE EXTENDED $quotedTable").collect()
    } catch {
      case _: AssertionError | _: Exception =>
        val simple = sql(s"DESCRIBE TABLE $quotedTable").collect()
        val cols = simple.takeWhile(r => !r.getString(0).startsWith("#"))
          .filter(r => r.getString(0).nonEmpty && !r.getString(0).startsWith("#"))
          .map(r => new Column(
            name = r.getString(0),
            description = null,
            dataType = if (r.size > 1) r.getString(1) else "string",
            nullable = true,
            isPartition = false,
            isBucket = false,
            isCluster = false
          ))
        return makeDataset(cols.toSeq)
    }
    var inPartition = false
    var inClustering = false
    var inDetailed = false
    val dataColumns = scala.collection.mutable.ArrayBuffer[(String, String)]()
    val partitionCols = scala.collection.mutable.ArrayBuffer[(String, String)]()
    val clusterCols = scala.collection.mutable.Set[String]()
    var bucketCols = Set.empty[String]
    var partitionColsFromDetailed = Set.empty[String]

    for (i <- rows.indices) {
      val row = rows(i)
      val colName = if (row.size > 0) Option(row.getString(0)).getOrElse("").trim else ""
      val dataType = if (row.size > 1) Option(row.getString(1)).getOrElse("") else ""
      if (colName == "# Partition Information") {
        inPartition = true
        inClustering = false
        inDetailed = false
      } else if (colName == "# Partitioning") {
        // V2 describe outputs this after "# Partition Information" when table has bucket/cluster;
        // the rows are "Part 0", "Part 1", ... not column names, so stop collecting partition cols.
        inPartition = false
        inClustering = false
        inDetailed = false
      } else if (colName == "# Clustering Information") {
        inPartition = false
        inClustering = true
        inDetailed = false
      } else if (colName == "# Detailed Table Information") {
        inPartition = false
        inClustering = false
        inDetailed = true
      } else if (inDetailed && colName.nonEmpty && !colName.startsWith("#")) {
        if (colName == "Bucket Columns" && row.size > 1) {
          bucketCols = parseJsonArrayColumnNames(Option(row.getString(1)).getOrElse(""))
        } else if (colName == "Partition Columns" && row.size > 1) {
          partitionColsFromDetailed = parseJsonArrayColumnNames(
            Option(row.getString(1)).getOrElse(""))
        }
      } else if (!colName.startsWith("#") && colName.nonEmpty) {
        if (inPartition) {
          partitionCols += ((colName, dataType))
        } else if (inClustering) {
          clusterCols += colName
        } else if (!inDetailed) {
          dataColumns += ((colName, dataType))
        }
      }
    }

    val dataNames = dataColumns.map(_._1)
    val partitionNames = partitionCols.map(_._1)
    val dataSet = dataNames.toSet
    val allColNames = dataNames ++ partitionNames.filterNot(dataSet)
    val typeByCol = (dataColumns ++ partitionCols).toMap
    val partitionSet = partitionNames.toSet ++ partitionColsFromDetailed
    val columns = allColNames.map { name =>
      val dt = typeByCol.getOrElse(name, "string")
      new Column(
        name = name,
        description = null,
        dataType = dt,
        nullable = true,
        isPartition = partitionSet.contains(name),
        isBucket = bucketCols.contains(name),
        isCluster = clusterCols.contains(name)
      )
    }
    makeDataset(columns.toSeq)
  }

  private def parseJsonArrayColumnNames(json: String): Set[String] = {
    // JSON array: ["a", "b"]
    val quoted = "\"([^\"]+)\"".r
    val fromQuoted = quoted.findAllIn(json).matchData.map(_.group(1)).toSet
    if (fromQuoted.nonEmpty) return fromQuoted
    // CatalogTable format: [a, b] or [`a`, `b`]
    val bracket = "\\[(.*)\\]".r
    json match {
      case bracket(inner) =>
        inner.split(",").map(_.trim).map { s =>
          if (s.startsWith("`") && s.endsWith("`")) s.drop(1).dropRight(1) else s
        }.filter(_.nonEmpty).toSet
      case _ => Set.empty
    }
  }

  @throws[AnalysisException]("database does not exist")
  override def getDatabase(dbName: String): Database = {
    val idents = parseIdent(dbName)
    val identsToDescribe = if (idents.length == 1) {
      val sessionQualified = "spark_catalog" +: idents
      if (databaseExists(sessionQualified.mkString("."))) sessionQualified
      else (catalogSupport.currentCatalog() +: idents).toSeq
    } else {
      idents
    }
    val quotedNs = QuotingUtils.quoteNameParts(identsToDescribe)
    val rows = sql(s"DESCRIBE NAMESPACE EXTENDED $quotedNs").collect()
    val info = rows.map(row => (row.getString(0), Option(row.getString(1)).getOrElse(""))).toMap
    val name = info.getOrElse("Namespace Name", identsToDescribe.last)
    val catalog = info.getOrElse("Catalog Name",
      if (identsToDescribe.length > 1) identsToDescribe.head else "")
    new Database(
      name = name,
      catalog = catalog,
      description = info.get("Comment").orNull,
      locationUri = info.get("Location").orNull
    )
  }

  @throws[AnalysisException]("table does not exist")
  override def getTable(tableName: String): Table = {
    val nameParts = parseIdent(tableName)
    val quotedTable = quoted(nameParts)
    val rows = sql(s"DESCRIBE TABLE EXTENDED $quotedTable").collect()
    val detailedIdx = rows.indexWhere(r => r.getString(0) == "# Detailed Table Information")
    val info = if (detailedIdx >= 0) {
      rows.drop(detailedIdx + 1)
        .takeWhile(r => !r.getString(0).startsWith("#") || r.getString(0).isEmpty)
        .map(r => (r.getString(0), Option(r.getString(1)).getOrElse("")))
        .toMap
    } else {
      Map.empty[String, String]
    }
    val tableType = info.getOrElse("Type", "TABLE")
    val isTemp = info.get("Is Temporary").exists(_.equalsIgnoreCase("true"))
    val ns = if (nameParts.length > 2) nameParts.drop(1).dropRight(1).toArray
      else if (nameParts.length == 1) {
        val defaultDb = catalogSupport.currentDatabase
        val db = info.getOrElse("Database", defaultDb)
        Array(db)
      }
      else nameParts.dropRight(1).toArray
    val catalogName = if (nameParts.length > 2) nameParts.head
      else catalogSupport.currentCatalog()
    new Table(
      name = nameParts.last,
      catalog = catalogName,
      namespace = ns,
      description = info.get("Comment").orNull,
      tableType = tableType,
      isTemporary = isTemp
    )
  }

  @throws[AnalysisException]("database or table does not exist")
  override def getTable(dbName: String, tableName: String): Table = {
    getTable(s"$dbName.$tableName")
  }

  @throws[AnalysisException]("function does not exist")
  override def getFunction(functionName: String): Function = {
    val nameParts = parseIdent(functionName)
    val quotedFunc = quoted(nameParts)
    try {
      val describeRows = sql(s"DESCRIBE FUNCTION EXTENDED $quotedFunc").collect()
      val (namespace, isTemp) = if (nameParts.length > 1) {
        val ns = if (nameParts.length > 2) nameParts.drop(1).dropRight(1).toArray
          else Array(nameParts(0))
        (ns, false)
      } else {
        val qualified = s"${catalogSupport.currentCatalog()}.${catalogSupport.currentDatabase}" +
          s".${quote(nameParts.last)}"
        try {
          sql(s"DESCRIBE FUNCTION EXTENDED $qualified").collect()
          (Array(catalogSupport.currentDatabase), false)
        } catch {
          case _: Exception => (null, true)
        }
      }
      val catalogName = if (nameParts.length > 2) nameParts.head
        else catalogSupport.currentCatalog()
      val (description, className) = parseDescribeFunctionOutput(describeRows)
      new Function(
        name = nameParts.last,
        catalog = catalogName,
        namespace = namespace,
        description = description,
        className = className,
        isTemporary = isTemp
      )
    } catch {
      case ae: AnalysisException if nameParts.length >= 3 =>
        // DESCRIBE FUNCTION throws for non-session catalogs; use CatalogSupport to load metadata.
        val catalogName = nameParts.head
        val namespace = nameParts.drop(1).dropRight(1).toArray
        catalogSupport.getFunctionMetadata(catalogName, namespace, nameParts.last) match {
          case Some((description, className)) =>
            new Function(
              name = nameParts.last,
              catalog = catalogName,
              namespace = namespace,
              description = description,
              className = className,
              isTemporary = false
            )
          case None =>
            throw ae
        }
    }
  }

  private def parseDescribeFunctionOutput(rows: Array[_ <: Row]): (String, String) = {
    var description: String = "N/A."
    var className: String = null
    rows.foreach { row =>
      if (row.size > 0) {
      val line = Option(row.getString(0)).getOrElse("").trim
      if (line.startsWith("Class:")) {
        className = line.stripPrefix("Class:").trim
      } else if (line.startsWith("Usage:") || line.startsWith("Extended Usage:")) {
        val value = (if (line.startsWith("Usage:")) line.stripPrefix("Usage:")
          else line.stripPrefix("Extended Usage:")).trim
        // V1 describe outputs "No example/argument for <func>." when there is no usage; use N/A.
        if (value.nonEmpty && !value.matches("No example/argument for .+")) {
          description = value
        }
      }
      }
    }
    (description, className)
  }

  @throws[AnalysisException]("database or function does not exist")
  override def getFunction(dbName: String, functionName: String): Function = {
    getFunction(s"$dbName.$functionName")
  }

  override def databaseExists(dbName: String): Boolean = {
    try {
      val parts = try parseIdent(dbName) catch { case NonFatal(_) => Seq(dbName) }
      val (sqlStr, nameToMatch) = if (parts.length == 1) {
        ("SHOW NAMESPACES", parts.head)
      } else {
        val quotedCatalog = quote(parts.head)
        (s"SHOW NAMESPACES IN $quotedCatalog", parts.tail.mkString("."))
      }
      val rows = sql(sqlStr).collect()
      rows.exists(row => row.getString(0).equalsIgnoreCase(nameToMatch))
    } catch {
      case _: AnalysisException => false
      case NonFatal(_) => false
    }
  }

  override def tableExists(tableName: String): Boolean = {
    val nameParts = parseIdent(tableName)
    if (nameParts.isEmpty) return false
    val nsParts = nameParts.init
    val tablePart = nameParts.last
    try {
      val quotedNs = if (nsParts.isEmpty) catalogSupport.currentDatabase
        else nsParts.map(quote).mkString(".")
      val sqlStr = if (nsParts.isEmpty) "SHOW TABLES" else s"SHOW TABLES IN $quotedNs"
      val rows = sql(sqlStr).collect()
      val namespaceRequested = nsParts.nonEmpty
      rows.exists { row =>
        val tableNameCol = if (row.size > 1) row.getString(1) else row.getString(0)
        val isTemp = row.size >= 3 && row.get(2) == true
        // When a namespace is requested, only persistent tables count; except global_temp
        // holds global temp views (isTemp=true), so allow them when namespace is global_temp.
        val nameMatches = tableNameCol != null && tableNameCol.equalsIgnoreCase(tablePart)
        val allowTemp = namespaceRequested && nsParts.nonEmpty &&
          nsParts.head.equalsIgnoreCase("global_temp")
        nameMatches && (!namespaceRequested || !isTemp || allowTemp)
      }
    } catch {
      case _: AnalysisException => false
      case NonFatal(_) => false
    }
  }

  override def tableExists(dbName: String, tableName: String): Boolean =
    tableExists(s"$dbName.$tableName")

  override def functionExists(functionName: String): Boolean = {
    val nameParts = parseIdent(functionName)
    if (nameParts.isEmpty) return false
    try {
      val quotedFunc = quoted(nameParts)
      sql(s"DESCRIBE FUNCTION $quotedFunc").collect()
      true
    } catch {
      case _: AnalysisException =>
        // DESCRIBE FUNCTION throws for non-session catalogs (e.g. testcat). Use SHOW FUNCTIONS.
        if (nameParts.length >= 3) {
          val quotedNs = nameParts.dropRight(1).map(quote).mkString(".")
          val funcName = nameParts.last
          try {
            val rows = sql(s"SHOW FUNCTIONS IN $quotedNs").collect()
            rows.exists { row =>
              val n = if (row.size > 0) Option(row.getString(0)).orNull else null
              if (n == null) false
              else {
                // V2 SHOW FUNCTIONS returns qualified name (e.g. `testcat`.`my_db2`.`my_func2`)
                val lastPart = n.replaceAll("`", "").split("\\.").last
                lastPart.equalsIgnoreCase(funcName)
              }
            }
          } catch { case _: AnalysisException => false }
        } else false
    }
  }

  override def functionExists(dbName: String, functionName: String): Boolean =
    functionExists(s"$dbName.$functionName")

  override def createTable(tableName: String, path: String): DataFrame = {
    val defaultSource = session.conf.get("spark.sql.sources.default", "parquet")
    createTable(tableName, path, defaultSource)
  }

  override def createTable(tableName: String, path: String, source: String): DataFrame =
    createTable(tableName, source, Map("path" -> path))

  override def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame =
    createTable(tableName, source, new StructType, options)

  override def createTable(
      tableName: String,
      source: String,
      description: String,
      options: Map[String, String]): DataFrame =
    createTable(tableName, source, new StructType, description, options)

  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame =
    createTable(tableName, source, schema, "", options)

  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]): DataFrame = {
    val quotedTable = quoted(parseIdent(tableName))
    val schemaPart = if (schema.isEmpty) "" else {
      schema.map { f =>
        val cmt = f.getComment()
          .map(c => s" COMMENT '${escapeSingleQuotes(c)}'")
          .getOrElse("")
        s"${quote(f.name)} ${f.dataType.sql}$cmt"
      }.mkString(", ")
    }
    val optionsClause = if (options.isEmpty) "" else {
      val opts = options.map { case (k, v) =>
        s"${quote(k)} '${escapeSingleQuotes(v)}'"
      }.mkString(", ")
      " OPTIONS (" + opts + ")"
    }
    val commentPart = if (description.nonEmpty) {
      s" COMMENT '${escapeSingleQuotes(description)}'"
    } else ""
    val sqlStr = if (schema.isEmpty) {
      s"CREATE TABLE $quotedTable USING $source$optionsClause$commentPart"
    } else {
      s"CREATE TABLE $quotedTable ($schemaPart) USING $source$optionsClause$commentPart"
    }
    sql(sqlStr)
    session.table(tableName)
  }

  override def dropTempView(viewName: String): Boolean = {
    try {
      sql(s"DROP VIEW IF EXISTS ${quote(viewName)}").collect()
      true
    } catch { case NonFatal(_) => false }
  }

  override def dropGlobalTempView(viewName: String): Boolean = {
    try {
      sql(s"DROP VIEW IF EXISTS global_temp.${quote(viewName)}").collect()
      true
    } catch { case NonFatal(_) => false }
  }

  override def recoverPartitions(tableName: String): Unit = {
    val nameParts = parseIdent(tableName)
    val quotedTable = quoted(nameParts)
    try {
      sql(s"MSCK REPAIR TABLE $quotedTable")
    } catch {
      case e: AnalysisException
        if e.getCondition == "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE" &&
           Option(e.getMessageParameters.get("operation")).contains("MSCK REPAIR TABLE") =>
        throw new AnalysisException(
          errorClass = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
          messageParameters = Map(
            "viewName" -> Option(e.getMessageParameters.get("viewName"))
              .filter(v => v != null && v.startsWith("`"))
              .getOrElse(QuotingUtils.quoteNameParts(nameParts)),
            "operation" -> "recoverPartitions()"))
    }
  }

  override def isCached(tableName: String): Boolean =
    catalogSupport.isTableCached(session, tableName)

  private def storageLevelToOptionString(level: StorageLevel): String = level match {
    case l if l == StorageLevel.NONE => "NONE"
    case l if l == StorageLevel.DISK_ONLY => "DISK_ONLY"
    case l if l == StorageLevel.DISK_ONLY_2 => "DISK_ONLY_2"
    case l if l == StorageLevel.DISK_ONLY_3 => "DISK_ONLY_3"
    case l if l == StorageLevel.MEMORY_ONLY => "MEMORY_ONLY"
    case l if l == StorageLevel.MEMORY_ONLY_2 => "MEMORY_ONLY_2"
    case l if l == StorageLevel.MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
    case l if l == StorageLevel.MEMORY_ONLY_SER_2 => "MEMORY_ONLY_SER_2"
    case l if l == StorageLevel.MEMORY_AND_DISK => "MEMORY_AND_DISK"
    case l if l == StorageLevel.MEMORY_AND_DISK_2 => "MEMORY_AND_DISK_2"
    case l if l == StorageLevel.MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    case l if l == StorageLevel.MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
    case l if l == StorageLevel.OFF_HEAP => "OFF_HEAP"
    case _ => ""
  }

  override def cacheTable(tableName: String): Unit =
    sql(s"CACHE TABLE ${quoted(parseIdent(tableName))}")

  override def cacheTable(tableName: String, storageLevel: StorageLevel): Unit = {
    val quotedName = quoted(parseIdent(tableName))
    val levelStr = storageLevelToOptionString(storageLevel)
    val sqlStr = if (levelStr.nonEmpty) {
      s"CACHE TABLE $quotedName OPTIONS (storageLevel '$levelStr')"
    } else {
      s"CACHE TABLE $quotedName"
    }
    sql(sqlStr)
  }

  override def uncacheTable(tableName: String): Unit =
    sql(s"UNCACHE TABLE ${quoted(parseIdent(tableName))}").collect()

  override def clearCache(): Unit = sql("CLEAR CACHE").collect()

  override def refreshTable(tableName: String): Unit = {
    // Use SQL DDL only. RefreshTableCommand will refresh metadata and recache by plan
    // so cache name and storage level are preserved (SPARK-27248).
    val quotedName = quoted(parseIdent(tableName))
    sql(s"REFRESH TABLE $quotedName").collect()
  }

  override def refreshByPath(resourcePath: String): Unit =
    catalogSupport.refreshByPath(session, resourcePath)

  override def currentCatalog(): String = catalogSupport.currentCatalog()

  override def setCurrentCatalog(catalogName: String): Unit =
    sql(s"SET CATALOG ${quote(catalogName)}")

  override def listCatalogs(): Dataset[CatalogMetadata] = listCatalogsInternal(None)

  override def listCatalogs(pattern: String): Dataset[CatalogMetadata] =
    listCatalogsInternal(Some(pattern))

  private def listCatalogsInternal(patternOpt: Option[String]): Dataset[CatalogMetadata] = {
    val sqlStr = patternOpt match {
      case Some(p) => s"SHOW CATALOGS LIKE '${escapeSingleQuotes(p)}'"
      case None => "SHOW CATALOGS"
    }
    val rows = sql(sqlStr).collect()
    makeDataset(rows.map(row => new CatalogMetadata(row.getString(0), null)).toSeq)
  }
}
