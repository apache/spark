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

package org.apache.spark.sql.execution.datasources.v2

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, Column, FunctionCatalog, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableCatalogCapability, TableChange, V1Table}
import org.apache.spark.sql.connector.catalog.NamespaceChange.RemoveProperty
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A [[TableCatalog]] that translates calls to the v1 SessionCatalog.
 */
class V2SessionCatalog(catalog: SessionCatalog)
  extends TableCatalog with FunctionCatalog with SupportsNamespaces with SQLConfHelper {
  import V2SessionCatalog._

  override val defaultNamespace: Array[String] = Array(SQLConf.get.defaultDatabase)

  override def name: String = CatalogManager.SESSION_CATALOG_NAME

  // This class is instantiated by Spark, so `initialize` method will not be called.
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def capabilities(): util.Set[TableCatalogCapability] = {
    Set(
      TableCatalogCapability.SUPPORT_COLUMN_DEFAULT_VALUE
    ).asJava
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        catalog
          .listTables(db)
          .map(ident => Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
          .toArray
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      V1Table(catalog.getTableMetadata(ident.asTableIdentifier))
    } catch {
      case _: NoSuchDatabaseException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }
  }

  override def loadTable(ident: Identifier, timestamp: Long): Table = {
    failTimeTravel(ident, loadTable(ident))
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    failTimeTravel(ident, loadTable(ident))
  }

  private def failTimeTravel(ident: Identifier, t: Table): Table = {
    t match {
      case V1Table(catalogTable) =>
        if (catalogTable.tableType == CatalogTableType.VIEW) {
          throw QueryCompilationErrors.timeTravelUnsupportedError(
            toSQLId(catalogTable.identifier.nameParts))
        } else {
          throw QueryCompilationErrors.timeTravelUnsupportedError(
            toSQLId(catalogTable.identifier.nameParts))
        }

      case _ => throw QueryCompilationErrors.timeTravelUnsupportedError(
        toSQLId(ident.asTableIdentifier.nameParts))
    }
  }

  override def invalidateTable(ident: Identifier): Unit = {
    catalog.refreshTable(ident.asTableIdentifier)
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    createTable(ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties)
  }

  override def purgeTable(ident: Identifier): Boolean = {
    dropTableInternal(ident, purge = true)
  }

  // TODO: remove it when no tests calling this deprecated method.
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper
    val (partitionColumns, maybeBucketSpec) = partitions.toSeq.convertTransforms
    val provider = properties.getOrDefault(TableCatalog.PROP_PROVIDER, conf.defaultDataSourceName)
    val tableProperties = properties.asScala
    val location = Option(properties.get(TableCatalog.PROP_LOCATION))
    val storage = DataSource.buildStorageFormatFromOptions(toOptions(tableProperties.toMap))
        .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val isExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val tableType = if (isExternal || location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    val tableDesc = CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = conf.manageFilesourcePartitions,
      comment = Option(properties.get(TableCatalog.PROP_COMMENT)))

    try {
      catalog.createTable(tableDesc, ignoreIfExists = false)
    } catch {
      case _: TableAlreadyExistsException =>
        throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }

    loadTable(ident)
  }

  private def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }.toMap
  }

  override def alterTable(
      ident: Identifier,
      changes: TableChange*): Table = {
    val catalogTable = try {
      catalog.getTableMetadata(ident.asTableIdentifier)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }

    val properties = CatalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
    val schema = CatalogV2Util.applySchemaChanges(
      catalogTable.schema, changes, catalogTable.provider, "ALTER TABLE")
    val comment = properties.get(TableCatalog.PROP_COMMENT)
    val owner = properties.getOrElse(TableCatalog.PROP_OWNER, catalogTable.owner)
    val location = properties.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI)
    val storage = if (location.isDefined) {
      catalogTable.storage.copy(locationUri = location)
    } else {
      catalogTable.storage
    }

    try {
      catalog.alterTable(
        catalogTable.copy(
          properties = properties, schema = schema, owner = owner, comment = comment,
          storage = storage))
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    dropTableInternal(ident)
  }

  private def dropTableInternal(ident: Identifier, purge: Boolean = false): Boolean = {
    try {
      loadTable(ident) match {
        case V1Table(v1Table) if v1Table.tableType == CatalogTableType.VIEW =>
          throw QueryCompilationErrors.wrongCommandForObjectTypeError(
            operation = "DROP TABLE",
            requiredType = s"${CatalogTableType.EXTERNAL.name} or" +
              s" ${CatalogTableType.MANAGED.name}",
            objectName = v1Table.qualifiedName,
            foundType = v1Table.tableType.name,
            alternative = "DROP VIEW"
          )
        case _ =>
      }
      catalog.invalidateCachedTable(ident.asTableIdentifier)
      catalog.dropTable(
        ident.asTableIdentifier,
        ignoreIfNotExists = true,
        purge = purge)
      true
    } catch {
      case _: NoSuchTableException =>
        false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (tableExists(newIdent)) {
      throw QueryCompilationErrors.tableAlreadyExistsError(newIdent)
    }

    // Load table to make sure the table exists
    loadTable(oldIdent)
    catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
  }

  implicit class TableIdentifierHelper(ident: Identifier) {
    def asTableIdentifier: TableIdentifier = {
      ident.namespace match {
        case Array(db) =>
          TableIdentifier(ident.name, Some(db))
        case other =>
          throw QueryCompilationErrors.requiresSinglePartNamespaceError(other)
      }
    }

    def asFunctionIdentifier: FunctionIdentifier = {
      ident.namespace match {
        case Array(db) =>
          FunctionIdentifier(ident.name, Some(db))
        case other =>
          throw QueryCompilationErrors.requiresSinglePartNamespaceError(other)
      }
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(db) =>
      catalog.databaseExists(db)
    case _ =>
      false
  }

  override def listNamespaces(): Array[Array[String]] = {
    catalog.listDatabases().map(Array(_)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if catalog.databaseExists(db) =>
        Array()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        try {
          catalog.getDatabaseMetadata(db).toMetadata
        } catch {
          case _: NoSuchDatabaseException =>
            throw QueryCompilationErrors.noSuchNamespaceError(namespace)
        }

      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = namespace match {
    case Array(db) if !catalog.databaseExists(db) =>
      catalog.createDatabase(
        toCatalogDatabase(db, metadata, defaultLocation = Some(catalog.getDefaultDBPath(db))),
        ignoreIfExists = false)

    case Array(_) =>
      throw QueryCompilationErrors.namespaceAlreadyExistsError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    namespace match {
      case Array(db) =>
        // validate that this catalog's reserved properties are not removed
        changes.foreach {
          case remove: RemoveProperty
            if CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.contains(remove.property) =>
            throw QueryExecutionErrors.cannotRemoveReservedPropertyError(remove.property)
          case _ =>
        }

        val metadata = catalog.getDatabaseMetadata(db).toMetadata
        catalog.alterDatabase(
          toCatalogDatabase(db, CatalogV2Util.applyNamespaceChanges(metadata, changes)))

      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def dropNamespace(
      namespace: Array[String],
      cascade: Boolean): Boolean = namespace match {
    case Array(db) if catalog.databaseExists(db) =>
      catalog.dropDatabase(db, ignoreIfNotExists = false, cascade)
      true

    case Array(_) =>
      // exists returned false
      false

    case _ =>
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)
  }

  def isTempView(ident: Identifier): Boolean = {
    catalog.isTempView(ident.namespace() :+ ident.name())
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    V1Function(catalog.lookupPersistentFunction(ident.asFunctionIdentifier))
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        catalog.listFunctions(db).filter(_._2 == "USER").map { case (funcIdent, _) =>
          assert(funcIdent.database.isDefined)
          Identifier.of(Array(funcIdent.database.get), funcIdent.identifier)
        }.toArray
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def functionExists(ident: Identifier): Boolean = {
    catalog.isPersistentFunction(ident.asFunctionIdentifier)
  }

  override def toString: String = s"V2SessionCatalog($name)"
}

private[sql] object V2SessionCatalog {

  private def toCatalogDatabase(
      db: String,
      metadata: util.Map[String, String],
      defaultLocation: Option[URI] = None): CatalogDatabase = {
    CatalogDatabase(
      name = db,
      description = metadata.getOrDefault(SupportsNamespaces.PROP_COMMENT, ""),
      locationUri = Option(metadata.get(SupportsNamespaces.PROP_LOCATION))
          .map(CatalogUtils.stringToURI)
          .orElse(defaultLocation)
          .getOrElse(throw QueryExecutionErrors.missingDatabaseLocationError()),
      properties = metadata.asScala.toMap --
        Seq(SupportsNamespaces.PROP_COMMENT, SupportsNamespaces.PROP_LOCATION))
  }

  private implicit class CatalogDatabaseHelper(catalogDatabase: CatalogDatabase) {
    def toMetadata: util.Map[String, String] = {
      val metadata = mutable.HashMap[String, String]()

      catalogDatabase.properties.foreach {
        case (key, value) => metadata.put(key, value)
      }
      metadata.put(SupportsNamespaces.PROP_LOCATION, catalogDatabase.locationUri.toString)
      metadata.put(SupportsNamespaces.PROP_COMMENT, catalogDatabase.description)

      metadata.asJava
    }
  }
}
