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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.{FullQualifiedTableName, FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils, ClusterBySpec, SessionCatalog}
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, Column, FunctionCatalog, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableCatalogCapability, TableChange, V1Table}
import org.apache.spark.sql.connector.catalog.NamespaceChange.RemoveProperty
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

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
        throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
    }
  }

  // Get data source options from the catalog table properties with the path option.
  private def getDataSourceOptions(
      properties: Map[String, String],
      storage: CatalogStorageFormat): CaseInsensitiveStringMap = {
    val propertiesWithPath = properties ++
      storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    new CaseInsensitiveStringMap(propertiesWithPath.asJava)
  }

  private def hasCustomSessionCatalog: Boolean = {
    catalog.conf.contains(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key)
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      val table = catalog.getTableMetadata(ident.asTableIdentifier)
      val qualifiedIdent = catalog.qualifyIdentifier(table.identifier)
      // The custom session catalog may extend `DelegatingCatalogExtension` and rely on the returned
      // table here. To avoid breaking it we do not resolve the table provider and still return
      // `V1Table` if the custom session catalog is present.
      if (table.provider.isDefined && !hasCustomSessionCatalog) {
        val qualifiedTableName =
          FullQualifiedTableName(qualifiedIdent.catalog.get, table.database, table.identifier.table)
        // Check if the table is in the v1 table cache to skip the v2 table lookup.
        if (catalog.getCachedTable(qualifiedTableName) != null) {
          return V1Table(table)
        }
        DataSourceV2Utils.getTableProvider(table.provider.get, conf) match {
          case Some(provider) =>
            // Get the table properties during creation and append the path option
            // to the properties.
            val dsOptions = getDataSourceOptions(table.properties, table.storage)
            // If the source accepts external table metadata, we can pass the schema and
            // partitioning information stored in Hive to `getTable` to avoid expensive
            // schema/partitioning inference.
            if (provider.supportsExternalMetadata()) {
              provider.getTable(
                table.schema,
                getV2Partitioning(table),
                dsOptions.asCaseSensitiveMap())
            } else {
              provider.getTable(
                provider.inferSchema(dsOptions),
                provider.inferPartitioning(dsOptions),
                dsOptions.asCaseSensitiveMap())
            }
          case _ =>
            V1Table(table)
        }
      } else {
        V1Table(table)
      }
    } catch {
      case _: NoSuchNamespaceException =>
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
    val nameParts = t match {
      case V1Table(catalogTable) => catalogTable.identifier.nameParts
      case _ => ident.asTableIdentifier.nameParts
    }
    throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(nameParts))
  }

  private def getV2Partitioning(table: CatalogTable): Array[Transform] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val v2Partitioning = table.partitionColumnNames.asTransforms
    val v2Bucketing = table.bucketSpec.map(
      spec => Array(spec.asTransform)).getOrElse(Array.empty)
    val v2Clustering = table.clusterBySpec.map(
      spec => Array(spec.asTransform)).getOrElse(Array.empty)
    v2Partitioning ++ v2Bucketing ++ v2Clustering
  }

  override def invalidateTable(ident: Identifier): Unit = {
    catalog.refreshTable(ident.asTableIdentifier)
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val provider = properties.getOrDefault(TableCatalog.PROP_PROVIDER, conf.defaultDataSourceName)
    val tableProperties = properties.asScala.toMap
    val location = Option(properties.get(TableCatalog.PROP_LOCATION))
    val storage = DataSource.buildStorageFormatFromOptions(toOptions(tableProperties))
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val isExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val isManagedLocation = Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
      .exists(_.equalsIgnoreCase("true"))
    val tableType = if (isExternal || (location.isDefined && !isManagedLocation)) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    val (newSchema, newPartitions) = DataSourceV2Utils.getTableProvider(provider, conf) match {
      // If the provider does not support external metadata, users should not be allowed to
      // specify custom schema when creating the data source table, since the schema will not
      // be used when loading the table.
      case Some(p) if !p.supportsExternalMetadata() =>
        if (schema.nonEmpty) {
          throw new SparkUnsupportedOperationException(
            errorClass = "CANNOT_CREATE_DATA_SOURCE_TABLE.EXTERNAL_METADATA_UNSUPPORTED",
            messageParameters = Map("tableName" -> ident.fullyQuoted, "provider" -> provider))
        }
        // V2CreateTablePlan does not allow non-empty partitions when schema is empty. This
        // is checked in `PreProcessTableCreation` rule.
        assert(partitions.isEmpty,
          s"Partitions should be empty when the schema is empty: ${partitions.mkString(", ")}")
        (schema, partitions)

      case Some(tableProvider) =>
        assert(tableProvider.supportsExternalMetadata())
        lazy val dsOptions = getDataSourceOptions(tableProperties, storage)
        if (schema.isEmpty) {
          assert(partitions.isEmpty,
            s"Partitions should be empty when the schema is empty: ${partitions.mkString(", ")}")
          // Infer the schema and partitions and store them in the catalog.
          (tableProvider.inferSchema(dsOptions), tableProvider.inferPartitioning(dsOptions))
        } else {
          val partitioning = if (partitions.isEmpty) {
            tableProvider.inferPartitioning(dsOptions)
          } else {
            partitions
          }
          val table = tableProvider.getTable(schema, partitions, dsOptions)
          // Check if the schema of the created table matches the given schema.
          val tableSchema = table.columns().asSchema
          if (!DataType.equalsIgnoreNullability(table.columns().asSchema, schema)) {
            throw QueryCompilationErrors.dataSourceTableSchemaMismatchError(tableSchema, schema)
          }
          (schema, partitioning)
        }

      case _ =>
        // The provider is not a V2 provider so we return the schema and partitions as is.
        (schema, partitions)
    }

    val (partitionColumns, maybeBucketSpec, maybeClusterBySpec) =
      newPartitions.toImmutableArraySeq.convertTransforms

    val tableDesc = CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties ++
        maybeClusterBySpec.map(
          clusterBySpec => ClusterBySpec.toProperty(newSchema, clusterBySpec, conf.resolver)),
      tracksPartitionsInCatalog = conf.manageFilesourcePartitions,
      comment = Option(properties.get(TableCatalog.PROP_COMMENT)))

    try {
      catalog.createTable(tableDesc, ignoreIfExists = false)
    } catch {
      case _: TableAlreadyExistsException =>
        throw QueryCompilationErrors.tableAlreadyExistsError(ident)
    }

    null // Return null to save the `loadTable` call for CREATE TABLE without AS SELECT.
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    throw QueryCompilationErrors.createTableDeprecatedError()
  }

  private def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filter { case (k, _) => k.startsWith(TableCatalog.OPTION_PREFIX) }.map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }
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

    val finalProperties = CatalogV2Util.applyClusterByChanges(properties, schema, changes)
    try {
      catalog.alterTable(
        catalogTable.copy(
          properties = finalProperties, schema = schema, owner = owner, comment = comment,
          storage = storage))
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }

    null // Return null to save the `loadTable` call for ALTER TABLE.
  }

  override def purgeTable(ident: Identifier): Boolean = {
    dropTableInternal(ident, purge = true)
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
        case null =>
          false
        case _ =>
          catalog.invalidateCachedTable(ident.asTableIdentifier)
          catalog.dropTable(
            ident.asTableIdentifier,
            ignoreIfNotExists = true,
            purge = purge)
          true
      }
    } catch {
      case _: NoSuchTableException =>
        false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (tableExists(newIdent)) {
      throw QueryCompilationErrors.tableAlreadyExistsError(newIdent)
    }

    catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
  }

  implicit class TableIdentifierHelper(ident: Identifier) {
    def asTableIdentifier: TableIdentifier = {
      ident.namespace match {
        case Array(db) =>
          TableIdentifier(ident.name, Some(db))
        case other =>
          throw QueryCompilationErrors.requiresSinglePartNamespaceError(other.toImmutableArraySeq)
      }
    }

    def asFunctionIdentifier: FunctionIdentifier = {
      ident.namespace match {
        case Array(db) =>
          FunctionIdentifier(ident.name, Some(db))
        case other =>
          throw QueryCompilationErrors.requiresSinglePartNamespaceError(other.toImmutableArraySeq)
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
        throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        try {
          catalog.getDatabaseMetadata(db).toMetadata
        } catch {
          case _: NoSuchNamespaceException =>
            throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
        }

      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
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
        throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
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
      throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
  }

  def isTempView(ident: Identifier): Boolean = {
    catalog.isTempView((ident.namespace() :+ ident.name()).toImmutableArraySeq)
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
        throw QueryCompilationErrors.noSuchNamespaceError(name() +: namespace)
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
