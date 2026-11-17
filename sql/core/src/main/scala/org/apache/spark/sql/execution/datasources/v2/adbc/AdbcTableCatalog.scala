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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.util
import java.util.Locale

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcException, AdbcStatusCode, BulkIngestMode}
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NonEmptyNamespaceException, NoSuchNamespaceException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap}

/**
 * A catalog of the tables of a database, accessed with an ADBC driver. It's configured with the
 * same options as the ADBC data source, except `dbtable` and `query`:
 * {{{
 *   spark.sql.catalog.mydb=org.apache.spark.sql.execution.datasources.v2.adbc.AdbcTableCatalog
 *   spark.sql.catalog.mydb.driver=...
 *   spark.sql.catalog.mydb.uri=...
 * }}}
 *
 * A namespace is a database schema of the connection's current catalog, if the driver reports
 * one. A table without a namespace is in the connection's current schema. Tables are created with
 * the driver's bulk ingestion, which maps the Spark types to the database's.
 */
class AdbcTableCatalog extends TableCatalog with SupportsNamespaces {

  private var catalogName: String = _
  private var catalogOptions: Map[String, String] = _
  // The catalog's options, for a connection.
  private var options: AdbcOptions = _

  override def name(): String = catalogName

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    // The relation is given by each table.
    val relationOptions = Set(AdbcOptions.ADBC_TABLE_NAME, AdbcOptions.ADBC_SCHEMA_NAME,
      AdbcOptions.ADBC_QUERY_STRING).map(_.toLowerCase(Locale.ROOT))
    catalogOptions = options.asCaseSensitiveMap().asScala.toMap.filter { case (k, _) =>
      !relationOptions.contains(k.toLowerCase(Locale.ROOT))
    }
    // Validated now, rather than when a table is first used.
    this.options = new AdbcOptions(catalogOptions + (AdbcOptions.ADBC_TABLE_NAME -> "t"))
  }

  /** The options of a table: the catalog's, with the table to read. */
  private def tableOptions(schema: String, table: String): AdbcOptions = new AdbcOptions(
    catalogOptions ++ Map(
      AdbcOptions.ADBC_SCHEMA_NAME -> schema,
      AdbcOptions.ADBC_TABLE_NAME -> table))

  private def withConnection[T](f: AdbcConnection => T): T = AdbcUtils.withConnection(options)(f)

  // The quote string doesn't change, so look it up at most once.
  private lazy val identifierQuoteChar: String = options.identifierQuoteChar
    .orElse(withConnection(AdbcUtils.getIdentifierQuoteChar))
    .getOrElse(AdbcOptions.DEFAULT_IDENTIFIER_QUOTE_CHAR)

  /** Returns the table, or the schema if `table` is None, as it appears in SQL. */
  private def sqlName(schema: String, table: Option[String]): String =
    (schema +: table.toSeq).map(AdbcUtils.quoteIdentifier(_, identifierQuoteChar)).mkString(".")

  // The catalog that the connection uses, which holds the namespaces. None if the driver
  // doesn't report it, and then the namespaces of all catalogs are used.
  private lazy val currentCatalog: Option[String] = withConnection { conn =>
    try {
      Option(conn.getCurrentCatalog)
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => None
    }
  }

  // The database schema that the connection uses, which holds the tables without a namespace.
  private lazy val currentDbSchema: Option[String] = withConnection { conn =>
    try {
      Option(conn.getCurrentDbSchema)
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => None
    }
  }

  override def defaultNamespace(): Array[String] =
    currentDbSchema.map(Array(_)).getOrElse(Array.empty[String])

  /**
   * Returns the database schema of a namespace: its single part, or the current schema if it's
   * empty. None if it has more parts, or is empty and the driver doesn't report the current one.
   */
  private def dbSchemaOf(namespace: Array[String]): Option[String] = namespace match {
    case Array() => currentDbSchema
    case Array(schema) => Some(schema)
    case _ => None
  }

  private def dbSchema(namespace: Array[String]): String = dbSchemaOf(namespace).getOrElse {
    throw QueryCompilationErrors.requiresSinglePartNamespaceError(namespace.toSeq)
  }

  private def schemas(): Seq[String] =
    withConnection(AdbcCatalogObjects.schemas(_, currentCatalog))

  private def tables(schema: String, table: Option[String]): Seq[String] =
    withConnection(AdbcCatalogObjects.tables(_, currentCatalog, schema, table))

  // ------------------------------------------------------------
  // Tables
  // ------------------------------------------------------------

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val schema = dbSchema(namespace)
    if (!schemas().contains(schema)) {
      throw new NoSuchNamespaceException(namespace)
    }
    tables(schema, None).map(t => Identifier.of(namespace, t)).toArray
  }

  override def tableExists(ident: Identifier): Boolean =
    dbSchemaOf(ident.namespace()).exists(tables(_, Some(ident.name())).nonEmpty)

  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) {
      throw QueryCompilationErrors.noSuchTableError(name(), ident)
    }
    val options = tableOptions(dbSchema(ident.namespace()), ident.name())
    AdbcTable(AdbcUtils.getSchema(options), options, inCatalog = true)
  }

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    if (tableInfo.partitions().nonEmpty) {
      throw QueryCompilationErrors.catalogOperationNotSupported(this, "partitioned tables")
    }
    checkProperties(tableInfo.properties(), "table")
    val schema = dbSchema(ident.namespace())
    if (tableExists(ident)) {
      throw new TableAlreadyExistsException(ident)
    }
    val target = AdbcWriteTarget(ident.name(), Some(schema), sqlName(schema, Some(ident.name())))
    val arrowSchema = ArrowUtils.toArrowSchema(tableInfo.schema(), SQLConf.get.sessionLocalTimeZone,
      errorOnDuplicatedFieldNames = true, largeVarTypes = false)
    withConnection { conn =>
      // Ingesting no rows in create mode creates the table, with the driver's type mapping.
      Using.resource(VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)) { root =>
        Using.resource(target.bulkIngest(conn, BulkIngestMode.CREATE)) { stmt =>
          stmt.bind(root)
          stmt.executeUpdate()
        }
      }
    }
    loadTable(ident)
  }

  /** Fails for properties that can't be set, such as a comment. The owner is ignored. */
  private def checkProperties(properties: util.Map[String, String], kind: String): Unit = {
    properties.keySet().asScala.filter(_ != TableCatalog.PROP_OWNER).toSeq.sorted.headOption
      .foreach { key =>
        throw QueryCompilationErrors.catalogOperationNotSupported(this, s"$kind property $key")
      }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw QueryCompilationErrors.unsupportedTableOperationError(this, ident, "ALTER TABLE")

  override def dropTable(ident: Identifier): Boolean = {
    if (tableExists(ident)) {
      execute(s"DROP TABLE ${sqlName(dbSchema(ident.namespace()), Some(ident.name()))}")
      true
    } else {
      false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw QueryCompilationErrors.unsupportedTableOperationError(this, oldIdent, "RENAME TABLE")

  // ------------------------------------------------------------
  // Namespaces
  // ------------------------------------------------------------

  override def listNamespaces(): Array[Array[String]] = schemas().map(Array(_)).toArray

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) {
      listNamespaces()
    } else if (namespaceExists(namespace)) {
      Array.empty
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(schema) => schemas().contains(schema)
    case _ => false
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace)
    }
    util.Collections.emptyMap()
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    val schema = dbSchema(namespace)
    checkProperties(metadata, "namespace")
    if (namespaceExists(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace)
    }
    execute(s"CREATE SCHEMA ${sqlName(schema, None)}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw QueryCompilationErrors.catalogOperationNotSupported(this, "ALTER NAMESPACE")

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    if (!namespaceExists(namespace)) {
      false
    } else if (cascade) {
      throw QueryCompilationErrors.catalogOperationNotSupported(this, "DROP NAMESPACE CASCADE")
    } else if (tables(namespace.head, None).nonEmpty) {
      throw new NonEmptyNamespaceException(namespace)
    } else {
      execute(s"DROP SCHEMA ${sqlName(namespace.head, None)}")
      true
    }
  }

  private def execute(sql: String): Unit = withConnection { conn =>
    Using.resource(conn.createStatement()) { stmt =>
      stmt.setSqlQuery(sql)
      stmt.executeUpdate()
    }
  }
}

/** Reads the database objects that a driver reports with `getObjects`. */
private[adbc] object AdbcCatalogObjects {

  /** Returns the names of the database schemas of a catalog, or of all catalogs if None. */
  def schemas(conn: AdbcConnection, catalog: Option[String]): Seq[String] =
    objects(conn, AdbcConnection.GetObjectsDepth.DB_SCHEMAS, catalog, None, None)
      .map(_._1).distinct

  /** Returns the names of the tables in a schema, or only `table` if it exists. */
  def tables(
      conn: AdbcConnection,
      catalog: Option[String],
      schema: String,
      table: Option[String]): Seq[String] =
    objects(conn, AdbcConnection.GetObjectsDepth.TABLES, catalog, Some(schema), table)
      .flatMap(_._2).distinct

  /**
   * Returns each schema with its tables. The names given are patterns, where `_` and `%` match
   * any characters, so the result is filtered to the exact names.
   */
  private def objects(
      conn: AdbcConnection,
      depth: AdbcConnection.GetObjectsDepth,
      catalog: Option[String],
      schema: Option[String],
      table: Option[String]): Seq[(String, Seq[String])] = {
    val result = mutable.ArrayBuffer[(String, Seq[String])]()
    val reader = conn.getObjects(depth, catalog.orNull, schema.orNull, table.orNull, null, null)
    Using.resource(reader) { reader =>
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val catalogs = root.getVector("catalog_name")
        val schemas = root.getVector("catalog_db_schemas")
        val rows = (0 until root.getRowCount).filter { i =>
          catalog.forall(c => Option(catalogs.getObject(i)).map(_.toString).contains(c))
        }
        rows.foreach { i =>
          Option(schemas.getObject(i)).foreach { value =>
            value.asInstanceOf[util.List[util.Map[String, AnyRef]]].asScala.foreach { s =>
              val schemaName = Option(s.get("db_schema_name")).map(_.toString).getOrElse("")
              val tableNames = Option(s.get("db_schema_tables")).toSeq.flatMap { tables =>
                tables.asInstanceOf[util.List[util.Map[String, AnyRef]]].asScala
                  .map(_.get("table_name").toString)
              }
              result += schemaName -> tableNames.filter(n => table.forall(_ == n)).toSeq
            }
          }
        }
      }
    }
    result.filter { case (s, _) => schema.forall(_ == s) }.toSeq
  }
}
