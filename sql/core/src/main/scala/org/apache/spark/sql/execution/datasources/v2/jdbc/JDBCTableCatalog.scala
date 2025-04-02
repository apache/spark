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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.SQLException
import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, FunctionCatalog, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JDBCRDD, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCTableCatalog extends TableCatalog
  with SupportsNamespaces
  with FunctionCatalog
  with DataTypeErrorsBase
  with Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private var catalogName: String = null
  private var options: JDBCOptions = _
  private var dialect: JdbcDialect = _
  private var functions: Map[String, UnboundFunction] = _

  override def name(): String = {
    require(catalogName != null, "The JDBC table catalog is not initialed")
    catalogName
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName == null, "The JDBC table catalog is already initialed")
    catalogName = name

    val map = options.asCaseSensitiveMap().asScala.toMap
    // The `JDBCOptions` checks the existence of the table option. This is required by JDBC v1, but
    // JDBC V2 only knows the table option when loading a table. Here we put a table option with a
    // fake value, so that it can pass the check of `JDBCOptions`.
    this.options = new JDBCOptions(map + (JDBCOptions.JDBC_TABLE_NAME -> "__invalid_dbtable"))
    dialect = JdbcDialects.get(this.options.url)
    functions = dialect.functions.toMap
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    JdbcUtils.withConnection(options) { conn =>
      val schemaPattern = if (namespace.length == 1) namespace.head else null
      val rs = JdbcUtils.classifyException(
        condition = "FAILED_JDBC.GET_TABLES",
        messageParameters = Map(
          "url" -> options.getRedactUrl(),
          "namespace" -> toSQLId(namespace.toSeq)),
        dialect,
        description = s"Failed get tables from: ${namespace.mkString(".")}",
        isRuntime = false) {
        conn.getMetaData.getTables(null, schemaPattern, "%", Array("TABLE"))
      }
      new Iterator[Identifier] {
        def hasNext = rs.next()
        def next() = Identifier.of(namespace, rs.getString("TABLE_NAME"))
      }.toArray
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    val writeOptions = new JdbcOptionsInWrite(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    JdbcUtils.classifyException(
      condition = "FAILED_JDBC.TABLE_EXISTS",
      messageParameters = Map(
        "url" -> options.getRedactUrl(),
        "tableName" -> toSQLId(ident)),
      dialect,
      description = s"Failed table existence check: $ident",
      isRuntime = false) {
      JdbcUtils.withConnection(options)(JdbcUtils.tableExists(_, writeOptions))
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    JdbcUtils.withConnection(options) { conn =>
      try {
        JdbcUtils.dropTable(conn, getTableName(ident), options)
        true
      } catch {
        case _: SQLException => false
      }
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    checkNamespace(oldIdent.namespace())
    JdbcUtils.withConnection(options) { conn =>
      JdbcUtils.classifyException(
        condition = "FAILED_JDBC.RENAME_TABLE",
        messageParameters = Map(
          "url" -> options.getRedactUrl(),
          "oldName" -> toSQLId(oldIdent),
          "newName" -> toSQLId(newIdent)),
        dialect,
        description = s"Failed table renaming from $oldIdent to $newIdent",
        isRuntime = false) {
        JdbcUtils.renameTable(conn, oldIdent, newIdent, options)
      }
    }
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val optionsWithTableName = new JDBCOptions(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    JdbcUtils.classifyException(
      condition = "FAILED_JDBC.LOAD_TABLE",
      messageParameters = Map(
        "url" -> options.getRedactUrl(),
        "tableName" -> toSQLId(ident)),
      dialect,
      description = s"Failed to load table: $ident",
      isRuntime = false
    ) {
      val schema = JDBCRDD.resolveTable(optionsWithTableName)
      JDBCTable(ident, schema, optionsWithTableName)
    }
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    checkNamespace(ident.namespace())
    if (partitions.nonEmpty) {
      throw QueryExecutionErrors.cannotCreateJDBCTableWithPartitionsError()
    }

    var tableOptions = options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident))
    var tableComment: String = ""
    var tableProperties: String = ""
    if (!properties.isEmpty) {
      properties.asScala.foreach {
        case (k, v) => k match {
          case TableCatalog.PROP_COMMENT => tableComment = v
          case TableCatalog.PROP_PROVIDER =>
            throw QueryCompilationErrors.cannotCreateJDBCTableUsingProviderError()
          case TableCatalog.PROP_OWNER => // owner is ignored. It is default to current user name.
          case TableCatalog.PROP_LOCATION =>
            throw QueryCompilationErrors.cannotCreateJDBCTableUsingLocationError()
          case _ => tableProperties = tableProperties + " " + s"$k $v"
        }
      }
    }

    if (tableComment != "") {
      tableOptions = tableOptions + (JDBCOptions.JDBC_TABLE_COMMENT -> tableComment)
    }
    if (tableProperties != "") {
      // table property is set in JDBC_CREATE_TABLE_OPTIONS, which will be appended
      // to CREATE TABLE statement.
      // E.g., "CREATE TABLE t (name string) ENGINE InnoDB DEFAULT CHARACTER SET utf8"
      // Spark doesn't check if these table properties are supported by databases. If
      // table property is invalid, database will fail the table creation.
      tableOptions = tableOptions + (JDBCOptions.JDBC_CREATE_TABLE_OPTIONS -> tableProperties)
    }

    val writeOptions = new JdbcOptionsInWrite(tableOptions)
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    JdbcUtils.withConnection(options) { conn =>
      JdbcUtils.classifyException(
        condition = "FAILED_JDBC.CREATE_TABLE",
        messageParameters = Map(
          "url" -> options.getRedactUrl(),
          "tableName" -> toSQLId(ident)),
        dialect,
        description = s"Failed table creation: $ident",
        isRuntime = false) {
        JdbcUtils.createTable(conn, getTableName(ident), schema, caseSensitive, writeOptions)
      }
    }

    JDBCTable(ident, schema, writeOptions)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    checkNamespace(ident.namespace())
    JdbcUtils.withConnection(options) { conn =>
      JdbcUtils.classifyException(
        condition = "FAILED_JDBC.ALTER_TABLE",
        messageParameters = Map(
          "url" -> options.getRedactUrl(),
          "tableName" -> toSQLId(ident)),
        dialect,
        description = s"Failed table altering: $ident",
        isRuntime = false) {
        JdbcUtils.alterTable(conn, getTableName(ident), changes, options)
      }
      loadTable(ident)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(db) =>
      JdbcUtils.withConnection(options) { conn =>
        JdbcUtils.classifyException(
          condition = "FAILED_JDBC.NAMESPACE_EXISTS",
          messageParameters = Map(
            "url" -> options.getRedactUrl(),
            "namespace" -> toSQLId(namespace.toSeq)),
          dialect,
          description = s"Failed namespace exists: ${namespace.mkString}",
          isRuntime = false) {
          JdbcUtils.schemaExists(conn, options, db)
        }
      }
    case _ => false
  }

  override def listNamespaces(): Array[Array[String]] = {
    JdbcUtils.withConnection(options) { conn =>
      JdbcUtils.classifyException(
        condition = "FAILED_JDBC.LIST_NAMESPACES",
        messageParameters = Map("url" -> options.getRedactUrl()),
        dialect,
        description = s"Failed list namespaces",
        isRuntime = false) {
        JdbcUtils.listSchemas(conn, options)
      }
    }
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(_) if namespaceExists(namespace) =>
        Array()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        if (!namespaceExists(namespace)) {
          throw QueryCompilationErrors.noSuchNamespaceError(Array(db))
        }
        mutable.HashMap[String, String]().asJava

      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = namespace match {
    case Array(db) if !namespaceExists(namespace) =>
      var comment = ""
      if (!metadata.isEmpty) {
        metadata.asScala.foreach {
          case (k, v) => k match {
            case SupportsNamespaces.PROP_COMMENT => comment = v
            case SupportsNamespaces.PROP_OWNER => // ignore
            case SupportsNamespaces.PROP_LOCATION =>
              throw QueryCompilationErrors.cannotCreateJDBCNamespaceUsingProviderError()
            case _ =>
              throw QueryCompilationErrors.cannotCreateJDBCNamespaceWithPropertyError(k)
          }
        }
      }
      JdbcUtils.withConnection(options) { conn =>
        JdbcUtils.classifyException(
          condition = "FAILED_JDBC.CREATE_NAMESPACE",
          messageParameters = Map(
            "url" -> options.getRedactUrl(),
            "namespace" -> toSQLId(db)),
          dialect,
          description = s"Failed create name space: $db",
          isRuntime = false) {
          JdbcUtils.createSchema(conn, options, db, comment)
        }
      }

    case Array(_) =>
      throw QueryCompilationErrors.namespaceAlreadyExistsError(namespace)

    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    namespace match {
      case Array(db) =>
        changes.foreach {
          case set: NamespaceChange.SetProperty =>
            if (set.property() == SupportsNamespaces.PROP_COMMENT) {
              JdbcUtils.withConnection(options) { conn =>
                JdbcUtils.classifyException(
                  condition = "FAILED_JDBC.CREATE_NAMESPACE_COMMENT",
                  messageParameters = Map(
                    "url" -> options.getRedactUrl(),
                    "namespace" -> toSQLId(db)),
                  dialect,
                  description = s"Failed create comment on name space: $db",
                  isRuntime = false) {
                  JdbcUtils.alterSchemaComment(conn, options, db, set.value)
                }
              }
            } else {
              throw QueryCompilationErrors.cannotSetJDBCNamespaceWithPropertyError(set.property)
            }

          case unset: NamespaceChange.RemoveProperty =>
            if (unset.property() == SupportsNamespaces.PROP_COMMENT) {
              JdbcUtils.withConnection(options) { conn =>
                JdbcUtils.classifyException(
                  condition = "FAILED_JDBC.REMOVE_NAMESPACE_COMMENT",
                  messageParameters = Map(
                    "url" -> options.getRedactUrl(),
                    "namespace" -> toSQLId(db)),
                  dialect,
                  description = s"Failed remove comment on name space: $db",
                  isRuntime = false) {
                  JdbcUtils.removeSchemaComment(conn, options, db)
                }
              }
            } else {
              throw QueryCompilationErrors.cannotUnsetJDBCNamespaceWithPropertyError(unset.property)
            }

          case _ =>
            throw QueryCompilationErrors.unsupportedJDBCNamespaceChangeInCatalogError(changes)
        }

      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def dropNamespace(
      namespace: Array[String],
      cascade: Boolean): Boolean = namespace match {
    case Array(db) if namespaceExists(namespace) =>
      JdbcUtils.withConnection(options) { conn =>
        JdbcUtils.classifyException(
          condition = "FAILED_JDBC.DROP_NAMESPACE",
          messageParameters = Map(
            "url" -> options.getRedactUrl(),
            "namespace" -> toSQLId(db)),
          dialect,
          description = s"Failed drop name space: $db",
          isRuntime = false) {
          JdbcUtils.dropSchema(conn, options, db, cascade)
          true
        }
      }

    case _ =>
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    // In JDBC there is no nested database/schema
    if (namespace.length > 1) {
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  private def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty) {
      functions.keys.map(Identifier.of(namespace, _)).toArray
    } else {
      Array.empty[Identifier]
    }
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    if (ident.namespace().nonEmpty) {
      throw QueryCompilationErrors.noSuchFunctionError(ident.asFunctionIdentifier)
    }
    functions.get(ident.name()) match {
      case Some(func) =>
        func
      case _ =>
        throw new NoSuchFunctionException(ident)
    }
  }

  private def toSQLId(ident: Identifier): String = {
    toSQLId(ident.namespace.toSeq :+ ident.name)
  }
}
