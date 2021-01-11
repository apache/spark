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

import java.sql.{Connection, SQLException}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JDBCRDD, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCTableCatalog extends TableCatalog with SupportsNamespaces with Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper

  private var catalogName: String = null
  private var options: JDBCOptions = _
  private var dialect: JdbcDialect = _

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
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    withConnection { conn =>
      val schemaPattern = if (namespace.length == 1) namespace.head else null
      val rs = conn.getMetaData
        .getTables(null, schemaPattern, "%", Array("TABLE"));
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
    classifyException(s"Failed table existence check: $ident") {
      withConnection(JdbcUtils.tableExists(_, writeOptions))
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    withConnection { conn =>
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
    withConnection { conn =>
      classifyException(s"Failed table renaming from $oldIdent to $newIdent") {
        JdbcUtils.renameTable(conn, getTableName(oldIdent), getTableName(newIdent), options)
      }
    }
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val optionsWithTableName = new JDBCOptions(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    try {
      val schema = JDBCRDD.resolveTable(optionsWithTableName)
      JDBCTable(ident, schema, optionsWithTableName)
    } catch {
      case _: SQLException => throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    checkNamespace(ident.namespace())
    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException("Cannot create JDBC table with partition")
    }

    var tableOptions = options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident))
    var tableComment: String = ""
    var tableProperties: String = ""
    if (!properties.isEmpty) {
      properties.asScala.foreach {
        case (k, v) => k match {
          case TableCatalog.PROP_COMMENT => tableComment = v
          case TableCatalog.PROP_PROVIDER =>
            throw new AnalysisException("CREATE TABLE ... USING ... is not supported in" +
              " JDBC catalog.")
          case TableCatalog.PROP_OWNER => // owner is ignored. It is default to current user name.
          case TableCatalog.PROP_LOCATION =>
            throw new AnalysisException("CREATE TABLE ... LOCATION ... is not supported in" +
              " JDBC catalog.")
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
    withConnection { conn =>
      classifyException(s"Failed table creation: $ident") {
        JdbcUtils.createTable(conn, getTableName(ident), schema, caseSensitive, writeOptions)
      }
    }

    JDBCTable(ident, schema, writeOptions)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    checkNamespace(ident.namespace())
    withConnection { conn =>
      classifyException(s"Failed table altering: $ident") {
        JdbcUtils.alterTable(conn, getTableName(ident), changes, options)
      }
      loadTable(ident)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(db) =>
      withConnection { conn =>
        val rs = conn.getMetaData.getSchemas(null, db)
        while (rs.next()) {
          if (rs.getString(1) == db) return true;
        }
        false
      }
    case _ => false
  }

  override def listNamespaces(): Array[Array[String]] = {
    withConnection { conn =>
      val schemaBuilder = ArrayBuilder.make[Array[String]]
      val rs = conn.getMetaData.getSchemas()
      while (rs.next()) {
        schemaBuilder += Array(rs.getString(1))
      }
      schemaBuilder.result
    }
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(_) if namespaceExists(namespace) =>
        Array()
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        if (!namespaceExists(namespace)) throw new NoSuchNamespaceException(db)
        mutable.HashMap[String, String]().asJava

      case _ =>
        throw new NoSuchNamespaceException(namespace)
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
              throw new AnalysisException("CREATE NAMESPACE ... LOCATION ... is not supported in" +
                " JDBC catalog.")
            case _ =>
              throw new AnalysisException(s"CREATE NAMESPACE with property $k is not supported in" +
                " JDBC catalog.")
          }
        }
      }
      withConnection { conn =>
        classifyException(s"Failed create name space: $db") {
          JdbcUtils.createNamespace(conn, options, db, comment)
        }
      }

    case Array(_) =>
      throw new NamespaceAlreadyExistsException(namespace)

    case _ =>
      throw new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    namespace match {
      case Array(db) =>
        changes.foreach {
          case set: NamespaceChange.SetProperty =>
            if (set.property() == SupportsNamespaces.PROP_COMMENT) {
              withConnection { conn =>
                JdbcUtils.createNamespaceComment(conn, options, db, set.value)
              }
            } else {
              throw new AnalysisException(s"SET NAMESPACE with property ${set.property} " +
                "is not supported in JDBC catalog.")
            }

          case unset: NamespaceChange.RemoveProperty =>
            if (unset.property() == SupportsNamespaces.PROP_COMMENT) {
              withConnection { conn =>
                JdbcUtils.removeNamespaceComment(conn, options, db)
              }
            } else {
              throw new AnalysisException(s"Remove NAMESPACE property ${unset.property} " +
                "is not supported in JDBC catalog.")
            }

          case _ =>
            throw new AnalysisException(s"Unsupported NamespaceChange $changes in JDBC catalog.")
        }

      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def dropNamespace(namespace: Array[String]): Boolean = namespace match {
    case Array(db) if namespaceExists(namespace) =>
      if (listTables(Array(db)).nonEmpty) {
        throw new IllegalStateException(s"Namespace ${namespace.quoted} is not empty")
      }
      withConnection { conn =>
        classifyException(s"Failed drop name space: $db") {
          JdbcUtils.dropNamespace(conn, options, db)
          true
        }
      }

    case _ =>
      throw new NoSuchNamespaceException(namespace)
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    // In JDBC there is no nested database/schema
    if (namespace.length > 1) {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  private def withConnection[T](f: Connection => T): T = {
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  private def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
  }

  private def classifyException[T](message: String)(f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable => throw dialect.classifyException(message, e)
    }
  }
}
