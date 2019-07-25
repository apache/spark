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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JDBCRDD, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCTableCatalog extends TableCatalog with Logging {

  private var _name: String = _
  private var options: JDBCOptions = _
  private var dialect: JdbcDialect = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
    val map = options.asCaseSensitiveMap().asScala.toMap
    // The `JDBCOptions` checks the existence of the table option. This is required by JDBC v1, but
    // JDBC V2 only knows the table option when loading a table. Here we put a table option with a
    // fake value, so that it can pass the check of `JDBCOptions`.
    this.options = new JDBCOptions(map + (JDBCOptions.JDBC_TABLE_NAME -> "__invalid"))
    this.dialect = JdbcDialects.get(this.options.url)
  }

  override def name(): String = {
    _name
  }

  private def withConnection[T](f: Connection => T): T = {
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    // In JDBC there is no nested database/schema
    if (namespace.length > 1) {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  private def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
  }

  private def createOptionsWithTableName(ident: Identifier): JDBCOptions = {
    new JDBCOptions(options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    withConnection { conn =>
      val md = conn.getMetaData
      val schemaPattern = if (namespace.length == 1) namespace.head else null
      val rs = md.getTables(null, schemaPattern, "%", Array("TABLE"));
      val tables = ArrayBuffer.empty[Identifier]
      while (rs.next()) {
        tables += Identifier.of(namespace, rs.getString("TABLE_NAME"))
      }
      tables.toArray
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    val ns = ident.namespace()
    checkNamespace(ns)
    withConnection { conn =>
      val md = conn.getMetaData
      val schemaPattern = if (ns.length == 1) ns.head else null
      val rs = md.getTables(null, schemaPattern, ident.name(), Array("TABLE"));
      rs.next()
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    // TODO: support this by adding more APIs to `JdbcDialect` which can generate the SQL statement
    //       to rename a table.
    throw new UnsupportedOperationException("rename table")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // TODO: support this by adding more APIs to `JdbcDialect` which can generate the SQL statement
    //       to alter a table.
    throw new UnsupportedOperationException("alter table")
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    try {
      withConnection { conn =>
        val statement = conn.createStatement
        statement.setQueryTimeout(options.queryTimeout)
        statement.executeUpdate(s"DROP TABLE ${getTableName(ident)}")
        true
      }
    } catch {
      case _: SQLException => false
    }
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val optionsWithTableName = createOptionsWithTableName(ident)
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
      properties: util.Map[String, String]): Table = {
    checkNamespace(ident.namespace())
    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException("Cannot create JDBC table with partition")
    }
    // TODO: we can support this, but we need to add an API to `JdbcDialect` to generate the SQL
    //       statement to specify table options.
    if (!properties.isEmpty) {
      logWarning("Cannot create JDBC table with properties, these properties will be " +
        "ignored: " + properties.asScala.map { case (k, v) => s"$k=$v" }.mkString("[", ", ", "]"))
    }

    val writeOptions = new JdbcOptionsInWrite(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    withConnection { conn =>
      JdbcUtils.createTable(conn, getTableName(ident), schema, caseSensitive, writeOptions)
    }

    JDBCTable(ident, schema, writeOptions)
  }
}
