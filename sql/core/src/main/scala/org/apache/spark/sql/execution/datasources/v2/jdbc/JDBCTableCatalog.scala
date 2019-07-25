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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalog.v2.{Identifier, TableChange}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.DataSourceV1TableCatalog
import org.apache.spark.sql.sources.v2.Table
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCTableCatalog extends DataSourceV1TableCatalog with Logging {

  private var _name: String = _
  private var options: JDBCOptions = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
    val map = options.asCaseSensitiveMap().asScala.toMap
    // The `JDBCOptions` checks the existence of the table option. This is required by JDBC v1, but
    // JDBC V2 only knows the table option when loading a table. Here we put a table option with a
    // fake value, so that it can pass the check of `JDBCOptions`.
    this.options = new JDBCOptions(map + (JDBCOptions.JDBC_TABLE_NAME -> "__invalid"))
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
    // In JDBC the tables must be in a database.
    // TODO: support default database.
    if (namespace.length != 1) {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  private def createOptionsWithTableName(ident: Identifier): JDBCOptions = {
    // TODO: if table name contains special chars, we should quote it w.r.t. the JDBC dialect.
    val tblName = (ident.namespace() :+ ident.name()).mkString(".")
    new JDBCOptions(options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> tblName))
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    // TODO: implement it when SHOW TABLES command support DS V2.
    throw new UnsupportedOperationException("list table")
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
    if (!partitions.isEmpty) {
      throw new UnsupportedOperationException("Cannot create JDBC table with partition")
    }
    // TODO: we can support this, but we need to add an API to `JdbcDialect` to generate the SQL
    //       statement to specify table options. Many options are not supported because of no table
    //       properties, e.g. the custom schema option, the partition column option, etc.
    if (!properties.isEmpty) {
      logWarning("Cannot create JDBC table with properties, these properties will be " +
        "ignored: " + properties.asScala.map { case (k, v) => s"$k=$v" }.mkString("[", ", ", "]"))
    }

    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(options.url)
    schema.fields.foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = JdbcUtils.getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    // TODO: support the `JDBC_CREATE_TABLE_COLUMN_TYPES` option, after we support table properties.
    val schemaStr = if (sb.length < 2) "" else sb.substring(2)
    val sql = s"CREATE TABLE $ident ($schemaStr)"
    withConnection { conn =>
      val statement = conn.createStatement
      statement.setQueryTimeout(options.queryTimeout)
      statement.executeUpdate(sql)
    }

    JDBCTable(ident, schema, createOptionsWithTableName(ident))
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // TODO: support this by adding more APIs to `JdbcDialect` which can generate the SQL statement
    //       to alter a table.
    throw new UnsupportedOperationException("alter table")
  }

  override def dropTable(ident: Identifier): Boolean = {
    try {
      withConnection { conn =>
        val statement = conn.createStatement
        statement.setQueryTimeout(options.queryTimeout)
        statement.executeUpdate(s"DROP TABLE $ident")
        true
      }
    } catch {
      case _: SQLException => false
    }
  }
}
