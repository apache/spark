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

import java.sql.Connection

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCTableCatalog extends TableCatalog {

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
    val schemaPattern = if (namespace.length == 1) namespace.head else null
    withConnection { conn =>
      val rs = conn.getMetaData.getTables(null, schemaPattern, "%", Array("TABLE"));
      new Iterator[Identifier] {
        def hasNext = rs.next()
        def next = Identifier.of(namespace, rs.getString("TABLE_NAME"))
      }.toArray
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    // scalastyle:off throwerror
    throw new NotImplementedError()
    // scalastyle:on throwerror
  }

  override def dropTable(ident: Identifier): Boolean = {
    // scalastyle:off throwerror
    throw new NotImplementedError()
    // scalastyle:on throwerror
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    // scalastyle:off throwerror
    throw new NotImplementedError()
    // scalastyle:on throwerror
  }

  override def loadTable(ident: Identifier): Table = {
    // scalastyle:off throwerror
    throw new NotImplementedError()
    // scalastyle:on throwerror
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    // scalastyle:off throwerror
    throw new NotImplementedError()
    // scalastyle:on throwerror
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
}
