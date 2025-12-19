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

package org.apache.spark.sql.connector

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, DelegatingCatalogExtension, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/**
 * A V2SessionCatalog implementation that can be extended to generate arbitrary `Table` definitions
 * for testing DDL as well as write operations (through df.write.saveAsTable, df.write.insertInto
 * and SQL), also supports v2 function operations.
 */
private[connector] trait TestV2SessionCatalogBase[T <: Table] extends DelegatingCatalogExtension {

  protected val tables: java.util.Map[Identifier, T] = new ConcurrentHashMap[Identifier, T]()
  protected val functions: java.util.Map[Identifier, UnboundFunction] =
    new ConcurrentHashMap[Identifier, UnboundFunction]()

  private val tableCreated: AtomicBoolean = new AtomicBoolean(false)
  private val funcCreated: AtomicBoolean = new AtomicBoolean(false)

  def checkUsage(): Unit = {
    assert(tableCreated.get || funcCreated.get,
      "Either tables or functions are not created, maybe didn't use the session catalog code path?")
  }

  private def addTable(ident: Identifier, table: T): Unit = {
    tableCreated.set(true)
    tables.put(ident, table)
  }

  protected def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): T

  override def loadTable(ident: Identifier): Table = {
    if (tables.containsKey(ident)) {
      tables.get(ident)
    } else {
      // Table was created through the built-in catalog via v1 command, this is OK as the
      // `loadTable` should always be invoked, and we set the `tableCreated` to pass validation.
      tableCreated.set(true)
      super.loadTable(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
    val key = TestV2SessionCatalogBase.SIMULATE_ALLOW_EXTERNAL_PROPERTY
    val newProps = new java.util.HashMap[String, String]()
    newProps.putAll(properties)
    if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      newProps.put(TableCatalog.PROP_EXTERNAL, "true")
    }

    val propsWithLocation = if (newProps.containsKey(key)) {
      // Always set a location so that CREATE EXTERNAL TABLE won't fail with LOCATION not specified.
      if (!newProps.containsKey(TableCatalog.PROP_LOCATION)) {
        newProps.put(TableCatalog.PROP_LOCATION, "file:/abc")
        newProps
      } else {
        newProps
      }
    } else {
      newProps
    }
    super.createTable(ident, columns, partitions, propsWithLocation)
    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    val t = newTable(ident.quoted, schema, partitions, propsWithLocation)
    addTable(ident, t)
    t
  }

  override def dropTable(ident: Identifier): Boolean = {
    tables.remove(ident)
    super.dropTable(ident)
  }

  def clearTables(): Unit = {
    tables.keySet().asScala.foreach(super.dropTable)
    tables.clear()
    tableCreated.set(false)
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    (Try(listFunctions0(namespace)), Try(super.listFunctions(namespace))) match {
      case (Success(v2), Success(v1)) => v2 ++ v1
      case (Success(v2), Failure(_)) => v2
      case (Failure(_), Success(v1)) => v1
      case (Failure(_), Failure(_)) =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  private def listFunctions0(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty || namespaceExists(namespace)) {
      functions.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    Option(functions.get(ident)) match {
      case Some(func) => func
      case _ =>
        super.loadFunction(ident)
    }
  }

  override def functionExists(ident: Identifier): Boolean = {
    functions.containsKey(ident) || super.functionExists(ident)
  }

  def createFunction(ident: Identifier, fn: UnboundFunction): UnboundFunction = {
    funcCreated.set(true)
    functions.put(ident, fn)
  }

  def dropFunction(ident: Identifier): Unit = {
    functions.remove(ident)
  }

  def clearFunctions(): Unit = {
    functions.clear()
    funcCreated.set(false)
  }
}

object TestV2SessionCatalogBase {
  val SIMULATE_ALLOW_EXTERNAL_PROPERTY = "spark.sql.test.simulateAllowExternal"
}
