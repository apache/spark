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

package org.apache.spark.sql.catalog.v2

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.sources.v2.{Table, TableCapability}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TestTableCatalog extends TableCatalog with SupportsNamespaces {
  import CatalogV2Implicits._

  override val defaultNamespace: Array[String] = Array()
  protected val namespaces: util.Map[List[String], Map[String, String]] =
    new ConcurrentHashMap[List[String], Map[String, String]]()

  private val tables: util.Map[Identifier, Table] = new ConcurrentHashMap[Identifier, Table]()
  private var _name: Option[String] = None

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = Some(name)
  }

  override def name: String = _name.get

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    tables.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    Option(tables.get(ident)) match {
      case Some(table) =>
        table
      case _ =>
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident)
    }

    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Catalog $name: Partitioned tables are not supported")
    }

    val table = InMemoryTable(ident.quoted, schema, properties)

    tables.put(ident, table)

    table
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident)
    val properties = CatalogV2Util.applyPropertiesChanges(table.properties, changes)
    val schema = CatalogV2Util.applySchemaChanges(table.schema, changes)
    val newTable = InMemoryTable(table.name, schema, properties)

    tables.put(ident, newTable)

    newTable
  }

  override def dropTable(ident: Identifier): Boolean = Option(tables.remove(ident)).isDefined

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (tables.containsKey(newIdent)) {
      throw new TableAlreadyExistsException(newIdent)
    }

    Option(tables.remove(oldIdent)) match {
      case Some(table) =>
        tables.put(newIdent, InMemoryTable(table.name, table.schema, table.properties))
      case _ =>
        throw new NoSuchTableException(oldIdent)
    }
  }

  private def allNamespaces: Seq[Seq[String]] = {
    (tables.keySet.asScala.map(_.namespace.toSeq) ++ namespaces.keySet.asScala).toSeq.distinct
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    allNamespaces.exists(_.startsWith(namespace))
  }

  override def listNamespaces: Array[Array[String]] = {
    allNamespaces.map(_.head).distinct.map(Array(_)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    allNamespaces
        .filter(_.size > namespace.length)
        .filter(_.startsWith(namespace))
        .map(_.take(namespace.length + 1))
        .distinct
        .map(_.toArray)
        .toArray
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    Option(namespaces.get(namespace.toSeq)) match {
      case Some(metadata) =>
        metadata.asJava
      case _ if namespaceExists(namespace) =>
        util.Collections.emptyMap[String, String]
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    if (namespaceExists(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace)
    }

    Option(namespaces.putIfAbsent(namespace.toList, metadata.asScala.toMap)) match {
      case Some(_) =>
        throw new NamespaceAlreadyExistsException(namespace)
      case _ =>
        // created successfully
    }
  }

  override def alterNamespace(
      namespace: Array[String],
      changes: NamespaceChange*): Unit = {
    val metadata = loadNamespaceMetadata(namespace).asScala.toMap
    namespaces.put(namespace.toList, CatalogV2Util.applyNamespaceChanges(metadata, changes))
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    if (listTables(namespace).nonEmpty) {
      throw new IllegalStateException(s"Cannot delete non-empty namespace: ${namespace.quoted}")
    }
    Option(namespaces.remove(namespace.toList)).isDefined
  }
}

case class InMemoryTable(
    name: String,
    schema: StructType,
    override val properties: util.Map[String, String]) extends Table {
  override def partitioning: Array[Transform] = Array.empty
  override def capabilities: util.Set[TableCapability] = InMemoryTable.CAPABILITIES
}

object InMemoryTable {
  val CAPABILITIES: util.Set[TableCapability] = Set.empty[TableCapability].asJava
}
