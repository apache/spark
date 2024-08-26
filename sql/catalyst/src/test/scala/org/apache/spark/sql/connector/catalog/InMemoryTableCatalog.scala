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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NonEmptyNamespaceException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class BasicInMemoryTableCatalog extends TableCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected val namespaces: util.Map[List[String], Map[String, String]] =
    new ConcurrentHashMap[List[String], Map[String, String]]()

  protected val tables: util.Map[Identifier, Table] =
    new ConcurrentHashMap[Identifier, Table]()

  private val invalidatedTables: util.Set[Identifier] = ConcurrentHashMap.newKeySet()

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
        throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    val versionIdent = Identifier.of(ident.namespace, ident.name + version)
    Option(tables.get(versionIdent)) match {
      case Some(table) =>
        table
      case _ =>
        throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
  }

  override def loadTable(ident: Identifier, timestamp: Long): Table = {
    val timestampIdent = Identifier.of(ident.namespace, ident.name + timestamp)
    Option(tables.get(timestampIdent)) match {
      case Some(table) =>
        table
      case _ =>
        throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
  }

  override def invalidateTable(ident: Identifier): Unit = {
    invalidatedTables.add(ident)
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    createTable(ident, columns, partitions, properties, Distributions.unspecified(),
      Array.empty, None, None)
  }

  def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String],
      distribution: Distribution,
      ordering: Array[SortOrder],
      requiredNumPartitions: Option[Int],
      advisoryPartitionSize: Option[Long],
      distributionStrictlyRequired: Boolean = true,
      numRowsPerSplit: Int = Int.MaxValue): Table = {
    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }

    InMemoryTableCatalog.maybeSimulateFailedTableCreation(properties)

    val tableName = s"$name.${ident.quoted}"
    val table = new InMemoryTable(tableName, schema, partitions, properties, distribution,
      ordering, requiredNumPartitions, advisoryPartitionSize, distributionStrictlyRequired,
      numRowsPerSplit)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident).asInstanceOf[InMemoryTable]
    val properties = CatalogV2Util.applyPropertiesChanges(table.properties, changes)
    val schema = CatalogV2Util.applySchemaChanges(table.schema, changes, None, "ALTER TABLE")
    val finalPartitioning = CatalogV2Util.applyClusterByChanges(table.partitioning, schema, changes)

    // fail if the last column in the schema was dropped
    if (schema.fields.isEmpty) {
      throw new IllegalArgumentException(s"Cannot drop all fields")
    }

    val newTable = new InMemoryTable(table.name, schema, finalPartitioning, properties)
      .withData(table.data)

    tables.put(ident, newTable)

    newTable
  }

  override def dropTable(ident: Identifier): Boolean = Option(tables.remove(ident)).isDefined

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (tables.containsKey(newIdent)) {
      throw new TableAlreadyExistsException(newIdent.asMultipartIdentifier)
    }

    Option(tables.remove(oldIdent)) match {
      case Some(table) =>
        tables.put(newIdent, table)
      case _ =>
        throw new NoSuchTableException(oldIdent.asMultipartIdentifier)
    }
  }

  def isTableInvalidated(ident: Identifier): Boolean = {
    invalidatedTables.contains(ident)
  }

  def clearTables(): Unit = {
    tables.clear()
  }
}

class InMemoryTableCatalog extends BasicInMemoryTableCatalog with SupportsNamespaces {

  override def capabilities: java.util.Set[TableCatalogCapability] = {
    Set(
      TableCatalogCapability.SUPPORT_COLUMN_DEFAULT_VALUE,
      TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS
    ).asJava
  }

  protected def allNamespaces: Seq[Seq[String]] = {
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
        throw new NoSuchNamespaceException(name() +: namespace)
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

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    try {
      if (!cascade) {
        if (listTables(namespace).nonEmpty || listNamespaces(namespace).nonEmpty) {
          throw new NonEmptyNamespaceException(namespace)
        }
      } else {
        listNamespaces(namespace).foreach(namespace => dropNamespace(namespace, cascade))
        listTables(namespace).foreach(dropTable)
      }
    } catch {
      case _: NoSuchNamespaceException =>
    }
    Option(namespaces.remove(namespace.toList)).isDefined
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty || namespaceExists(namespace)) {
      super.listTables(namespace)
    } else {
      throw new NoSuchNamespaceException(name() +: namespace)
    }
  }
}

object InMemoryTableCatalog {
  val SIMULATE_FAILED_CREATE_PROPERTY = "spark.sql.test.simulateFailedCreate"
  val SIMULATE_DROP_BEFORE_REPLACE_PROPERTY = "spark.sql.test.simulateDropBeforeReplace"

  def maybeSimulateFailedTableCreation(tableProperties: util.Map[String, String]): Unit = {
    if ("true".equalsIgnoreCase(tableProperties.get(SIMULATE_FAILED_CREATE_PROPERTY))) {
      throw new IllegalStateException("Manual create table failure.")
    }
  }
}
