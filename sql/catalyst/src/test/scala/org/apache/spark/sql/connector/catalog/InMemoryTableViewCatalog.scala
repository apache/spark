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

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, NoSuchViewException, TableAlreadyExistsException, ViewAlreadyExistsException}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An in-memory [[TableViewCatalog]] for tests. Tables and views share a single keyspace per
 * the [[TableViewCatalog]] contract; the stored value's runtime type ([[TableInfo]] vs
 * [[ViewInfo]]) is the kind discriminator. Also implements [[SupportsNamespaces]] with a
 * minimal namespace store, so analyzer rules that read namespace metadata (e.g.
 * `ApplyDefaultCollation` consulting `loadNamespaceMetadata` for `PROP_COLLATION`) work
 * uniformly with the v1 session catalog. Suitable for any test suite that wants to exercise
 * v2 view DDL or inspection commands against a non-session catalog.
 */
class InMemoryTableViewCatalog extends TableViewCatalog with SupportsNamespaces {

  private val store =
    new ConcurrentHashMap[(Seq[String], String), TableInfo]()
  private val namespaces =
    new ConcurrentHashMap[Seq[String], util.Map[String, String]]()

  override def loadTableOrView(ident: Identifier): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    Option(store.get(key))
      .map(new MetadataTable(_, ident.toString))
      .getOrElse(throw new NoSuchTableException(ident))
  }

  // ----- TableCatalog -----------------------------------------------------------------

  override def createTable(ident: Identifier, info: TableInfo): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    if (store.putIfAbsent(key, info) != null) {
      throw new TableAlreadyExistsException(ident)
    }
    new MetadataTable(info, ident.toString)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable not supported on InMemoryTableViewCatalog")
  }

  override def dropTable(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    val existing = store.get(key)
    if (existing == null || existing.isInstanceOf[ViewInfo]) return false
    store.remove(key) != null
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldKey = (oldIdent.namespace().toSeq, oldIdent.name())
    val newKey = (newIdent.namespace().toSeq, newIdent.name())
    val existing = store.get(oldKey)
    if (existing == null || existing.isInstanceOf[ViewInfo]) {
      throw new NoSuchTableException(oldIdent)
    }
    if (store.putIfAbsent(newKey, existing) != null) {
      throw new TableAlreadyExistsException(newIdent)
    }
    store.remove(oldKey)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val target = namespace.toSeq
    val ids = new java.util.ArrayList[Identifier]()
    store.forEach { (key, info) =>
      if (key._1 == target && !info.isInstanceOf[ViewInfo]) {
        ids.add(Identifier.of(key._1.toArray, key._2))
      }
    }
    ids.toArray(new Array[Identifier](0))
  }

  // ----- ViewCatalog ------------------------------------------------------------------

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    val target = namespace.toSeq
    val ids = new java.util.ArrayList[Identifier]()
    store.forEach { (key, info) =>
      if (key._1 == target && info.isInstanceOf[ViewInfo]) {
        ids.add(Identifier.of(key._1.toArray, key._2))
      }
    }
    ids.toArray(new Array[Identifier](0))
  }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    if (store.putIfAbsent(key, info) != null) {
      throw new ViewAlreadyExistsException(ident)
    }
    info
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    val existing = store.get(key)
    if (existing == null || !existing.isInstanceOf[ViewInfo]) {
      throw new NoSuchViewException(ident)
    }
    store.put(key, info)
    info
  }

  override def dropView(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    val existing = store.get(key)
    if (existing == null || !existing.isInstanceOf[ViewInfo]) return false
    store.remove(key) != null
  }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldKey = (oldIdent.namespace().toSeq, oldIdent.name())
    val newKey = (newIdent.namespace().toSeq, newIdent.name())
    val existing = store.get(oldKey)
    if (existing == null || !existing.isInstanceOf[ViewInfo]) {
      throw new NoSuchViewException(oldIdent)
    }
    if (store.putIfAbsent(newKey, existing) != null) {
      throw new ViewAlreadyExistsException(newIdent)
    }
    store.remove(oldKey)
  }

  // ----- SupportsNamespaces -----------------------------------------------------------

  // A namespace exists if it was explicitly created or if any stored entry sits under it.
  private def implicitNamespaces: Set[Seq[String]] =
    store.keySet.asScala.iterator.map(_._1).toSet

  override def listNamespaces(): Array[Array[String]] = {
    val all = (namespaces.keySet.asScala ++ implicitNamespaces).toSet
    all.iterator.filter(_.nonEmpty).map(ns => Array(ns.head)).toArray.distinct
  }

  override def listNamespaces(parent: Array[String]): Array[Array[String]] = {
    val parentSeq = parent.toSeq
    val all = (namespaces.keySet.asScala ++ implicitNamespaces).toSet
    all.iterator
      .filter(_.size > parentSeq.size)
      .filter(_.startsWith(parentSeq))
      .map(_.take(parentSeq.size + 1).toArray)
      .toArray
      .distinct
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    val ns = namespace.toSeq
    namespaces.containsKey(ns) || implicitNamespaces.exists(_.startsWith(ns))
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    val ns = namespace.toSeq
    Option(namespaces.get(ns)) match {
      case Some(metadata) => metadata
      case _ if namespaceExists(namespace) => util.Collections.emptyMap[String, String]
      case _ => throw new NoSuchNamespaceException(name() +: namespace)
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    val ns = namespace.toSeq
    if (namespaces.putIfAbsent(ns, new util.HashMap[String, String](metadata)) != null) {
      throw new NamespaceAlreadyExistsException(namespace)
    }
  }

  override def alterNamespace(
      namespace: Array[String],
      changes: NamespaceChange*): Unit = {
    val ns = namespace.toSeq
    val current = Option(namespaces.get(ns)).getOrElse {
      if (!namespaceExists(namespace)) {
        throw new NoSuchNamespaceException(name() +: namespace)
      }
      new util.HashMap[String, String]()
    }
    val updated = CatalogV2Util.applyNamespaceChanges(current, changes.toSeq)
    namespaces.put(ns, updated)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    val ns = namespace.toSeq
    if (!cascade && implicitNamespaces.exists(_.startsWith(ns))) {
      throw new org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException(
        name() +: namespace)
    }
    if (cascade) {
      val keysToRemove = store.keySet.asScala.filter(_._1.startsWith(ns)).toSeq
      keysToRemove.foreach(store.remove)
    }
    namespaces.remove(ns) != null || implicitNamespaces.exists(_.startsWith(ns))
  }

  // Test-only accessors --------------------------------------------------------------

  /** Returns the stored entry (table or view) for the identifier, or throws if missing. */
  def getStoredInfo(namespace: Array[String], name: String): TableInfo = {
    Option(store.get((namespace.toSeq, name))).getOrElse {
      throw new NoSuchTableException(Identifier.of(namespace, name))
    }
  }

  /** Returns the stored ViewInfo, or throws if the entry is missing or is not a view. */
  def getStoredView(namespace: Array[String], name: String): ViewInfo = {
    getStoredInfo(namespace, name) match {
      case v: ViewInfo => v
      case _ => throw new IllegalStateException(
        s"stored entry at ${namespace.mkString(".")}.$name is not a view")
    }
  }

  // CatalogPlugin --------------------------------------------------------------------

  private var catalogName: String = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}
