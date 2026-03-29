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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{NonEmptyNamespaceException, NoSuchNamespaceException, NoSuchViewException, ViewAlreadyExistsException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An in-memory [[ViewCatalog]] implementation for testing.
 *
 * Views are stored in a [[ConcurrentHashMap]] keyed by [[Identifier]]. The catalog also
 * implements [[SupportsNamespaces]] so that namespaces can be created up-front in tests.
 */
class InMemoryViewCatalog extends ViewCatalog with SupportsNamespaces {

  private var _name: String = _

  protected val namespaces: java.util.Map[List[String], Map[String, String]] =
    new ConcurrentHashMap[List[String], Map[String, String]]()

  protected val views: java.util.Map[Identifier, ViewInfo] =
    new ConcurrentHashMap[Identifier, ViewInfo]()

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
  }

  override def name(): String = _name

  // ---- ViewCatalog ----

  override def listViews(namespace: String*): Array[Identifier] = {
    views.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray
  }

  override def loadView(ident: Identifier): View = {
    val info = views.get(ident)
    if (info == null) throw new NoSuchViewException(ident)
    toView(info)
  }

  override def createView(
      viewInfo: ViewInfo): View = {
    if (views.putIfAbsent(viewInfo.ident(), viewInfo) != null) {
      throw new ViewAlreadyExistsException(viewInfo.ident())
    }
    toView(viewInfo)
  }

  override def alterView(ident: Identifier, changes: ViewChange*): View = {
    val info = views.get(ident)
    if (info == null) throw new NoSuchViewException(ident)
    // Minimal implementation: apply SetProperties / RemoveProperty changes.
    val updatedProps = changes.foldLeft(info.properties().asScala.toMap) {
      case (props, set: ViewChange.SetProperty) => props + (set.property() -> set.value())
      case (props, remove: ViewChange.RemoveProperty) => props - remove.property()
      case (props, _) => props
    }
    val updated = new ViewInfo(
      info.ident(), info.sql(), info.currentCatalog(), info.currentNamespace(),
      info.schema(), info.queryColumnNames(), info.columnAliases(), info.columnComments(),
      updatedProps.asJava)
    views.put(ident, updated)
    toView(updated)
  }

  override def dropView(ident: Identifier): Boolean = {
    views.remove(ident) != null
  }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val info = views.remove(oldIdent)
    if (info == null) throw new NoSuchViewException(oldIdent)
    if (views.putIfAbsent(newIdent, info) != null) {
      views.put(oldIdent, info)
      throw new ViewAlreadyExistsException(newIdent)
    }
  }

  // ---- SupportsNamespaces ----

  override def listNamespaces(): Array[Array[String]] = {
    namespaces.keySet.asScala.map(_.toArray).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespaces.keySet.asScala
      .filter(ns => ns.length == namespace.length + 1 && ns.startsWith(namespace.toList))
      .map(_.toArray)
      .toArray
  }

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    val meta = namespaces.get(namespace.toList)
    if (meta == null) throw new NoSuchNamespaceException(namespace)
    meta.asJava
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: java.util.Map[String, String]): Unit = {
    namespaces.putIfAbsent(namespace.toList, metadata.asScala.toMap)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    val meta = namespaces.get(namespace.toList)
    if (meta == null) throw new NoSuchNamespaceException(namespace)
    val updated = changes.foldLeft(meta) {
      case (m, set: NamespaceChange.SetProperty) => m + (set.property() -> set.value())
      case (m, remove: NamespaceChange.RemoveProperty) => m - remove.property()
      case (m, _) => m
    }
    namespaces.put(namespace.toList, updated)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    if (!cascade && views.keySet.asScala.exists(_.namespace.sameElements(namespace))) {
      throw NonEmptyNamespaceException(namespace, null)
    }
    namespaces.remove(namespace.toList) != null
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    namespaces.containsKey(namespace.toList)
  }

  // ---- Helpers ----

  private def toView(info: ViewInfo): View = new View {
    override def name(): String = info.ident().name()
    override def query(): String = info.sql()
    override def currentCatalog(): String = info.currentCatalog()
    override def currentNamespace(): Array[String] = info.currentNamespace()
    override def schema(): StructType = info.schema()
    override def queryColumnNames(): Array[String] = info.queryColumnNames()
    override def columnAliases(): Array[String] = info.columnAliases()
    override def columnComments(): Array[String] = info.columnComments()
    override def properties(): java.util.Map[String, String] = info.properties()
  }
}
