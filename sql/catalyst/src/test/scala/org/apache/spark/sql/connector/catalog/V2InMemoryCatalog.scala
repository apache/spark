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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchFunctionException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2InMemoryCatalog extends BasicInMemoryCatalog with SupportsNamespaces {
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
    listNamespaces(namespace).foreach(dropNamespace)
    try {
      listTables(namespace).foreach(dropTable)
    } catch {
      case _: NoSuchNamespaceException =>
    }
    Option(namespaces.remove(namespace.toList)).isDefined
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty || namespaceExists(namespace)) {
      super.listTables(namespace)
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty || namespaceExists(namespace)) {
      super.listFunctions(namespace)
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }
}

object V2InMemoryCatalog {
  val SIMULATE_FAILED_CREATE_PROPERTY = "spark.sql.test.simulateFailedCreate"
  val SIMULATE_DROP_BEFORE_REPLACE_PROPERTY = "spark.sql.test.simulateDropBeforeReplace"

  def maybeSimulateFailedTableCreation(tableProperties: util.Map[String, String]): Unit = {
    if ("true".equalsIgnoreCase(tableProperties.get(SIMULATE_FAILED_CREATE_PROPERTY))) {
      throw new IllegalStateException("Manual create table failure.")
    }
  }
}
