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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogManager.{
  SESSION_NAMESPACE,
  SYSTEM_CATALOG_NAME
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.LexicalThreadLocal

/**
 * A lexical registry of schema-only relations supplied by an analysis caller.
 *
 * Supplied names are stored as canonical identities. During lookup, a match
 * eclipses the real table or view of that identity (temp view, global temp
 * view, persistent table, or persistent view) for that analysis only.
 */
private[sql] object SuppliedRelationRegistry
  extends LexicalThreadLocal[Seq[(Seq[String], StructType)]] {

  val IDENTITY_TAG: TreeNodeTag[Seq[String]] =
    TreeNodeTag[Seq[String]]("suppliedRelationIdentity")

  def create(
      catalogManager: CatalogManager,
      relations: Seq[(Seq[String], StructType)]): Handle = {
    val entries = relations.map { case (nameParts, schema) =>
      canonicalize(catalogManager, nameParts) -> schema
    }
    createHandle(Some(entries))
  }

  def lookup(
      nameParts: Seq[String],
      resolver: (String, String) => Boolean): Option[StructType] = {
    get().flatMap(_.collectFirst {
      case (providedName, schema) if sameName(providedName, nameParts, resolver) => schema
    })
  }

  def isSupplied(plan: LogicalPlan): Boolean = plan match {
    case s: SubqueryAlias => isSupplied(s.child)
    case other => other.getTagValue(IDENTITY_TAG).isDefined
  }

  def sessionIdentity(name: String): Seq[String] = {
    Seq(SYSTEM_CATALOG_NAME, SESSION_NAMESPACE, name)
  }

  private def sameName(
      left: Seq[String],
      right: Seq[String],
      resolver: (String, String) => Boolean): Boolean = {
    left.length == right.length && left.zip(right).forall {
      case (a, b) => resolver(a, b)
    }
  }

  private def canonicalize(
      catalogManager: CatalogManager,
      nameParts: Seq[String]): Seq[String] = {
    if (CatalogManager.isSessionQualifiedViewName(nameParts)) {
      sessionIdentity(nameParts.last)
    } else if (nameParts.length == 2 &&
        catalogManager.v1SessionCatalog.isGlobalTempViewDB(nameParts.head)) {
      nameParts
    } else {
      nameParts match {
        case Seq(name) =>
          catalogManager.currentCatalog.name() +:
            catalogManager.currentNamespace.toSeq :+ name
        case Seq(namespace, name) =>
          catalogManager.currentCatalog.name() +: Seq(namespace, name)
        case catalog +: identifier =>
          catalog +: identifier
      }
    }
  }
}
