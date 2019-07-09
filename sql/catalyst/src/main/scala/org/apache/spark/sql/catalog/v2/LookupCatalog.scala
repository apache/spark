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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogManager

/**
 * A trait to encapsulate catalog lookup function and helpful extractors.
 */
@Experimental
trait LookupCatalog {

  val catalogManager: CatalogManager

  /**
   * Extract catalog plugin and identifier from a multi-part identifier.
   */
  object CatalogObjectIdentifier {
    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = {
      assert(parts.nonEmpty)
      if (parts.length == 1) {
        catalogManager.getDefaultCatalog().map { catalog =>
          (catalog, Identifier.of(Array.empty, parts.last))
        }
      } else {
        try {
          val catalog = catalogManager.getCatalog(parts.head)
          Some((catalog, Identifier.of(parts.tail.init.toArray, parts.last)))
        } catch {
          case _: CatalogNotFoundException =>
            catalogManager.getDefaultCatalog().map { catalog =>
              (catalog, Identifier.of(parts.init.toArray, parts.last))
            }
        }
      }
    }
  }

  /**
   * Extract legacy table identifier from a multi-part identifier.
   *
   * For legacy support only. Please use [[CatalogObjectIdentifier]] instead on DSv2 code paths.
   */
  object AsTableIdentifier {
    def unapply(parts: Seq[String]): Option[TableIdentifier] = parts match {
      case CatalogObjectIdentifier(_, _) =>
        throw new IllegalStateException(parts.mkString(".") + " is not a TableIdentifier.")
      case Seq(tblName) => Some(TableIdentifier(tblName))
      case Seq(dbName, tblName) => Some(TableIdentifier(tblName, Some(dbName)))
      case _ => None
    }
  }
}
