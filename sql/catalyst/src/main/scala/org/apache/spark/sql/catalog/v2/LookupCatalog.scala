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

/**
 * A trait to encapsulate catalog lookup function and helpful extractors.
 */
@Experimental
trait LookupCatalog {

  def lookupCatalog: Option[(String) => CatalogPlugin] = None

  type CatalogObjectIdentifier = (Option[CatalogPlugin], Identifier)

  /**
   * Extract catalog plugin and identifier from a multi-part identifier.
   */
  object CatalogObjectIdentifier {
    def unapply(parts: Seq[String]): Option[CatalogObjectIdentifier] = lookupCatalog.map { lookup =>
      parts match {
        case Seq(name) =>
          (None, Identifier.of(Array.empty, name))
        case Seq(catalogName, tail @ _*) =>
          try {
            val catalog = lookup(catalogName)
            (Some(catalog), Identifier.of(tail.init.toArray, tail.last))
          } catch {
            case _: CatalogNotFoundException =>
              (None, Identifier.of(parts.init.toArray, parts.last))
          }
      }
    }
  }

  /**
   * Extract legacy table identifier from a multi-part identifier.
   *
   * For legacy support only. Please use
   * [[org.apache.spark.sql.catalog.v2.LookupCatalog.CatalogObjectIdentifier]] in DSv2 code paths.
   */
  object AsTableIdentifier {
    def unapply(parts: Seq[String]): Option[TableIdentifier] = parts match {
      case CatalogObjectIdentifier(None, ident) =>
        ident.namespace match {
          case Array() =>
            Some(TableIdentifier(ident.name))
          case Array(database) =>
            Some(TableIdentifier(ident.name, Some(database)))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }
}
