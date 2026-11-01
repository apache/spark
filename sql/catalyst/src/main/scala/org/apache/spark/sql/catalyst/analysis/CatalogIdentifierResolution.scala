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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, LookupCatalog}

/**
 * Resolves command target names to a [[ResolvedIdentifier]] for [[ResolveCatalogs]].
 */
class CatalogIdentifierResolution(override val catalogManager: CatalogManager)
    extends LookupCatalog {

  /** A temporary view resolves to [[FakeSystemCatalog]] when `allowTemp` is set. */
  def resolveIdentifier(
      nameParts: Seq[String],
      allowTemp: Boolean,
      columnOutput: Seq[Attribute]): ResolvedIdentifier = {
    if (allowTemp && catalogManager.v1SessionCatalog.isTempView(nameParts)) {
      val identifier = Identifier.of(nameParts.dropRight(1).toArray, nameParts.last)
      ResolvedIdentifier(FakeSystemCatalog, identifier, columnOutput)
    } else {
      val CatalogAndIdentifier(catalog, identifier) = nameParts
      ResolvedIdentifier(catalog, identifier, columnOutput)
    }
  }
}
