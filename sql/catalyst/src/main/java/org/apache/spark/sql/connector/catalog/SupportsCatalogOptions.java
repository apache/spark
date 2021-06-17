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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * An interface, which TableProviders can implement, to support table existence checks and creation
 * through a catalog, without having to use table identifiers. For example, when file based data
 * sources use the `DataFrameWriter.save(path)` method, the option `path` can translate to a
 * PathIdentifier. A catalog can then use this PathIdentifier to check the existence of a table, or
 * whether a table can be created at a given directory.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsCatalogOptions extends TableProvider {
  /**
   * Return a {@link Identifier} instance that can identify a table for a DataSource given
   * DataFrame[Reader|Writer] options.
   *
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   */
  Identifier extractIdentifier(CaseInsensitiveStringMap options);

  /**
   * Return the name of a catalog that can be used to check the existence of, load, and create
   * a table for this DataSource given the identifier that will be extracted by
   * {@link #extractIdentifier(CaseInsensitiveStringMap) extractIdentifier}. A `null` value can
   * be used to defer to the V2SessionCatalog.
   *
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   */
  default String extractCatalog(CaseInsensitiveStringMap options) {
    return CatalogManager.SESSION_CATALOG_NAME();
  }
}
