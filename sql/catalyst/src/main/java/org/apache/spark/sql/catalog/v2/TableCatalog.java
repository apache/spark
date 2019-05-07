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

package org.apache.spark.sql.catalog.v2;

import org.apache.spark.sql.catalog.v2.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Catalog methods for working with Tables.
 * <p>
 * TableCatalog implementations may be case sensitive or case insensitive. Spark will pass
 * {@link Identifier table identifiers} without modification. Field names passed to
 * {@link #alterTable(Identifier, TableChange...)} will be normalized to match the case used in the
 * table schema when updating, renaming, or dropping existing columns when catalyst analysis is case
 * insensitive.
 */
public interface TableCatalog extends CatalogPlugin {
  /**
   * List the tables in a namespace from the catalog.
   * <p>
   * If the catalog supports views, this must return identifiers for only tables and not views.
   *
   * @param namespace a multi-part namespace
   * @return an array of Identifiers for tables
   * @throws NoSuchNamespaceException If the namespace does not exist (optional).
   */
  Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Load table metadata by {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist or is a view
   */
  Table loadTable(Identifier ident) throws NoSuchTableException;

  /**
   * Invalidate cached table metadata for an {@link Identifier identifier}.
   * <p>
   * If the table is already loaded or cached, drop cached data. If the table does not exist or is
   * not cached, do nothing. Calling this method should not query remote services.
   *
   * @param ident a table identifier
   */
  default void invalidateTable(Identifier ident) {
  }

  /**
   * Test whether a table exists using an {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must return false.
   *
   * @param ident a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(Identifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param schema the schema of the new table, as a struct type
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Apply a set of {@link TableChange changes} to a table in the catalog.
   * <p>
   * Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the table.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @param changes changes to apply to the table
   * @return updated metadata for the table
   * @throws NoSuchTableException If the table doesn't exist or is a view
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException;

  /**
   * Drop a table in the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must not drop the view and must return false.
   *
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   */
  boolean dropTable(Identifier ident);
}
