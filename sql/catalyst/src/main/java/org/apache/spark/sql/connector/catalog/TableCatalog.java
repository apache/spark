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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
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
@Experimental
public interface TableCatalog extends CatalogPlugin, SupportCreateTable {
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
   * Renames a table in the catalog.
   * <p>
   * If the catalog supports views and contains a view for the old identifier and not a table, this
   * throws {@link NoSuchTableException}. Additionally, if the new identifier is a table or a view,
   * this throws {@link TableAlreadyExistsException}.
   * <p>
   * If the catalog does not support table renames between namespaces, it throws
   * {@link UnsupportedOperationException}.
   *
   * @param oldIdent the table identifier of the existing table to rename
   * @param newIdent the new table identifier of the table
   * @throws NoSuchTableException If the table to rename doesn't exist or is a view
   * @throws TableAlreadyExistsException If the new table name already exists or is a view
   * @throws UnsupportedOperationException If the namespaces of old and new identiers do not
   *                                       match (optional)
   */
  void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException;
}
