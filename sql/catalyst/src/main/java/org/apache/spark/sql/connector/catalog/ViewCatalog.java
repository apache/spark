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

import java.util.Map;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.types.StructType;

/**
 * Catalog methods for working with views.
 */
@Experimental
public interface ViewCatalog extends CatalogPlugin {

  /**
   * List the views in a namespace from the catalog.
   * <p>
   * If the catalog supports tables, this must return identifiers for only views and not tables.
   *
   * @param namespace a multi-part namespace
   * @return an array of Identifiers for views
   * @throws NoSuchNamespaceException If the namespace does not exist (optional).
   */
  default Identifier[] listViews(String... namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  /**
   * Load view description by {@link Identifier ident} from the catalog.
   * <p>
   * If the catalog supports tables and contains a table for the identifier and not a view, this
   * must throw {@link NoSuchViewException}.
   *
   * @param ident the view identifier
   * @return the view description
   * @throws NoSuchViewException If the view doesn't exist or is a table
   */
  View loadView(Identifier ident) throws NoSuchViewException;

  /**
   * Test whether a view exists using an {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must return false.
   *
   * @param ident the view identifier
   * @return true if the view exists, false otherwise
   */
  default boolean viewExists(Identifier ident) {
    try {
      return loadView(ident) != null;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  /**
   * Create a view in the catalog.
   *
   * @param ident the view identifier
   * @param sql SQL text that defines the view
   * @param schema The view query output schema
   * @param catalogAndNamespace The current catalog and namespace
   * @param properties The view properties
   * @throws ViewAlreadyExistsException If a view or table already exists for the identifier
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  void createView(
      Identifier ident,
      String sql,
      StructType schema,
      String[] catalogAndNamespace,
      Map<String, String> properties) throws ViewAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Replace a view in the catalog.
   * <p>
   * The default implementation has a race condition.
   * Catalogs are encouraged to implement this operation atomically.
   *
   * @param ident the view identifier
   * @param sql SQL text that defines the view
   * @param schema The view query output schema
   * @param catalogAndNamespace The current catalog and namespace
   * @param properties The view properties
   * @throws NoSuchViewException If the view doesn't exist or is a table
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  default void replaceView(
      Identifier ident,
      String sql,
      StructType schema,
      String[] catalogAndNamespace,
      Map<String, String> properties) throws NoSuchViewException, NoSuchNamespaceException {
    if (!viewExists(ident)) {
      throw new NoSuchViewException(ident);
    }
    dropView(ident);
    try {
      createView(ident, sql, schema, catalogAndNamespace, properties);
    } catch (ViewAlreadyExistsException e) {
      throw new RuntimeException("Race condition in drop and create", e);
    }
  }

  /**
   * Create or replace a view in the catalog.
   * <p>
   * The default implementation has race conditions.
   * Catalogs are encouraged to implement this operation atomically.
   *
   * @param ident the view identifier
   * @param sql SQL text that defines the view
   * @param schema The view query output schema
   * @param catalogAndNamespace The current catalog and namespace
   * @param properties The view properties
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  default void createOrReplaceView(
      Identifier ident,
      String sql,
      StructType schema,
      String[] catalogAndNamespace,
      Map<String, String> properties) throws NoSuchNamespaceException {
    if (!viewExists(ident)) {
      try {
        createView(ident, sql, schema, catalogAndNamespace, properties);
      } catch (ViewAlreadyExistsException e) {
        throw new RuntimeException("Race condition in check and create", e);
      }
    } else {
      try {
        replaceView(ident, sql, schema, catalogAndNamespace, properties);
      } catch (NoSuchViewException e) {
        throw new RuntimeException("Race condition in check and replace", e);
      }
    }
  }

  /**
   * Apply {@link ViewChange changes} to a view in the catalog.
   * <p>
   * Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the view.
   *
   * @param ident a view identifier
   * @param changes an array of changes to apply to the view
   * @throws NoSuchViewException If the view doesn't exist.
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  void alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException;

  /**
   * Drop a view in the catalog.
   * <p>
   * If the catalog supports tables and contains a table for the identifier and not a view, this
   * must not drop the table and must return false.
   *
   * @param ident the view identifier
   * @return true if a view was deleted, false if no view exists for the identifier
   */
  boolean dropView(Identifier ident);

  /**
   * Rename a view in the catalog.
   * <p>
   * If the catalog supports tables and contains a table with the old identifier, this throws
   * {@link NoSuchViewException}. Additionally, if it contains a table with the new identifier,
   * this throws {@link ViewAlreadyExistsException}.
   * <p>
   * If the catalog does not support view renames between namespaces, it throws
   * {@link UnsupportedOperationException}.
   *
   * @param oldIdent the view identifier of the existing view to rename
   * @param newIdent the new view identifier of the view
   * @throws NoSuchViewException If the view to rename doesn't exist or is a table
   * @throws ViewAlreadyExistsException If the new view name already exists or is a table
   * @throws UnsupportedOperationException If the namespaces of old and new identifiers do not
   * match (optional)
   */
  void renameView(Identifier oldIdent, Identifier newIdent)
      throws NoSuchViewException, ViewAlreadyExistsException;
}
