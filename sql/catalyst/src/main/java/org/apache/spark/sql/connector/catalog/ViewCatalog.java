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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.types.StructType;

/**
 * Catalog methods for working with views.
 */
@DeveloperApi
public interface ViewCatalog extends CatalogPlugin {

  /**
   * A reserved property to specify the description of the view.
   */
  String PROP_COMMENT = "comment";

  /**
   * A reserved property to specify the owner of the view.
   */
  String PROP_OWNER = "owner";

  /**
   * A reserved property to specify the software version used to create the view.
   */
  String PROP_CREATE_ENGINE_VERSION = "create_engine_version";

  /**
   * A reserved property to specify the software version used to change the view.
   */
  String PROP_ENGINE_VERSION = "engine_version";

  /**
   * All reserved properties of the view.
   */
  List<String> RESERVED_PROPERTIES = Arrays.asList(
        PROP_COMMENT,
        PROP_OWNER,
        PROP_CREATE_ENGINE_VERSION,
        PROP_ENGINE_VERSION);

  /**
   * List the views in a namespace from the catalog.
   * <p>
   * If the catalog supports tables, this must return identifiers for only views and not tables.
   *
   * @param namespace a multi-part namespace
   * @return an array of Identifiers for views
   * @throws NoSuchNamespaceException If the namespace does not exist (optional).
   */
  Identifier[] listViews(String... namespace) throws NoSuchNamespaceException;

  /**
   * Load view metadata by {@link Identifier ident} from the catalog.
   * <p>
   * If the catalog supports tables and contains a table for the identifier and not a view,
   * this must throw {@link NoSuchViewException}.
   *
   * @param ident a view identifier
   * @return the view description
   * @throws NoSuchViewException If the view doesn't exist or is a table
   */
  View loadView(Identifier ident) throws NoSuchViewException;

  /**
   * Invalidate cached view metadata for an {@link Identifier identifier}.
   * <p>
   * If the view is already loaded or cached, drop cached data. If the view does not exist or is
   * not cached, do nothing. Calling this method should not query remote services.
   *
   * @param ident a view identifier
   */
  default void invalidateView(Identifier ident) {
  }

  /**
   * Test whether a view exists using an {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table,
   * this must return false.
   *
   * @param ident a view identifier
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
   * @param ident a view identifier
   * @param sql the SQL text that defines the view
   * @param currentCatalog the current catalog
   * @param currentNamespace the current namespace
   * @param schema the view query output schema
   * @param queryColumnNames the query column names
   * @param columnAliases the column aliases
   * @param columnComments the column comments
   * @param properties the view properties
   * @return the view created
   * @throws ViewAlreadyExistsException If a view or table already exists for the identifier
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  View createView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties) throws ViewAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Apply {@link ViewChange changes} to a view in the catalog.
   * <p>
   * Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the view.
   *
   * @param ident a view identifier
   * @param changes an array of changes to apply to the view
   * @return the view altered
   * @throws NoSuchViewException If the view doesn't exist or is a table.
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException;

  /**
   * Drop a view in the catalog.
   * <p>
   * If the catalog supports tables and contains a table for the identifier and not a view, this
   * must not drop the table and must return false.
   *
   * @param ident a view identifier
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
