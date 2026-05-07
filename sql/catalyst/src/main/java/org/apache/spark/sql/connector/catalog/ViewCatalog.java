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
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;

/**
 * Catalog API for connectors that expose views.
 * <p>
 * Connectors that expose <i>only</i> views implement this interface. Connectors that expose
 * both tables and views must implement {@link TableViewCatalog} (which extends both this
 * interface and {@link TableCatalog} and adds the cross-cutting contract for the combined
 * case); the methods on this interface remain view-only -- they do not interact with tables.
 * <p>
 * The presence of {@code ViewCatalog} on the catalog plugin <i>is</i> the signal that it
 * supports views; there is no capability flag to declare.
 *
 * @since 4.2.0
 */
@Evolving
public interface ViewCatalog extends CatalogPlugin {

  /**
   * List the views in a namespace from the catalog.
   *
   * @param namespace a multi-part namespace
   * @return an array of identifiers for views
   * @throws NoSuchNamespaceException if the namespace does not exist (optional)
   */
  Identifier[] listViews(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Load view metadata by identifier.
   *
   * @param ident a view identifier
   * @return the view metadata
   * @throws NoSuchViewException if the view does not exist
   */
  ViewInfo loadView(Identifier ident) throws NoSuchViewException;

  /**
   * Test whether a view exists.
   * <p>
   * The default implementation calls {@link #loadView} and catches {@link NoSuchViewException}.
   * Catalogs that can answer existence cheaply should override.
   *
   * @param ident a view identifier
   * @return true if a view exists at {@code ident}, false otherwise
   */
  default boolean viewExists(Identifier ident) {
    try {
      loadView(ident);
      return true;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  /**
   * Invalidate cached metadata for a view.
   * <p>
   * If the view is currently cached, drop the cached entry; otherwise do nothing. This must not
   * issue remote calls.
   *
   * @param ident a view identifier
   */
  default void invalidateView(Identifier ident) {
  }

  /**
   * Create a view.
   *
   * @param ident the view identifier
   * @param info  the view metadata
   * @return the metadata of the newly created view; may equal {@code info}
   * @throws ViewAlreadyExistsException if a view already exists at {@code ident}
   * @throws NoSuchNamespaceException   if the identifier's namespace does not exist (optional)
   */
  ViewInfo createView(Identifier ident, ViewInfo info)
      throws ViewAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Atomically replace an existing view's metadata.
   * <p>
   * Used by {@code ALTER VIEW ... AS}. Implementations should commit the new metadata
   * atomically; views carry no data, so a single transactional metastore call (or equivalent)
   * is sufficient -- there is no separate staging API.
   *
   * @param ident the view identifier
   * @param info  the new view metadata
   * @return the metadata of the replaced view; may equal {@code info}
   * @throws NoSuchViewException if no view exists at {@code ident}
   */
  ViewInfo replaceView(Identifier ident, ViewInfo info) throws NoSuchViewException;

  /**
   * Create a view if one does not exist at {@code ident}, or atomically replace it if one does.
   * <p>
   * Used by {@code CREATE OR REPLACE VIEW}. The default implementation calls
   * {@link #replaceView}, falling back to {@link #createView} on
   * {@link NoSuchViewException}. The fallback is non-atomic across the two calls (a concurrent
   * drop or create can race), so catalogs that can answer the upsert in a single transactional
   * call should override this method to collapse to one RPC and to make the swap atomic.
   *
   * @param ident the view identifier
   * @param info  the view metadata
   * @return the metadata of the created or replaced view; may equal {@code info}
   * @throws ViewAlreadyExistsException if {@code ident} cannot host this view -- either a
   *                                    concurrent {@code CREATE VIEW} won the race in the
   *                                    default impl's gap between {@link #replaceView} and
   *                                    the fallback {@link #createView}, or, in a
   *                                    {@link TableViewCatalog}, a table sits at {@code ident}
   * @throws NoSuchNamespaceException   if the identifier's namespace does not exist (optional)
   */
  default ViewInfo createOrReplaceView(Identifier ident, ViewInfo info)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    try {
      return replaceView(ident, info);
    } catch (NoSuchViewException e) {
      return createView(ident, info);
    }
  }

  /**
   * Drop a view.
   *
   * @param ident a view identifier
   * @return true if a view was dropped, false otherwise
   */
  boolean dropView(Identifier ident);

  /**
   * Rename a view.
   * <p>
   * If the catalog supports tables and contains a table at the new identifier, this must throw
   * {@link ViewAlreadyExistsException}. If the source identifier resolves to a table rather than
   * a view, this must throw {@link NoSuchViewException}. The cross-type contract for catalogs
   * that expose both tables and views lives on {@link TableViewCatalog}.
   *
   * @param oldIdent the view identifier of the existing view to rename
   * @param newIdent the new view identifier
   * @throws NoSuchViewException        if no view exists at {@code oldIdent}
   * @throws ViewAlreadyExistsException if a view (or, in a {@link TableViewCatalog}, a table)
   *                                    already exists at {@code newIdent}
   */
  void renameView(Identifier oldIdent, Identifier newIdent)
      throws NoSuchViewException, ViewAlreadyExistsException;
}
