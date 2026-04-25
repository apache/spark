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
 * Catalog API for read and write access to views.
 * <p>
 * A connector that wants to expose views implements this interface. The interface is independent
 * from {@link TableCatalog}: a connector can implement just {@code ViewCatalog} (a view-only
 * catalog), just {@code TableCatalog} (a table-only catalog), or both. There is no capability
 * flag to declare; the presence of {@code ViewCatalog} on the catalog plugin <i>is</i> the
 * signal that it supports views.
 *
 * <h3>Mixed catalogs (implementing both {@code TableCatalog} and {@code ViewCatalog})</h3>
 *
 * The two interfaces are independent: every {@code TableCatalog} method behaves as if views did
 * not exist, and every {@code ViewCatalog} method behaves as if tables did not exist. The only
 * cross-cutting invariant is that <b>tables and views share a single identifier namespace</b> in
 * the catalog: the same identifier cannot resolve to both a table and a view at the same time.
 * That invariant manifests in two places:
 * <ul>
 *   <li>{@link TableCatalog#createTable} must reject (with
 *       {@link org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException}) if the
 *       identifier already names a view.</li>
 *   <li>{@link #createView} must reject (with {@link ViewAlreadyExistsException}) if the
 *       identifier already names a table.</li>
 * </ul>
 *
 * <h3>Resolution and the optional perf opt-in for mixed catalogs</h3>
 *
 * Spark resolves an identifier by calling {@link TableCatalog#loadTable} first; on
 * {@link org.apache.spark.sql.catalyst.analysis.NoSuchTableException} it falls back to
 * {@link #loadView} when the catalog also implements {@code ViewCatalog}. That fallback costs an
 * extra RPC per cold-cache view lookup. To skip it, a perf-conscious mixed catalog may return a
 * {@link MetadataOnlyTable} wrapping the {@link ViewInfo} from
 * {@link TableCatalog#loadTable} when the identifier resolves to a view; Spark recognizes the
 * {@code ViewInfo} payload and routes through view resolution without a follow-up
 * {@code loadView} call. {@code loadView} is still used directly for view DDL paths
 * (DROP VIEW, ALTER VIEW, SHOW CREATE TABLE, etc.).
 *
 * @since 4.2.0
 */
@Evolving
public interface ViewCatalog extends CatalogPlugin {

  /**
   * List the views in a namespace from the catalog.
   * <p>
   * For mixed catalogs, this must return identifiers for views only (tables are listed via
   * {@link TableCatalog#listTables}).
   *
   * @param namespace a multi-part namespace
   * @return an array of identifiers for views
   * @throws NoSuchNamespaceException if the namespace does not exist (optional)
   */
  Identifier[] listViews(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Load view metadata by identifier.
   * <p>
   * For mixed catalogs, throws {@link NoSuchViewException} when {@code ident} resolves to a
   * table rather than a view.
   *
   * @param ident a view identifier
   * @return the view metadata
   * @throws NoSuchViewException if the view does not exist (or {@code ident} is a table in a
   *                             mixed catalog)
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
   * <p>
   * In mixed catalogs, must throw {@link ViewAlreadyExistsException} if {@code ident} already
   * names a table or a view.
   *
   * @param ident the view identifier
   * @param info  the view metadata
   * @return the metadata of the newly created view; may equal {@code info}
   * @throws ViewAlreadyExistsException if a view or table already exists at {@code ident}
   * @throws NoSuchNamespaceException   if the identifier's namespace does not exist (optional)
   */
  ViewInfo createView(Identifier ident, ViewInfo info)
      throws ViewAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Atomically replace an existing view's metadata.
   * <p>
   * Used by {@code ALTER VIEW ... AS} and as the replace branch of {@code CREATE OR REPLACE
   * VIEW}. Implementations should commit the new metadata atomically; views carry no data, so a
   * single transactional metastore call (or equivalent) is sufficient -- there is no separate
   * staging API.
   *
   * @param ident the view identifier
   * @param info  the new view metadata
   * @return the metadata of the replaced view; may equal {@code info}
   * @throws NoSuchViewException if no view exists at {@code ident} (or {@code ident} is a table
   *                             in a mixed catalog)
   */
  ViewInfo replaceView(Identifier ident, ViewInfo info) throws NoSuchViewException;

  /**
   * Drop a view.
   * <p>
   * Returns {@code true} if a view was dropped at {@code ident}, {@code false} otherwise. In
   * mixed catalogs, returns {@code false} if {@code ident} is a table (the table is not
   * touched). Spark's resolver guards the call site so that {@code DROP VIEW} on a table or
   * {@code DROP TABLE} on a view surfaces the dedicated {@code EXPECT_VIEW_NOT_TABLE} /
   * {@code EXPECT_TABLE_NOT_VIEW} error before this method is invoked.
   *
   * @param ident a view identifier
   * @return true if a view was dropped, false otherwise
   */
  boolean dropView(Identifier ident);
}
