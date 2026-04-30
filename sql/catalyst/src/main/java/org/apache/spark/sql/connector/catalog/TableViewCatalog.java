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

import java.util.ArrayList;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;

/**
 * Catalog API for connectors that expose both tables and views in a single shared identifier
 * namespace.
 * <p>
 * Connectors that expose <i>both</i> tables and views must implement {@code TableViewCatalog};
 * implementing {@link TableCatalog} and {@link ViewCatalog} directly without
 * {@code TableViewCatalog} is rejected at catalog initialization. Connectors that expose only
 * tables implement just {@link TableCatalog}; connectors that expose only views implement just
 * {@link ViewCatalog}; this interface is not relevant to them.
 *
 * <h2>Two principles</h2>
 *
 * A {@code TableViewCatalog} follows two rules that, taken together, define every cross-cutting
 * subtlety:
 * <ol>
 *   <li><b>Orthogonal interfaces.</b> Every {@link TableCatalog} method behaves as if views did
 *       not exist, and every {@link ViewCatalog} method behaves as if tables did not exist.
 *       From the perspective of a {@code TableCatalog} caller, a view at an identifier is
 *       indistinguishable from "nothing there"; symmetrically for {@code ViewCatalog} on
 *       tables. The implementation, of course, knows about both kinds -- it just filters them
 *       apart at each method boundary.</li>
 *   <li><b>Single identifier namespace.</b> Tables and views share one keyspace within a
 *       namespace; the same {@link Identifier} cannot resolve to both at the same time. The
 *       implementation typically enforces this with a single backing keyspace plus a kind
 *       discriminator.</li>
 * </ol>
 *
 * <h2>Per-method cross-type behavior</h2>
 *
 * <b>Active rejection</b> (write-side methods that throw on cross-type collision):
 * <table>
 *   <caption>Cross-type rejection</caption>
 *   <tr><th>Method</th><th>Rejects when</th><th>Throws</th></tr>
 *   <tr><td>{@link TableCatalog#createTable}</td><td>a view sits at {@code ident}</td>
 *       <td>{@link org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException}</td></tr>
 *   <tr><td>{@link TableCatalog#renameTable}</td>
 *       <td>a view sits at {@code newIdent}</td>
 *       <td>{@link org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException}</td></tr>
 *   <tr><td>{@link ViewCatalog#createView}</td><td>a table sits at {@code ident}</td>
 *       <td>{@link org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException}</td></tr>
 *   <tr><td>{@link ViewCatalog#createOrReplaceView}</td><td>a table sits at {@code ident}</td>
 *       <td>{@link org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException}</td></tr>
 *   <tr><td>{@link ViewCatalog#replaceView}</td><td>a table sits at {@code ident}</td>
 *       <td>{@link org.apache.spark.sql.catalyst.analysis.NoSuchViewException}</td></tr>
 *   <tr><td>{@link ViewCatalog#renameView}</td>
 *       <td>a table sits at {@code newIdent}</td>
 *       <td>{@link org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException}</td></tr>
 * </table>
 *
 * <b>Passive filtering</b> (read / non-collision mutation methods that behave as if the wrong
 * kind doesn't exist):
 * <table>
 *   <caption>Cross-type filtering</caption>
 *   <tr><th>Method</th><th>On wrong-kind ident</th></tr>
 *   <tr><td>{@link TableCatalog#loadTable(Identifier)}</td>
 *       <td>throws {@code NoSuchTableException} for a view</td></tr>
 *   <tr><td>{@link TableCatalog#loadTable(Identifier, String)} /
 *       {@link TableCatalog#loadTable(Identifier, long)}</td>
 *       <td>throws {@code NoSuchTableException} for a view (no perf opt-in -- time-travel does
 *       not apply to views)</td></tr>
 *   <tr><td>{@link TableCatalog#tableExists}</td><td>returns {@code false} for a view</td></tr>
 *   <tr><td>{@link TableCatalog#dropTable} / {@link TableCatalog#purgeTable}</td>
 *       <td>returns {@code false} for a view; does not drop it</td></tr>
 *   <tr><td>{@link TableCatalog#renameTable}</td>
 *       <td>throws {@code NoSuchTableException} when the source is a view</td></tr>
 *   <tr><td>{@link TableCatalog#listTables}</td><td>tables only</td></tr>
 *   <tr><td>{@link ViewCatalog#loadView}</td>
 *       <td>throws {@code NoSuchViewException} for a table</td></tr>
 *   <tr><td>{@link ViewCatalog#viewExists}</td><td>returns {@code false} for a table</td></tr>
 *   <tr><td>{@link ViewCatalog#dropView}</td>
 *       <td>returns {@code false} for a table; does not drop it</td></tr>
 *   <tr><td>{@link ViewCatalog#renameView}</td>
 *       <td>throws {@code NoSuchViewException} when the source is a table</td></tr>
 *   <tr><td>{@link ViewCatalog#listViews}</td><td>views only</td></tr>
 * </table>
 *
 * <h2>Single-RPC perf entry points</h2>
 *
 * The orthogonal {@link TableCatalog} and {@link ViewCatalog} answer two cross-cutting
 * questions in two round trips each. {@code TableViewCatalog} adds dedicated methods so a
 * catalog can answer both in one round trip:
 * <ul>
 *   <li>{@link #loadTableOrView(Identifier)} -- the resolver's per-identifier read path. Returns
 *       a regular {@link Table} for a table, or a {@link MetadataTable} wrapping a
 *       {@link ViewInfo} for a view. Saves the {@code loadTable} -> {@code loadView} fallback
 *       on a cold cache.</li>
 *   <li>{@link #listRelationSummaries(String[])} -- a unified listing of tables and views with the
 *       kind preserved on each {@link TableSummary}. Default impl performs both
 *       {@link TableCatalog#listTableSummaries} and {@link ViewCatalog#listViews}; override to
 *       fetch in one round trip.</li>
 * </ul>
 *
 * @since 4.2.0
 */
@Evolving
public interface TableViewCatalog extends TableCatalog, ViewCatalog {

  /**
   * Load metadata for an identifier that may resolve to either a table or a view.
   * <p>
   * For a table, returns the table's {@link Table}. For a view, returns a
   * {@link MetadataTable} wrapping a {@link ViewInfo}; callers discriminate via
   * {@code getTableInfo() instanceof ViewInfo}. This lets the resolver answer in a single RPC
   * instead of falling back from {@link TableCatalog#loadTable} to {@link ViewCatalog#loadView}.
   *
   * @param ident the identifier
   * @return a {@link Table} for tables, or a {@link MetadataTable} wrapping a
   *         {@link ViewInfo} for views
   * @throws NoSuchTableException if neither a table nor a view exists at {@code ident}
   */
  Table loadTableOrView(Identifier ident) throws NoSuchTableException;

  /**
   * List the tables and views in a namespace, returned as {@link TableSummary} entries with
   * the kind preserved on each summary.
   * <p>
   * The default implementation enumerates via {@link TableCatalog#listTableSummaries} for
   * tables and {@link ViewCatalog#listViews} for views (two round trips). Catalogs that can
   * fetch the unified listing in a single round trip should override.
   *
   * @param namespace a multi-part namespace
   * @return an array of summaries for both tables and views in the namespace
   * @throws NoSuchNamespaceException if the namespace does not exist (optional)
   * @throws NoSuchTableException if a table listed by the underlying enumeration disappears
   *                              before its summary can be assembled (default impl only)
   */
  default TableSummary[] listRelationSummaries(String[] namespace)
      throws NoSuchNamespaceException, NoSuchTableException {
    TableSummary[] tableSummaries = listTableSummaries(namespace);
    Identifier[] viewIdentifiers = listViews(namespace);
    ArrayList<TableSummary> all = new ArrayList<>(
        tableSummaries.length + viewIdentifiers.length);
    for (TableSummary s : tableSummaries) {
      all.add(s);
    }
    for (Identifier id : viewIdentifiers) {
      all.add(TableSummary.of(id, TableSummary.VIEW_TABLE_TYPE));
    }
    return all.toArray(TableSummary[]::new);
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default implementation derives from {@link #loadTableOrView}: a {@link MetadataTable}
   * wrapping a {@link ViewInfo} is rejected as not-a-table; anything else is returned. Override
   * only if a tables-only path is materially cheaper than the unified one.
   */
  @Override
  default Table loadTable(Identifier ident) throws NoSuchTableException {
    Table t = loadTableOrView(ident);
    if (t instanceof MetadataTable mot && mot.getTableInfo() instanceof ViewInfo) {
      throw new NoSuchTableException(ident);
    }
    return t;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default implementation derives from {@link #loadTableOrView}: a {@link MetadataTable}
   * wrapping a {@link ViewInfo} is unwrapped and returned; anything else (table or absent) is
   * surfaced as {@link NoSuchViewException}. Override only if a views-only path is materially
   * cheaper than the unified one.
   */
  @Override
  default ViewInfo loadView(Identifier ident) throws NoSuchViewException {
    Table t;
    try {
      t = loadTableOrView(ident);
    } catch (NoSuchTableException e) {
      throw new NoSuchViewException(ident);
    }
    if (t instanceof MetadataTable mot && mot.getTableInfo() instanceof ViewInfo vi) {
      return vi;
    }
    throw new NoSuchViewException(ident);
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default implementation derives from {@link #loadTableOrView}: returns {@code true} only if
   * the entry exists and is not a view. Override only if a cheaper existence-check path exists.
   */
  @Override
  default boolean tableExists(Identifier ident) {
    try {
      Table t = loadTableOrView(ident);
      return !(t instanceof MetadataTable mot && mot.getTableInfo() instanceof ViewInfo);
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default implementation derives from {@link #loadTableOrView}: returns {@code true} only if
   * the entry exists and is a view. Override only if a cheaper existence-check path exists.
   */
  @Override
  default boolean viewExists(Identifier ident) {
    try {
      Table t = loadTableOrView(ident);
      return t instanceof MetadataTable mot && mot.getTableInfo() instanceof ViewInfo;
    } catch (NoSuchTableException e) {
      return false;
    }
  }
}
