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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * A view in a catalog -- the typed payload returned by {@link ViewCatalog#loadView} and accepted
 * by {@link ViewCatalog#createView} / {@link ViewCatalog#replaceView}. A {@code View} carries the
 * view-specific fields that cannot be represented as string table properties: the query text,
 * captured creation-time resolution context, captured SQL configs, schema-binding mode, and
 * query output column names. Columns and user TBLPROPERTIES are set via the typed builder.
 * <p>
 * Unlike a {@link Table}, a {@code View} is itself a {@link Relation} rather than something Spark
 * realizes into a {@code Table}: Spark expands the view's query text at read time and never builds
 * a view object. A {@link RelationCatalog} returns a {@code View} directly from
 * {@link RelationCatalog#loadRelation} for a view identifier, so it never has to smuggle a view
 * through the {@code Table} surface.
 *
 * @since 4.2.0
 */
@Evolving
public class View implements Relation {

  private final Column[] columns;
  private final Map<String, String> properties;
  private final String queryText;
  private final String currentCatalog;
  private final String[] currentNamespace;
  private final Map<String, String> sqlConfigs;
  private final String schemaMode;
  private final String[] queryColumnNames;
  private final DependencyList viewDependencies;

  protected View(Builder builder) {
    this.columns = builder.columns;
    this.properties = builder.properties;
    this.queryText = Objects.requireNonNull(builder.queryText, "queryText should not be null");
    this.currentCatalog = builder.currentCatalog;
    this.currentNamespace = builder.currentNamespace;
    this.sqlConfigs = Collections.unmodifiableMap(builder.sqlConfigs);
    this.schemaMode = builder.schemaMode;
    this.queryColumnNames = builder.queryColumnNames;
    this.viewDependencies = builder.viewDependencies;
    // Default PROP_TABLE_TYPE = VIEW so `properties()` reflects the typed View classification.
    // Callers can refine to a more specific view kind (for example, METRIC_VIEW) by calling
    // RelationBuilder.withTableType(...) on the builder before build().
    properties.putIfAbsent(TableCatalog.PROP_TABLE_TYPE, TableSummary.VIEW_TABLE_TYPE);
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  public StructType schema() {
    return CatalogV2Util.v2ColumnsToStructType(columns);
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** The SQL text of the view. */
  public String queryText() { return queryText; }

  /**
   * The current catalog at the time the view was created, used to resolve unqualified
   * identifiers in {@link #queryText()} at read time. May be {@code null} if the view was
   * created with no captured resolution context.
   */
  public String currentCatalog() { return currentCatalog; }

  /**
   * The current namespace at the time the view was created, used alongside
   * {@link #currentCatalog()} to resolve unqualified identifiers in {@link #queryText()} at
   * read time. Never {@code null}; empty when no namespace was captured.
   */
  public String[] currentNamespace() { return currentNamespace; }

  /**
   * The SQL configs captured at view creation time, applied when parsing and analyzing the
   * view body. Keys are unprefixed SQL config names (e.g. {@code spark.sql.ansi.enabled}).
   */
  public Map<String, String> sqlConfigs() { return sqlConfigs; }

  /**
   * The view's schema binding mode. Allowed values match the {@code toString} form of
   * {@code org.apache.spark.sql.catalyst.analysis.ViewSchemaMode}:
   * {@code BINDING}, {@code COMPENSATION}, {@code TYPE EVOLUTION}, {@code EVOLUTION}.
   * May be {@code null} when schema binding is not configured.
   */
  public String schemaMode() { return schemaMode; }

  /**
   * Output column names of the query that created the view, used to map the query output to
   * the view's declared columns during view resolution. Empty for views in {@code EVOLUTION}
   * mode, which always use the view's current schema.
   */
  public String[] queryColumnNames() { return queryColumnNames; }

  /**
   * Returns the structured list of objects this view depends on (source tables and functions),
   * or {@code null} if no dependency list was supplied. Unlike other view metadata which is
   * encoded into {@link #properties()}, dependency lists are a first-class field because their
   * nested structure does not round-trip cleanly through flat string properties.
   */
  public DependencyList viewDependencies() { return viewDependencies; }

  public static class Builder extends RelationBuilder<Builder> {
    private String queryText;
    private String currentCatalog;
    private String[] currentNamespace = new String[0];
    private Map<String, String> sqlConfigs = new HashMap<>();
    private String schemaMode;
    private String[] queryColumnNames = new String[0];
    private DependencyList viewDependencies = null;

    @Override
    protected Builder self() { return this; }

    public Builder withQueryText(String queryText) {
      this.queryText = queryText;
      return this;
    }

    public Builder withCurrentCatalog(String currentCatalog) {
      this.currentCatalog = currentCatalog;
      return this;
    }

    public Builder withCurrentNamespace(String[] currentNamespace) {
      this.currentNamespace = currentNamespace == null ? new String[0] : currentNamespace;
      return this;
    }

    public Builder withSqlConfigs(Map<String, String> sqlConfigs) {
      this.sqlConfigs = new HashMap<>(sqlConfigs);
      return this;
    }

    public Builder withSchemaMode(String schemaMode) {
      this.schemaMode = schemaMode;
      return this;
    }

    public Builder withQueryColumnNames(String[] queryColumnNames) {
      this.queryColumnNames = queryColumnNames == null ? new String[0] : queryColumnNames;
      return this;
    }

    /**
     * Sets the structured dependency list for this view. Source tables and functions referenced
     * by the view text should be recorded here so downstream consumers (e.g. catalogs persisting
     * lineage) can access them without re-analyzing the view body.
     */
    public Builder withViewDependencies(DependencyList viewDependencies) {
      this.viewDependencies = viewDependencies;
      return this;
    }

    public View build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new View(this);
    }
  }
}
