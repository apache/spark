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

/**
 * View metadata DTO -- the typed payload returned by {@link ViewCatalog#loadView} and accepted
 * by {@link ViewCatalog#createView} / {@link ViewCatalog#replaceView}. Carries the
 * view-specific fields that cannot be represented as string table properties: the query text,
 * captured creation-time resolution context, captured SQL configs, schema-binding mode, and
 * query output column names. Schema and user TBLPROPERTIES are inherited from {@link TableInfo}
 * via the typed builder.
 * <p>
 * {@code ViewInfo} extends {@link TableInfo} so that a {@link RelationCatalog} can opt into the
 * single-RPC perf path by returning a {@link MetadataOnlyTable} wrapping a {@code ViewInfo}
 * from {@link RelationCatalog#loadRelation} for a view identifier. Pure {@link ViewCatalog}
 * implementations never see {@code TableInfo}; the typed setters on {@link Builder} cover
 * everything they need to construct a {@code ViewInfo}.
 *
 * @since 4.2.0
 */
@Evolving
public class ViewInfo extends TableInfo {

  private final String queryText;
  private final String currentCatalog;
  private final String[] currentNamespace;
  private final Map<String, String> sqlConfigs;
  private final String schemaMode;
  private final String[] queryColumnNames;

  private ViewInfo(Builder builder) {
    super(builder);
    this.queryText = Objects.requireNonNull(builder.queryText, "queryText should not be null");
    this.currentCatalog = builder.currentCatalog;
    this.currentNamespace = builder.currentNamespace;
    this.sqlConfigs = Collections.unmodifiableMap(builder.sqlConfigs);
    this.schemaMode = builder.schemaMode;
    this.queryColumnNames = builder.queryColumnNames;
    // Force PROP_TABLE_TYPE = VIEW so that `properties()` reflects the typed ViewInfo
    // classification. Catalogs and generic viewers reading PROP_TABLE_TYPE from the properties
    // bag (e.g. TableCatalog.listTableSummaries default impl, DESCRIBE) see "VIEW" without
    // requiring authors to remember to call withTableType(VIEW).
    properties().put(TableCatalog.PROP_TABLE_TYPE, TableSummary.VIEW_TABLE_TYPE);
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

  public static class Builder extends BaseBuilder<Builder> {
    private String queryText;
    private String currentCatalog;
    private String[] currentNamespace = new String[0];
    private Map<String, String> sqlConfigs = new HashMap<>();
    private String schemaMode;
    private String[] queryColumnNames = new String[0];

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

    @Override
    public ViewInfo build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new ViewInfo(this);
    }
  }
}
