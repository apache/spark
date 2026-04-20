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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

public class TableInfo {

  private final Column[] columns;
  private final Map<String, String> properties;
  private final Transform[] partitions;
  private final Constraint[] constraints;
  private final String tableType;
  private final String viewDefinition;
  private final DependencyList viewDependencies;

  /**
   * Constructor for TableInfo used by the builder.
   * @param builder Builder.
   */
  private TableInfo(Builder builder) {
    this.columns = builder.columns;
    this.properties = builder.properties;
    this.partitions = builder.partitions;
    this.constraints = builder.constraints;
    this.tableType = builder.tableType;
    this.viewDefinition = builder.viewDefinition;
    this.viewDependencies = builder.viewDependencies;
  }

  public Column[] columns() {
    return columns;
  }

  public StructType schema() {
    return CatalogV2Util.v2ColumnsToStructType(columns);
  }

  public Map<String, String> properties() {
    return properties;
  }

  public Transform[] partitions() {
    return partitions;
  }

  public Constraint[] constraints() { return constraints; }

  /**
   * The table type (e.g. "MANAGED", "EXTERNAL", "VIEW", "METRIC_VIEW").
   * May be null if the connector should infer the type from properties.
   *
   * @see TableSummary for table type constants
   * @since 4.2.0
   */
  public String tableType() { return tableType; }

  /**
   * The view definition text. For metric views this is the YAML body;
   * for regular views this would be the SQL text. May be null for non-view tables.
   *
   * @since 4.2.0
   */
  public String viewDefinition() { return viewDefinition; }

  /**
   * The list of dependencies for this view or metric view. May be null
   * if dependency information is not provided.
   *
   * @since 4.2.0
   */
  public DependencyList viewDependencies() { return viewDependencies; }

  public static class Builder {
    private Column[] columns = new Column[0];
    private Map<String, String> properties = new HashMap<>();
    private Transform[] partitions = new Transform[0];
    private Constraint[] constraints = new Constraint[0];
    private String tableType;
    private String viewDefinition;
    private DependencyList viewDependencies;

    public Builder withColumns(Column[] columns) {
      this.columns = columns;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public Builder withPartitions(Transform[] partitions) {
      this.partitions = partitions;
      return this;
    }

    public Builder withConstraints(Constraint[] constraints) {
      this.constraints = constraints;
      return this;
    }

    /** @since 4.2.0 */
    public Builder withTableType(String tableType) {
      this.tableType = tableType;
      return this;
    }

    /** @since 4.2.0 */
    public Builder withViewDefinition(String viewDefinition) {
      this.viewDefinition = viewDefinition;
      return this;
    }

    /** @since 4.2.0 */
    public Builder withViewDependencies(DependencyList viewDependencies) {
      this.viewDependencies = viewDependencies;
      return this;
    }

    public TableInfo build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new TableInfo(this);
    }
  }
}
