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

import org.apache.spark.sql.catalyst.util.QuotingUtils;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

public class TableInfo {

  private final Column[] columns;
  private final Map<String, String> properties;
  private final Transform[] partitions;
  private final Constraint[] constraints;

  /**
   * Constructor for TableInfo used by the builder.
   * @param builder Builder.
   */
  private TableInfo(Builder builder) {
    this.columns = builder.columns;
    this.properties = builder.properties;
    this.partitions = builder.partitions;
    this.constraints = builder.constraints;
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

  public static class Builder {
    private Column[] columns = new Column[0];
    private Map<String, String> properties = new HashMap<>();
    private Transform[] partitions = new Transform[0];
    private Constraint[] constraints = new Constraint[0];

    public Builder withColumns(Column[] columns) {
      this.columns = columns;
      return this;
    }

    public Builder withSchema(StructType schema) {
      this.columns = CatalogV2Util.structTypeToV2Columns(schema);
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      this.properties = new HashMap<>(properties);
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

    // Convenience setters that write reserved keys into `properties`. These mutate the current
    // properties map, so call them after any `withProperties(...)` that replaces the map.

    public Builder withProvider(String provider) {
      properties.put(TableCatalog.PROP_PROVIDER, provider);
      return this;
    }

    public Builder withLocation(String location) {
      properties.put(TableCatalog.PROP_LOCATION, location);
      return this;
    }

    public Builder withComment(String comment) {
      properties.put(TableCatalog.PROP_COMMENT, comment);
      return this;
    }

    public Builder withCollation(String collation) {
      properties.put(TableCatalog.PROP_COLLATION, collation);
      return this;
    }

    public Builder withOwner(String owner) {
      properties.put(TableCatalog.PROP_OWNER, owner);
      return this;
    }

    public Builder withTableType(String tableType) {
      properties.put(TableCatalog.PROP_TABLE_TYPE, tableType);
      return this;
    }

    /**
     * Sets the view SQL text and marks this TableInfo as a view by setting
     * {@link TableCatalog#PROP_TABLE_TYPE} to {@link TableSummary#VIEW_TABLE_TYPE}.
     */
    public Builder withViewText(String viewText) {
      properties.put(TableCatalog.PROP_VIEW_TEXT, viewText);
      properties.put(TableCatalog.PROP_TABLE_TYPE, TableSummary.VIEW_TABLE_TYPE);
      return this;
    }

    public Builder withCurrentCatalog(String currentCatalog) {
      properties.put(TableCatalog.PROP_VIEW_CURRENT_CATALOG, currentCatalog);
      return this;
    }

    /**
     * Sets the current namespace of a view, encoded as a quoted multi-part identifier string
     * (see {@link TableCatalog#PROP_VIEW_CURRENT_NAMESPACE}). An empty array clears the property.
     */
    public Builder withCurrentNamespace(String[] currentNamespace) {
      if (currentNamespace != null && currentNamespace.length > 0) {
        properties.put(TableCatalog.PROP_VIEW_CURRENT_NAMESPACE,
            QuotingUtils.quoted(currentNamespace));
      } else {
        properties.remove(TableCatalog.PROP_VIEW_CURRENT_NAMESPACE);
      }
      return this;
    }

    public TableInfo build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new TableInfo(this);
    }
  }
}
