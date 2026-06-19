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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * Common metadata shared by {@link TableInfo} (data-source tables) and {@link ViewInfo} (views):
 * the columns and the string properties bag. Tables and views are modeled as sibling subclasses
 * rather than one extending the other, so that table-only concepts (partitioning, constraints,
 * provider, location) never leak onto the view builder and vice versa.
 * <p>
 * A {@code MetadataTable} wraps a {@code RelationInfo} so a single carrier can transport either
 * kind through {@link TableViewCatalog#loadTableOrView}; downstream consumers discriminate via
 * {@code getRelationInfo() instanceof ViewInfo}.
 *
 * @since 4.2.0
 */
@Evolving
public abstract class RelationInfo {

  private final Column[] columns;
  private final Map<String, String> properties;

  protected RelationInfo(BaseBuilder<?> builder) {
    this.columns = builder.columns;
    this.properties = builder.properties;
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

  /**
   * Shared builder state for {@link TableInfo} and {@link ViewInfo}. Holds only the fields common
   * to both -- columns and properties -- plus the convenience setters for reserved property keys
   * that apply to both tables and views. Setters return {@code B} so subclass builders chain
   * through their own type without a covariant override on each inherited setter. Table-only
   * setters live on {@link TableInfo.Builder}.
   */
  protected abstract static class BaseBuilder<B extends BaseBuilder<B>> {
    protected Column[] columns = new Column[0];
    protected Map<String, String> properties = new HashMap<>();

    protected abstract B self();

    public B withColumns(Column[] columns) {
      this.columns = columns;
      return self();
    }

    public B withSchema(StructType schema) {
      this.columns = CatalogV2Util.structTypeToV2Columns(schema);
      return self();
    }

    /**
     * Replaces the current properties map with a defensive copy of the given map. Any reserved
     * keys set earlier via convenience setters (e.g. {@link #withComment}) are discarded --
     * call those setters <i>after</i> this method, not before.
     */
    public B withProperties(Map<String, String> properties) {
      this.properties = new HashMap<>(properties);
      return self();
    }

    // Convenience setters below write reserved keys into the current `properties` map. Pair
    // each with a preceding `withProperties(...)` call if you want to start from a user map;
    // calling `withProperties` after a convenience setter discards the value the convenience
    // setter wrote.

    public B withComment(String comment) {
      properties.put(TableCatalog.PROP_COMMENT, comment);
      return self();
    }

    public B withCollation(String collation) {
      properties.put(TableCatalog.PROP_COLLATION, collation);
      return self();
    }

    public B withOwner(String owner) {
      properties.put(TableCatalog.PROP_OWNER, owner);
      return self();
    }

    public B withTableType(String tableType) {
      properties.put(TableCatalog.PROP_TABLE_TYPE, tableType);
      return self();
    }

    public abstract RelationInfo build();
  }
}
