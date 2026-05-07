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

  /**
   * Constructor for TableInfo used by the builder.
   */
  protected TableInfo(BaseBuilder<?> builder) {
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

  public static class Builder extends BaseBuilder<Builder> {
    @Override
    protected Builder self() { return this; }

    @Override
    public TableInfo build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new TableInfo(this);
    }
  }

  /**
   * Shared builder state for {@link TableInfo} and its subclasses. Setters return {@code B} so
   * subclass builders (e.g. {@link ViewInfo.Builder}) chain through their own type without
   * a covariant override on each inherited setter.
   */
  protected abstract static class BaseBuilder<B extends BaseBuilder<B>> {
    protected Column[] columns = new Column[0];
    protected Map<String, String> properties = new HashMap<>();
    protected Transform[] partitions = new Transform[0];
    protected Constraint[] constraints = new Constraint[0];

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
     * keys set earlier via convenience setters (e.g. {@link #withProvider}) are discarded --
     * call those setters <i>after</i> this method, not before.
     */
    public B withProperties(Map<String, String> properties) {
      this.properties = new HashMap<>(properties);
      return self();
    }

    public B withPartitions(Transform[] partitions) {
      this.partitions = partitions;
      return self();
    }

    public B withConstraints(Constraint[] constraints) {
      this.constraints = constraints;
      return self();
    }

    // Convenience setters below write reserved keys into the current `properties` map. Pair
    // each with a preceding `withProperties(...)` call if you want to start from a user map;
    // calling `withProperties` after a convenience setter discards the value the convenience
    // setter wrote.

    /** Writes {@link TableCatalog#PROP_PROVIDER} into the current properties map. */
    public B withProvider(String provider) {
      properties.put(TableCatalog.PROP_PROVIDER, provider);
      return self();
    }

    public B withLocation(String location) {
      properties.put(TableCatalog.PROP_LOCATION, location);
      return self();
    }

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

    public abstract TableInfo build();
  }
}
