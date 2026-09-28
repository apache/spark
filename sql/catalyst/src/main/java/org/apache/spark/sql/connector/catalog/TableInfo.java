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

import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * Metadata describing a data-source table: its columns, properties, partitioning and constraints.
 * Spark realizes a {@code TableInfo} into a {@link Table} via {@link DelegatingTable}; a catalog
 * that has its own {@link Table} object returns that instead. Views are described by the sibling
 * {@link View}, which -- unlike a table -- is itself a {@link Relation} because Spark never builds
 * a view object.
 */
public class TableInfo {

  /**
   * Write distribution mode for {@code DISTRIBUTED BY PARTITION}: cluster each write by the
   * table's partitioning. One of the three possible values of {@link #writeDistributionMode()}.
   */
  public static final String DISTRIBUTION_MODE_HASH = "hash";

  /**
   * Write distribution mode for a bare {@code ORDERED BY}: range-partition each write so the
   * ordering holds across its tasks, not only within one.
   */
  public static final String DISTRIBUTION_MODE_RANGE = "range";

  /**
   * Write distribution mode for {@code UNORDERED} or {@code LOCALLY ORDERED BY}: do not
   * distribute, so any ordering holds within a write task only.
   */
  public static final String DISTRIBUTION_MODE_NONE = "none";

  private final Column[] columns;
  private final Map<String, String> properties;
  private final Transform[] partitions;
  private final Constraint[] constraints;
  private final String writeDistributionMode;
  private final SortOrder[] writeOrdering;

  /**
   * Constructor for TableInfo used by the builder.
   */
  protected TableInfo(Builder builder) {
    this.columns = builder.columns;
    this.properties = builder.properties;
    this.partitions = builder.partitions;
    this.constraints = builder.constraints;
    this.writeDistributionMode = builder.writeDistributionMode;
    this.writeOrdering = builder.writeOrdering;
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
   * The requested write distribution: {@link #DISTRIBUTION_MODE_HASH},
   * {@link #DISTRIBUTION_MODE_RANGE}, {@link #DISTRIBUTION_MODE_NONE}, or null when the statement
   * did not ask for one, which leaves the choice to the catalog.
   * <p>
   * Only catalogs that report
   * {@link TableCatalogCapability#SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING} see a
   * request; for any other catalog, Spark rejects the statement.
   *
   * @since 4.4.0
   */
  public String writeDistributionMode() { return writeDistributionMode; }

  /**
   * The requested write ordering, empty when none was requested. Gated on the same capability as
   * {@link #writeDistributionMode()}.
   *
   * @since 4.4.0
   */
  public SortOrder[] writeOrdering() { return writeOrdering; }

  public static class Builder extends RelationBuilder<Builder> {
    protected Transform[] partitions = new Transform[0];
    protected Constraint[] constraints = new Constraint[0];
    protected String writeDistributionMode = null;
    protected SortOrder[] writeOrdering = new SortOrder[0];

    @Override
    protected Builder self() { return this; }

    public Builder withPartitions(Transform[] partitions) {
      this.partitions = partitions;
      return this;
    }

    public Builder withConstraints(Constraint[] constraints) {
      this.constraints = constraints;
      return this;
    }

    /**
     * Sets the requested write distribution. See {@link TableInfo#writeDistributionMode()}.
     *
     * @since 4.4.0
     */
    public Builder withWriteDistributionMode(String writeDistributionMode) {
      this.writeDistributionMode = writeDistributionMode;
      return this;
    }

    /**
     * Sets the requested write ordering. See {@link TableInfo#writeOrdering()}.
     *
     * @since 4.4.0
     */
    public Builder withWriteOrdering(SortOrder[] writeOrdering) {
      this.writeOrdering = writeOrdering;
      return this;
    }

    /** Writes {@link TableCatalog#PROP_PROVIDER} into the current properties map. */
    public Builder withProvider(String provider) {
      properties.put(TableCatalog.PROP_PROVIDER, provider);
      return this;
    }

    public Builder withLocation(String location) {
      properties.put(TableCatalog.PROP_LOCATION, location);
      return this;
    }

    public TableInfo build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new TableInfo(this);
    }
  }
}
