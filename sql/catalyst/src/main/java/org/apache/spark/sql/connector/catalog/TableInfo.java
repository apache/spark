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
   * The write distribution mode a statement asks for with {@code DISTRIBUTED BY PARTITION}:
   * cluster each write by the table's partitioning.
   * <p>
   * This is one of exactly three values a non-null {@link #writeDistributionMode()} can take. The
   * set is closed, so a catalog may treat an unrecognized value as an error rather than a mode it
   * does not know yet.
   */
  public static final String DISTRIBUTION_MODE_HASH = "hash";

  /**
   * The write distribution mode implied by a global {@code ORDERED BY}: range-partition each write
   * so the ordering holds across write tasks, not only within one. See
   * {@link #DISTRIBUTION_MODE_HASH}.
   */
  public static final String DISTRIBUTION_MODE_RANGE = "range";

  /**
   * The write distribution mode a statement asks for with {@code UNORDERED}, or with
   * {@code LOCALLY ORDERED BY}: do not distribute, so any ordering holds within a write task only.
   * <p>
   * This is an explicit request, distinct from a null {@link #writeDistributionMode()}, which means
   * the statement said nothing and leaves the choice to the catalog's own default. See
   * {@link #DISTRIBUTION_MODE_HASH}.
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
   * The write distribution the statement asked for, or null when it asked for none.
   * <p>
   * A non-null value is one of {@link #DISTRIBUTION_MODE_HASH}, {@link #DISTRIBUTION_MODE_RANGE} or
   * {@link #DISTRIBUTION_MODE_NONE}. Null means the statement said nothing, which is distinct from
   * {@code none}: null leaves the choice to the catalog's own default, while {@code none} is an
   * explicit request not to distribute.
   * <p>
   * A catalog only sees a non-null value if it returns
   * {@link TableCatalogCapability#SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING} from
   * {@link TableCatalog#capabilities()}; otherwise Spark rejects the statement while planning it,
   * rather than creating a table that silently lacks the requested layout.
   *
   * @since 4.4.0
   */
  public String writeDistributionMode() { return writeDistributionMode; }

  /**
   * The write ordering the statement asked for, empty when it asked for none. Gated on the same
   * capability as {@link #writeDistributionMode()}.
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
