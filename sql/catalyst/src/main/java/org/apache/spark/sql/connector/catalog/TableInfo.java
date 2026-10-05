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
 * Metadata describing a data-source table: its columns, properties, partitioning, constraints,
 * and requested write distribution and ordering.
 * Spark realizes a {@code TableInfo} into a {@link Table} via {@link DelegatingTable}; a catalog
 * that has its own {@link Table} object returns that instead. Views are described by the sibling
 * {@link View}, which -- unlike a table -- is itself a {@link Relation} because Spark never builds
 * a view object.
 */
public class TableInfo {

  private final Column[] columns;
  private final Map<String, String> properties;
  private final Transform[] partitions;
  private final Constraint[] constraints;
  private final WriteDistributionMode writeDistributionMode;
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
    this.writeOrdering =
      Objects.requireNonNull(builder.writeOrdering, "writeOrdering should not be null");
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
   * The requested write distribution, or null when the statement did not ask for one, which
   * leaves the choice to the catalog.
   * <p>
   * Only catalogs that report
   * {@link TableCatalogCapability#SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING} see a
   * request; for any other catalog, Spark rejects the statement.
   * <p>
   * Spark passes the request only to the {@code TableInfo} overloads of
   * {@link TableCatalog#createTable(Identifier, TableInfo)} and of the {@link StagingTableCatalog}
   * methods {@code stageCreate}, {@code stageReplace} and {@code stageCreateOrReplace}. Their
   * default implementations drop it, so a catalog that reports the capability must override each
   * one it can be reached through. {@link TableCatalog#createTableLike} never gets a request: Spark
   * does not copy the source table's write distribution and ordering into its {@code TableInfo}.
   *
   * @since 4.4.0
   */
  public WriteDistributionMode writeDistributionMode() { return writeDistributionMode; }

  /**
   * The requested write ordering; never null, and empty when none was requested. Gated on the
   * same capability and delivered the same way as {@link #writeDistributionMode()}.
   * <p>
   * A plain column is a {@link org.apache.spark.sql.connector.expressions.NamedReference}; any
   * other key is a {@link Transform}, such as {@code bucket(16, id)}. Spark checks that each
   * referenced column exists in the table schema, and the parser checks the arguments of
   * {@code bucket}, {@code years}, {@code months}, {@code days} and {@code hours}. Spark does not
   * check that a key is orderable or that any other transform accepts its arguments, so a catalog
   * must reject a key it cannot honor.
   *
   * @since 4.4.0
   */
  public SortOrder[] writeOrdering() { return writeOrdering; }

  public static class Builder extends RelationBuilder<Builder> {
    protected Transform[] partitions = new Transform[0];
    protected Constraint[] constraints = new Constraint[0];
    protected WriteDistributionMode writeDistributionMode = null;
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
    public Builder withWriteDistributionMode(WriteDistributionMode writeDistributionMode) {
      this.writeDistributionMode = writeDistributionMode;
      return this;
    }

    /**
     * Sets the requested write ordering, which must not be null. See
     * {@link TableInfo#writeOrdering()}.
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
