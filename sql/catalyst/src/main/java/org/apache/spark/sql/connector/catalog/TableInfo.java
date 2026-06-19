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

import java.util.Objects;

import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.expressions.Transform;

/**
 * Metadata for a data-source table. Adds the table-only fields -- partitioning and constraints --
 * on top of the columns and properties carried by {@link RelationInfo}. Views are represented by
 * the sibling {@link ViewInfo}; the two share {@link RelationInfo} rather than one extending the
 * other.
 */
public class TableInfo extends RelationInfo {

  private final Transform[] partitions;
  private final Constraint[] constraints;

  /**
   * Constructor for TableInfo used by the builder.
   */
  protected TableInfo(Builder builder) {
    super(builder);
    this.partitions = builder.partitions;
    this.constraints = builder.constraints;
  }

  public Transform[] partitions() {
    return partitions;
  }

  public Constraint[] constraints() { return constraints; }

  public static class Builder extends BaseBuilder<Builder> {
    protected Transform[] partitions = new Transform[0];
    protected Constraint[] constraints = new Constraint[0];

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

    /** Writes {@link TableCatalog#PROP_PROVIDER} into the current properties map. */
    public Builder withProvider(String provider) {
      properties.put(TableCatalog.PROP_PROVIDER, provider);
      return this;
    }

    public Builder withLocation(String location) {
      properties.put(TableCatalog.PROP_LOCATION, location);
      return this;
    }

    @Override
    public TableInfo build() {
      Objects.requireNonNull(columns, "columns should not be null");
      return new TableInfo(this);
    }
  }
}
