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
import java.util.Set;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * A concrete {@code Table} implementation that only contains the table metadata without
 * implementing read/write directly. It represents a general Spark data source table or
 * a Spark view, and relies on Spark to interpret the table metadata, resolve the table
 * provider into a data source, or read it as a view.
 *
 * @since 4.1.0
 */
@Evolving
public class MetadataOnlyTable implements Table {
  private final Builder builder;

  private MetadataOnlyTable(Builder builder) {
    this.builder = builder;
  }

  public static class Builder {
    private final Column[] columns;
    private String name = "data_source_table_or_view";
    private String provider = null;
    private String location = null;
    private String tableType = null;
    private String owner = null;
    private String comment = null;
    private String collation = null;
    private String viewText = null;
    private String createVersion = "";
    private long createTime = 0;
    private Map<String, String> tableProps = Map.ofEntries();
    private Transform[] partitioning = new Transform[0];

    public Builder(Column[] columns) {
      assert columns != null;
      this.columns = columns;
    }

    public Builder(StructType schema) {
      assert schema != null;
      this.columns = CatalogV2Util.structTypeToV2Columns(schema);
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withProvider(String provider) {
      this.provider = provider;
      return this;
    }

    public Builder withLocation(String location) {
      this.location = location;
      return this;
    }

    public Builder withTableType(String tableType) {
      this.tableType = tableType;
      return this;
    }

    public Builder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    public Builder withCollation(String collation) {
      this.collation = collation;
      return this;
    }

    public Builder withViewText(String viewText) {
      this.viewText = viewText;
      this.tableType = TableSummary.VIEW_TABLE_TYPE;
      return this;
    }

    public Builder withCreateVersion(String createVersion) {
      this.createVersion = createVersion;
      return this;
    }

    public Builder withCreateTime(long createTime) {
      this.createTime = createTime;
      return this;
    }

    public Builder withTableProps(Map<String, String> tableProps) {
      this.tableProps = tableProps;
      return this;
    }

    public Builder withPartitioning(Transform[] partitioning) {
      this.partitioning = partitioning;
      return this;
    }

    public MetadataOnlyTable build() {
      return new MetadataOnlyTable(this);
    }
  }

  public String getProvider() {
    return builder.provider;
  }

  public String getLocation() {
    return builder.location;
  }

  public String getTableType() {
    return builder.tableType;
  }

  public String getOwner() {
    return builder.owner;
  }

  public String getComment() {
    return builder.comment;
  }

  public String getCollation() {
    return builder.collation;
  }

  public String getViewText() {
    return builder.viewText;
  }

  public String getCreateVersion() {
    return builder.createVersion;
  }

  public long getCreateTime() {
    return builder.createTime;
  }

  public Map<String, String> getTableProps() {
    return builder.tableProps;
  }

  @Override
  public Column[] columns() {
    return builder.columns;
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> props = new java.util.HashMap<>(builder.tableProps);
    if (getProvider() != null) {
      props.put(TableCatalog.PROP_PROVIDER, getProvider());
    }
    if (getLocation() != null) {
      props.put(TableCatalog.PROP_LOCATION, getLocation());
    }
    if (getTableType() != null) {
      props.put(TableCatalog.PROP_TABLE_TYPE, getTableType());
    }
    if (getOwner() != null) {
      props.put(TableCatalog.PROP_OWNER, getOwner());
    }
    if (getComment() != null) {
      props.put(TableCatalog.PROP_COMMENT, getComment());
    }
    if (getCollation() != null) {
      props.put(TableCatalog.PROP_COLLATION, getCollation());
    }
    if (getViewText() != null) {
      props.put(TableCatalog.PROP_VIEW_TEXT, getViewText());
    }
    return props;
  }

  @Override
  public Transform[] partitioning() {
    return builder.partitioning;
  }

  @Override
  public String name() {
    return builder.name;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of();
  }
}
