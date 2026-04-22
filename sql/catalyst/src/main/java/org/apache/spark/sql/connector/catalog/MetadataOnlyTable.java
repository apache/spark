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

/**
 * A concrete {@code Table} implementation that only contains the table metadata without
 * implementing read/write directly. It represents a general Spark data source table or
 * a Spark view, and relies on Spark to interpret the table metadata, resolve the table
 * provider into a data source, or read it as a view.
 * <p>
 * Catalogs build the metadata via {@link TableInfo.Builder} (which provides convenience
 * setters for reserved properties such as {@link TableCatalog#PROP_PROVIDER},
 * {@link TableCatalog#PROP_LOCATION}, {@link TableCatalog#PROP_VIEW_TEXT}, etc.) and wrap
 * the resulting {@link TableInfo} in a {@code MetadataOnlyTable} to return from
 * {@link TableCatalog#loadTable(Identifier)}.
 *
 * @since 4.1.0
 */
@Evolving
public class MetadataOnlyTable implements Table {
  private static final String DEFAULT_NAME = "data_source_table_or_view";

  private final TableInfo info;
  private final String name;

  public MetadataOnlyTable(TableInfo info) {
    this(info, DEFAULT_NAME);
  }

  public MetadataOnlyTable(TableInfo info, String name) {
    this.info = info;
    this.name = name;
  }

  public TableInfo getTableInfo() {
    return info;
  }

  @Override
  public Column[] columns() {
    return info.columns();
  }

  @Override
  public Map<String, String> properties() {
    return info.properties();
  }

  @Override
  public Transform[] partitioning() {
    return info.partitions();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of();
  }
}
