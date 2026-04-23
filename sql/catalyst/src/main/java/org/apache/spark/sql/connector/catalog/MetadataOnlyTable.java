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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;

/**
 * A concrete {@code Table} implementation that contains only table metadata, deferring
 * read/write to Spark. It represents a general Spark data source table or a Spark view;
 * Spark resolves the table provider into a data source or expands the view text at read time.
 * <p>
 * Catalogs build the metadata via {@link TableInfo.Builder} (which provides convenience
 * setters for reserved properties such as {@link TableCatalog#PROP_PROVIDER},
 * {@link TableCatalog#PROP_LOCATION}, {@link TableCatalog#PROP_VIEW_TEXT}, etc.) and wrap
 * the resulting {@link TableInfo} in a {@code MetadataOnlyTable} to return from
 * {@link TableCatalog#loadTable(Identifier)}.
 *
 * @since 4.2.0
 */
@Evolving
public class MetadataOnlyTable implements Table {
  private final TableInfo info;
  private final String name;

  /**
   * @param info metadata for the table or view.
   * @param name human-readable name for this table, used by places that read {@link #name()}
   *             (e.g. the {@code Name} row of {@code DESCRIBE TABLE EXTENDED}). Catalogs
   *             returning a {@code MetadataOnlyTable} from {@link TableCatalog#loadTable}
   *             should typically pass {@code ident.toString()}, matching the quoted
   *             multi-part form used elsewhere for v2 identifiers.
   */
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
    return Collections.unmodifiableMap(info.properties());
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
