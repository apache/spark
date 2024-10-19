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

package org.apache.spark.sql.connector.catalog.index;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * Index in a table
 *
 * @since 3.3.0
 */
@Evolving
public final class TableIndex {
  private String indexName;
  private String indexType;
  private NamedReference[] columns;
  private Map<NamedReference, Properties> columnProperties = Collections.emptyMap();
  private Properties properties;

  public TableIndex(
      String indexName,
      String indexType,
      NamedReference[] columns,
      Map<NamedReference, Properties> columnProperties,
      Properties properties) {
    this.indexName = indexName;
    this.indexType = indexType;
    this.columns = columns;
    this.columnProperties = columnProperties;
    this.properties = properties;
  }

  /**
   * @return the Index name.
   */
  public String indexName() { return indexName; }

  /**
   * @return the indexType of this Index.
   */
  public String indexType() { return indexType; }

  /**
   * @return the column(s) this Index is on. Could be multi columns (a multi-column index).
   */
  public NamedReference[] columns() { return columns; }

  /**
   * @return the map of column and column property map.
   */
  public Map<NamedReference, Properties> columnProperties() { return columnProperties; }

  /**
   * Returns the index properties.
   */
  public Properties properties() { return properties; }
}
