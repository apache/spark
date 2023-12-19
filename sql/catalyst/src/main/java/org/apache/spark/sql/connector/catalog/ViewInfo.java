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

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A class that holds view information.
 */
@DeveloperApi
public class ViewInfo {
  private final Identifier ident;
  private final String sql;
  private final String currentCatalog;
  private final String[] currentNamespace;
  private final StructType schema;
  private final String[] queryColumnNames;
  private final String[] columnAliases;
  private final String[] columnComments;
  private final Map<String, String> properties;

  public ViewInfo(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties) {
    this.ident = ident;
    this.sql = sql;
    this.currentCatalog = currentCatalog;
    this.currentNamespace = currentNamespace;
    this.schema = schema;
    this.queryColumnNames = queryColumnNames;
    this.columnAliases = columnAliases;
    this.columnComments = columnComments;
    this.properties = properties;
  }

  /**
   * @return The view identifier
   */
  @Nonnull
  public Identifier ident() {
    return ident;
  }

  /**
   * @return The SQL text that defines the view
   */
  @Nonnull
  public String sql() {
    return sql;
  }

  /**
   * @return The current catalog
   */
  @Nonnull
  public String currentCatalog() {
    return currentCatalog;
  }

  /**
   * @return The current namespace
   */
  @Nonnull
  public String[] currentNamespace() {
    return currentNamespace;
  }

  /**
   * @return The view query output schema
   */
  @Nonnull
  public StructType schema() {
    return schema;
  }

  /**
   * @return The query column names
   */
  @Nonnull
  public String[] queryColumnNames() {
    return queryColumnNames;
  }

  /**
   * @return The column aliases
   */
  @Nonnull
  public String[] columnAliases() {
    return columnAliases;
  }

  /**
   * @return The column comments
   */
  @Nonnull
  public String[] columnComments() {
    return columnComments;
  }

  /**
   * @return The view properties
   */
  @Nonnull
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ViewInfo viewInfo = (ViewInfo) o;
    return ident.equals(viewInfo.ident) && sql.equals(viewInfo.sql) &&
        currentCatalog.equals(viewInfo.currentCatalog) &&
        Arrays.equals(currentNamespace, viewInfo.currentNamespace) &&
        schema.equals(viewInfo.schema) &&
        Arrays.equals(queryColumnNames, viewInfo.queryColumnNames) &&
        Arrays.equals(columnAliases, viewInfo.columnAliases) &&
        Arrays.equals(columnComments, viewInfo.columnComments) &&
        properties.equals(viewInfo.properties);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(ident, sql, currentCatalog, schema, properties);
    result = 31 * result + Arrays.hashCode(currentNamespace);
    result = 31 * result + Arrays.hashCode(queryColumnNames);
    result = 31 * result + Arrays.hashCode(columnAliases);
    result = 31 * result + Arrays.hashCode(columnComments);
    return result;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ViewInfo.class.getSimpleName() + "[", "]")
        .add("ident=" + ident)
        .add("sql='" + sql + "'")
        .add("currentCatalog='" + currentCatalog + "'")
        .add("currentNamespace=" + Arrays.toString(currentNamespace))
        .add("schema=" + schema)
        .add("queryColumnNames=" + Arrays.toString(queryColumnNames))
        .add("columnAliases=" + Arrays.toString(columnAliases))
        .add("columnComments=" + Arrays.toString(columnComments))
        .add("properties=" + properties)
        .toString();
  }
}
