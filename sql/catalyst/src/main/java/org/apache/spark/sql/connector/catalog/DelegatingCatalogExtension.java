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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A simple implementation of {@link CatalogExtension}, which implements all the catalog functions
 * by calling the built-in session catalog directly. This is created for convenience, so that users
 * only need to override some methods where they want to apply custom logic. For example, they can
 * override {@code createTable}, do something else before calling {@code super.createTable}.
 */
@Experimental
public abstract class DelegatingCatalogExtension implements CatalogExtension {

  private CatalogPlugin delegate;

  public final void setDelegateCatalog(CatalogPlugin delegate) {
    this.delegate = delegate;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {}

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return ((TableCatalog)delegate).listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return ((TableCatalog)delegate).loadTable(ident);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    ((TableCatalog)delegate).invalidateTable(ident);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return ((TableCatalog)delegate).tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return ((TableCatalog)delegate).createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException {
    return ((TableCatalog)delegate).alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return ((TableCatalog)delegate).dropTable(ident);
  }

  @Override
  public void renameTable(
      Identifier oldIdent,
      Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
    ((TableCatalog)delegate).renameTable(oldIdent, newIdent);
  }

  @Override
  public String[] defaultNamespace() {
    return ((SupportsNamespaces)delegate).defaultNamespace();
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return ((SupportsNamespaces)delegate).listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return ((SupportsNamespaces)delegate).listNamespaces(namespace);
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return ((SupportsNamespaces)delegate).namespaceExists(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(
      String[] namespace) throws NoSuchNamespaceException {
    return ((SupportsNamespaces)delegate).loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(
      String[] namespace,
      Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    ((SupportsNamespaces)delegate).createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(
      String[] namespace,
      NamespaceChange... changes) throws NoSuchNamespaceException {
    ((SupportsNamespaces)delegate).alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    return ((SupportsNamespaces)delegate).dropNamespace(namespace);
  }
}
