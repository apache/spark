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
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A simple implementation of {@link CatalogExtension}, which implements all the catalog functions
 * by calling the built-in session catalog directly. This is created for convenience, so that users
 * only need to override some methods where they want to apply custom logic. For example, they can
 * override {@code createTable}, do something else before calling {@code super.createTable}.
 *
 * @since 3.0.0
 */
@Evolving
public abstract class DelegatingCatalogExtension implements CatalogExtension {

  protected CatalogPlugin delegate;

  @Override
  public final void setDelegateCatalog(CatalogPlugin delegate) {
    this.delegate = delegate;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    delegate.initialize(name, options);
  }

  @Override
  public Set<TableCatalogCapability> capabilities() {
    return asTableCatalog().capabilities();
  }

  @Override
  public String[] defaultNamespace() {
    return delegate.defaultNamespace();
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return asTableCatalog().listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident);
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident, timestamp);
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident, version);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    asTableCatalog().invalidateTable(ident);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return asTableCatalog().tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return asTableCatalog().createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table createTable(
      Identifier ident,
      Column[] columns,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return asTableCatalog().createTable(ident, columns, partitions, properties);
  }

  @Override
  public Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException {
    return asTableCatalog().alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return asTableCatalog().dropTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    return asTableCatalog().purgeTable(ident);
  }

  @Override
  public void renameTable(
      Identifier oldIdent,
      Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
    asTableCatalog().renameTable(oldIdent, newIdent);
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return asNamespaceCatalog().listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().listNamespaces(namespace);
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return asNamespaceCatalog().namespaceExists(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(
      String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(
      String[] namespace,
      Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    asNamespaceCatalog().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(
      String[] namespace,
      NamespaceChange... changes) throws NoSuchNamespaceException {
    asNamespaceCatalog().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(
      String[] namespace,
      boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return asNamespaceCatalog().dropNamespace(namespace, cascade);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return asFunctionCatalog().loadFunction(ident);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return asFunctionCatalog().listFunctions(namespace);
  }

  @Override
  public boolean functionExists(Identifier ident) {
    return asFunctionCatalog().functionExists(ident);
  }

  private TableCatalog asTableCatalog() {
    return (TableCatalog) delegate;
  }

  private SupportsNamespaces asNamespaceCatalog() {
    return (SupportsNamespaces) delegate;
  }

  private FunctionCatalog asFunctionCatalog() {
    return (FunctionCatalog) delegate;
  }
}
