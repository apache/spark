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

  private TableCatalog delegate;

  public final void setDelegateCatalog(TableCatalog delegate) {
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
    return delegate.listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return delegate.loadTable(ident);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    delegate.invalidateTable(ident);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return delegate.tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return delegate.createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException {
    return delegate.alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return delegate.dropTable(ident);
  }

  @Override
  public void renameTable(
      Identifier oldIdent,
      Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
    delegate.renameTable(oldIdent, newIdent);
  }
}
