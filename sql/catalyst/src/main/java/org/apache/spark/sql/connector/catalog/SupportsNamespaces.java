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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;

import java.util.Map;

/**
 * Catalog methods for working with namespaces.
 * <p>
 * If an object such as a table, view, or function exists, its parent namespaces must also exist
 * and must be returned by the discovery methods {@link #listNamespaces()} and
 * {@link #listNamespaces(String[])}.
 * <p>
 * Catalog implementations are not required to maintain the existence of namespaces independent of
 * objects in a namespace. For example, a function catalog that loads functions using reflection
 * and uses Java packages as namespaces is not required to support the methods to create, alter, or
 * drop a namespace. Implementations are allowed to discover the existence of objects or namespaces
 * without throwing {@link NoSuchNamespaceException} when no namespace is found.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsNamespaces extends CatalogPlugin {

  /**
   * A reserved property to specify the location of the namespace. If the namespace
   * needs to store files, it should be under this location.
   */
  String PROP_LOCATION = "location";

  /**
   * A reserved property to specify the description of the namespace. The description
   * will be returned in the result of "DESCRIBE NAMESPACE" command.
   */
  String PROP_COMMENT = "comment";

  /**
   * A reserved property to specify the owner of the namespace.
   */
  String PROP_OWNER = "owner";

  /**
   * List top-level namespaces from the catalog.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist
   * and must be returned by this discovery method. For example, if table a.b.t exists, this method
   * must return ["a"] in the result array.
   *
   * @return an array of multi-part namespace names
   */
  String[][] listNamespaces() throws NoSuchNamespaceException;

  /**
   * List namespaces in a namespace.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist
   * and must be returned by this discovery method. For example, if table a.b.t exists, this method
   * invoked as listNamespaces(["a"]) must return ["a", "b"] in the result array.
   *
   * @param namespace a multi-part namespace
   * @return an array of multi-part namespace names
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Test whether a namespace exists.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist.
   * For example, if table a.b.t exists, this method invoked as namespaceExists(["a"]) or
   * namespaceExists(["a", "b"]) must return true.
   *
   * @param namespace a multi-part namespace
   * @return true if the namespace exists, false otherwise
   */
  default boolean namespaceExists(String[] namespace) {
    try {
      loadNamespaceMetadata(namespace);
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a multi-part namespace
   * @return a string map of properties for the given namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Create a namespace in the catalog.
   *
   * @param namespace a multi-part namespace
   * @param metadata a string map of properties for the given namespace
   * @throws NamespaceAlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  void createNamespace(
      String[] namespace,
      Map<String, String> metadata) throws NamespaceAlreadyExistsException;

  /**
   * Apply a set of metadata changes to a namespace in the catalog.
   *
   * @param namespace a multi-part namespace
   * @param changes a collection of changes to apply to the namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  void alterNamespace(
      String[] namespace,
      NamespaceChange... changes) throws NoSuchNamespaceException;

  /**
   * Drop a namespace from the catalog, recursively dropping all objects within the namespace.
   * <p>
   * If the catalog implementation does not support this operation, it may throw
   * {@link UnsupportedOperationException}.
   *
   * @param namespace a multi-part namespace
   * @return true if the namespace was dropped
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If drop is not a supported operation
   */
  boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException;
}
