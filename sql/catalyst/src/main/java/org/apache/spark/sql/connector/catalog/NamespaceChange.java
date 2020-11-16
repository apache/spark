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

/**
 * NamespaceChange subclasses represent requested changes to a namespace. These are passed to
 * {@link SupportsNamespaces#alterNamespace}. For example,
 * <pre>
 *   import NamespaceChange._
 *   val catalog = Catalogs.load(name)
 *   catalog.alterNamespace(ident,
 *       setProperty("prop", "value"),
 *       removeProperty("other_prop")
 *     )
 * </pre>
 *
 * @since 3.0.0
 */
@Evolving
public interface NamespaceChange {
  /**
   * Create a NamespaceChange for setting a namespace property.
   * <p>
   * If the property already exists, it will be replaced with the new value.
   *
   * @param property the property name
   * @param value the new property value
   * @return a NamespaceChange for the addition
   */
  static NamespaceChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a NamespaceChange for removing a namespace property.
   * <p>
   * If the property does not exist, the change will succeed.
   *
   * @param property the property name
   * @return a NamespaceChange for the addition
   */
  static NamespaceChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * A NamespaceChange to set a namespace property.
   * <p>
   * If the property already exists, it must be replaced with the new value.
   */
  final class SetProperty implements NamespaceChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public String property() {
      return property;
    }

    public String value() {
      return value;
    }
  }

  /**
   * A NamespaceChange to remove a namespace property.
   * <p>
   * If the property does not exist, the change should succeed.
   */
  final class RemoveProperty implements NamespaceChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }
  }
}
