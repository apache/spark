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

/**
 * ViewChange subclasses represent requested changes to a view.
 * These are passed to {@link ViewCatalog#alterView}.
 */
@DeveloperApi
public interface ViewChange {

  /**
   * Create a ViewChange for setting a table property.
   *
   * @param property the property name
   * @param value the new property value
   * @return a ViewChange
   */
  static ViewChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a ViewChange for removing a table property.
   *
   * @param property the property name
   * @return a ViewChange
   */
  static ViewChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  final class SetProperty implements ViewChange {
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

  final class RemoveProperty implements ViewChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }
  }
}
