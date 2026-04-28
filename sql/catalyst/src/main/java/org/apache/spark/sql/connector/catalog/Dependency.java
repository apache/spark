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
 * Represents a dependency of a SQL object such as a view or metric view.
 * <p>
 * A dependency is one of: {@link TableDependency} or {@link FunctionDependency}.
 *
 * @since 4.2.0
 */
@Evolving
public interface Dependency {

  static TableDependency table(String tableFullName) {
    return new TableDependency(tableFullName);
  }

  static FunctionDependency function(String functionFullName) {
    return new FunctionDependency(functionFullName);
  }
}
