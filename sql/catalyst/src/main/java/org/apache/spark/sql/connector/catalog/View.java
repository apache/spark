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

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.types.StructType;

/**
 * An interface representing a persisted view.
 */
@DeveloperApi
public interface View {
  /**
   * A name to identify this view.
   */
  String name();

  /**
   * The view query SQL text.
   */
  String query();

  /**
   * The current catalog when the view is created.
   */
  String currentCatalog();

  /**
   * The current namespace when the view is created.
   */
  String[] currentNamespace();

  /**
   * The schema for the view when the view is created after applying column aliases.
   */
  StructType schema();

  /**
   * The output column names of the query that creates this view.
   */
  String[] queryColumnNames();

  /**
   * The view column aliases.
   */
  String[] columnAliases();

  /**
   * The view column comments.
   */
  String[] columnComments();

  /**
   * The view properties.
   */
  Map<String, String> properties();
}
