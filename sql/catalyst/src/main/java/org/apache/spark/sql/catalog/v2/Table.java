/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalog.v2;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * Represents table metadata from a {@link TableCatalog} or other table sources.
 */
public interface Table {
  /**
   * Return the table properties.
   * @return this table's map of string properties
   */
  Map<String, String> properties();

  /**
   * Return the table schema.
   * @return this table's schema as a struct type
   */
  StructType schema();

  /**
   * Return the table partitioning expressions.
   * @return this table's partitioning expressions
   */
  List<Expression> partitionExpressions();
}
