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

import org.apache.spark.annotation.Evolving;

/**
 * A relation in a catalog: either a {@link Table} or a {@link View}. This is the common type
 * returned by {@link RelationCatalog#loadRelation} so a catalog that exposes both kinds can
 * answer a single read in one round trip; callers discriminate with {@code instanceof Table} /
 * {@code instanceof View}.
 * <p>
 * The two kinds are deliberately asymmetric, mirroring how Spark treats them: a {@link Table} is
 * an object Spark reads from and writes to, while a {@link View} carries only metadata (Spark
 * expands its query text at read time and never builds a view object). Modeling both as siblings
 * of {@code Relation} -- rather than smuggling a view through the {@code Table} surface -- keeps
 * table-only concepts (partitioning, constraints, scans, writes) off the view side.
 * <p>
 * In practice the only two kinds are {@link Table} and {@link View}. {@code Relation} is left
 * un-sealed because {@link Table} is itself an open interface (so a closed hierarchy would add
 * little) and because a sealed Java interface trips Scala's pattern-match analysis.
 *
 * @since 4.2.0
 */
@Evolving
public interface Relation {

  /**
   * The columns of this relation.
   */
  Column[] columns();

  /**
   * The string map of properties of this relation.
   */
  Map<String, String> properties();
}
