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
 *
 */

package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

/**
  * Result of a Cypher query.
  *
  * Wraps a [[DataFrame]] that contains the result rows.
  */
trait CypherResult {
  /**
    * Contains the result rows.
    *
    * The column names are aligned with the return item names specified within the Cypher query,
    * (e.g. `RETURN foo, bar AS baz` results in the columns `foo` and `baz`).
    *
    * @note Dot characters (i.e. `.`) within return item names are replaced by the underscore (i.e. `_`),
    *       (e.g. `MATCH (n:Person) RETURN n` results in the columns `n`, `n:Person` and `n_name`).
    */
  def df: DataFrame
}
