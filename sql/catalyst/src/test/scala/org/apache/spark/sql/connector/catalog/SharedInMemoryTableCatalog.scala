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

package org.apache.spark.sql.connector.catalog

/**
 * An InMemoryTableCatalog that shares table state across all instances.
 * This allows multiple SparkSessions (via newSession/cloneSession) to
 * read and write the same tables, simulating a real shared metastore.
 *
 * Use this catalog for multi-session concurrency tests where one session
 * writes and another session reads the same DSv2 table.
 */
class SharedInMemoryTableCatalog extends InMemoryTableCatalog {
  override protected val tables: java.util.Map[Identifier, Table] =
    SharedInMemoryTableCatalog.sharedTables
  override protected val namespaces
    : java.util.Map[List[String], Map[String, String]] =
    SharedInMemoryTableCatalog.sharedNamespaces
}

object SharedInMemoryTableCatalog {
  val sharedTables =
    new java.util.concurrent.ConcurrentHashMap[Identifier, Table]()
  val sharedNamespaces =
    new java.util.concurrent.ConcurrentHashMap[
      List[String], Map[String, String]]()

  def reset(): Unit = {
    sharedTables.clear()
    sharedNamespaces.clear()
  }
}
