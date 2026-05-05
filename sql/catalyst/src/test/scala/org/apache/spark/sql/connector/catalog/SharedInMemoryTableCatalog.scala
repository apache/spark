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

import java.util
import java.util.concurrent.ConcurrentHashMap

/**
 * A [[NullTableIdInMemoryTableCatalog]] that shares table state across
 * all instances. This allows multiple [[SparkSession]]s to read and
 * write the same tables, simulating a real shared metastore.
 *
 * Table IDs are null (inherited from [[NullTableIdInMemoryTableCatalog]]),
 * so cross-session drop+recreate is detected via column IDs rather
 * than table IDs.
 */
class SharedInMemoryTableCatalog extends NullTableIdInMemoryTableCatalog {
  tables = SharedInMemoryTableCatalog.sharedTables
  override protected val namespaces: util.Map[List[String], Map[String, String]] =
    SharedInMemoryTableCatalog.sharedNamespaces
}

object SharedInMemoryTableCatalog {
  val sharedTables = new ConcurrentHashMap[Identifier, Table]()
  val sharedNamespaces = new ConcurrentHashMap[List[String], Map[String, String]]()

  def reset(): Unit = {
    sharedTables.clear()
    sharedNamespaces.clear()
  }
}
