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

package org.apache.spark.sql.connector.catalog.transactions;

import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.read.Scan;

/**
 * A previously-materialized scan whose result is held in Spark's DataFrame cache.
 * <p>
 * Spark passes a list of these to {@link Transaction#registerScans} when it has a chance to
 * substitute a cached subtree into the current query and needs the connector to decide whether
 * reusing that cached read is consistent with the transaction's isolation contract.
 *
 * @since 4.2.0
 */
@Evolving
public final class CachedScan {
  private final Table table;
  private final Scan scan;

  public CachedScan(Table table, Scan scan) {
    this.table = Objects.requireNonNull(table, "table");
    this.scan = Objects.requireNonNull(scan, "scan");
  }

  /** The table that was scanned. */
  public Table table() { return table; }

  /** The scan instance that was executed to produce the cached result. */
  public Scan scan() { return scan; }
}
