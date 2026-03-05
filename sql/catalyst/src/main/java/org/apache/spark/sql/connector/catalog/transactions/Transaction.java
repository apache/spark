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

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.TransactionalCatalogPlugin;

import java.io.Closeable;

/**
 * Represents a transaction.
 * <p>
 * Spark begins a transaction with {@link TransactionalCatalogPlugin#beginTransaction} and
 * executes read/write operations against the transaction's catalog. On success, Spark
 * calls {@link #commit()}; on failure, Spark calls {@link #abort()}. In both cases Spark
 * subsequently calls {@link #close()} to release resources.
 *
 * @since 4.2.0
 */
public interface Transaction extends Closeable {

  /**
   * Returns the catalog associated with this transaction. This catalog is responsible for tracking
   * read/write operations that occur within the boundaries of a transaction. This allows
   * connectors to perform conflict resolution at commit time.
   */
  CatalogPlugin catalog();

  /**
   * Commits the transaction. All writes performed under it become visible to other readers.
   * <p>
   * The connector is responsible for detecting and resolving conflicting commits or throwing
   * an exception if resolution is not possible.
   * <p>
   * This method must be called exactly once. Spark calls {@link #close()} immediately after
   * this method returns, so implementations should not release resources inside
   * {@code commit()} itself.
   */
  void commit();

  /**
   * Aborts the transaction, discarding any staged changes.
   * <p>
   * This method must be idempotent. If the transaction has already been committed or aborted,
   * invoking it must have no effect.
   * <p>
   * Spark calls {@link #close()} immediately after this method returns.
   */
  void abort();

  /**
   * Releases any resources held by this transaction.
   * <p>
   * Spark always calls this method after {@link #commit()} or {@link #abort()}, regardless of
   * whether those methods succeed or not.
   * <p>
   * This method must be idempotent. If the transaction has already been closed,
   * invoking it must have no effect.
   */
  @Override
  void close();
}
