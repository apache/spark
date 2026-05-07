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

package org.apache.spark.sql.catalyst.transactions

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.TransactionalCatalogPlugin
import org.apache.spark.sql.connector.catalog.transactions.{Transaction, TransactionInfoImpl}
import org.apache.spark.util.Utils

object TransactionUtils {
  def commit(txn: Transaction): Unit = {
    Utils.tryWithSafeFinally {
      txn.commit()
    } {
      txn.close()
    }
  }

  def abort(txn: Transaction): Unit = {
    Utils.tryWithSafeFinally {
      txn.abort()
    } {
      txn.close()
    }
  }

  def beginTransaction(catalog: TransactionalCatalogPlugin): Transaction = {
    val info = TransactionInfoImpl(id = UUID.randomUUID.toString)
    val txn = catalog.beginTransaction(info)
    if (txn.catalog.name != catalog.name) {
      abort(txn)
      throw SparkException.internalError(
        s"""Transaction catalog name (${txn.catalog.name})
           |must match original catalog name (${catalog.name}).""".stripMargin)
    }
    txn
  }
}
