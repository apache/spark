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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.util.ArrayImplicits._

/**
 * Per-microbatch processor for SCD Type 1 AutoCDC flows, complying to the specified [[changeArgs]]
 * configuration.
 */
case class Scd1BatchProcessor(changeArgs: ChangeArgs) {

  /**
   * Deduplicate the incoming CDC microbatch by key, keeping the most recent event per key
   * as ordered by [[ChangeArgs.sequencing]].
   *
   * For SCD1 we only care about the most recent (by sequence value) event per key. When
   * multiple events share the same key and the same sequence value, the row selected is
   * non-deterministic and undefined.
   * 
   * The schema of the returned dataframe matches the schema of the microbatch exactly.
   */
  def deduplicateMicrobatch(microbatchDf: DataFrame): DataFrame = {
    // The `max_by` API can only return a single column, so pack/unpack the entire row into a
    // temporary column before and after the `max_by` operation.
    val winningRowCol = OutOfOrderCdcMergeUtils.tempColName("__winning_row")

    val allMicrobatchColumns =
      microbatchDf.columns
        .map(colName => F.col(QuotingUtils.quoteIdentifier(colName)))
        .toImmutableArraySeq

    microbatchDf
      .groupBy(changeArgs.keys.map(k => F.col(k.quoted)): _*)
      .agg(
        F.max_by(F.struct(allMicrobatchColumns: _*), changeArgs.sequencing)
          .as(winningRowCol)
      )
      .select(F.col(s"$winningRowCol.*"))
  }
}
