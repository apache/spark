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

package org.apache.spark.sql.streaming

trait StateStoreMetricsTest extends StreamTest {

  def assertNumStateRows(total: Seq[Long], updated: Seq[Long]): AssertOnQuery =
    AssertOnQuery { q =>
      val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
      assert(
        progressWithData.stateOperators.map(_.numRowsTotal) === total,
        "incorrect total rows")
      assert(
        progressWithData.stateOperators.map(_.numRowsUpdated) === updated,
        "incorrect updates rows")
      true
    }

  def assertNumStateRows(total: Long, updated: Long): AssertOnQuery =
    assertNumStateRows(Seq(total), Seq(updated))
}
