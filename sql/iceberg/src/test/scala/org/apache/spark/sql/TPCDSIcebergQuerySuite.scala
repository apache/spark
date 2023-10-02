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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class TPCDSIcebergQuerySuite extends TPCDSQuerySuite with TPCDSIcebergBase {
  // these queries are failing due to call of estimation of stats before pushdown of filters &
  // columns
  override def excludedTpcdsQueries: Set[String] = super.excludedTpcdsQueries ++
    Set("q14a", "q14b", "q38", "q87")

  override def excludedTpcdsV2_7_0Queries: Set[String] = super.excludedTpcdsV2_7_0Queries ++
    Set("q14", "q14a")
}

// TODO(Asif): Fix the catalog issue of test
/*
@ExtendedSQLTest
class TPCDSIcebergQueryWithStatsSuite extends TPCDSIcebergQuerySuite {
  override def injectStats: Boolean = true
}
*/

@ExtendedSQLTest
class TPCDSIcebergQueryANSISuite extends TPCDSIcebergQuerySuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
