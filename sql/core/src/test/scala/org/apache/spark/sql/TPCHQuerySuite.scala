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

import org.apache.spark.sql.catalyst.util.resourceToString

/**
 * This test suite ensures all the TPC-H queries can be successfully analyzed, optimized
 * and compiled without hitting the max iteration threshold.
 */
class TPCHQuerySuite extends BenchmarkQueryTest with TPCHBase {
  tpchQueries.foreach { name =>
    val queryString = resourceToString(s"tpch/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }
}
