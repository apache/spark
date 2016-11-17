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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.hive.execution.UDFLongToString
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveUDFBenchmark extends BenchmarkBase with TestHiveSingleton {

  ignore("HiveSimpleUDF") {
    val N = 2L << 26
    sparkSession.range(N).createOrReplaceTempView("t")
    sparkSession.sql(s"CREATE TEMPORARY FUNCTION f AS '${classOf[UDFLongToString].getName}'")

    /*
     Java HotSpot(TM) 64-Bit Server VM 1.8.0_31-b13 on Mac OS X 10.10.2
     Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz

     Call Hive UDF:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
       -------------------------------------------------------------------------------------------
     Call Hive UDF wholestage off                3 /    4      40941.7           0.0       1.0X
     Call Hive UDF wholestage on                 1 /    2      96620.3           0.0       2.4X
     */
    runBenchmark("Call Hive UDF", N) {
      sparkSession.sql("SELECT f(id) FROM t")
    }
    sparkSession.sql("DROP TEMPORARY FUNCTION IF EXISTS f")
  }
}
