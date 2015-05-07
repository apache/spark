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

package org.apache.spark.sql.hive

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class HiveContextInitSuite extends FunSuite {
  test("SPARK-6675 Hive setConf") {
    val hc = new HiveContext(TestHive.sparkContext)
    hc.setConf("hive.metastore.warehouse.dir", "/home/spark/hive/warehouse_test")
    hc.setConf("spark.sql.shuffle.partitions", "10")
    assert(hc.getAllConfs.get("hive.metastore.warehouse.dir")
       .toList.contains("/home/spark/hive/warehouse_test"))
    assert(hc.getAllConfs.get("spark.sql.shuffle.partitions").toList.contains("10"))
  }
}
