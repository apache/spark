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

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.QueryTest

class HiveDataFrameSuite extends QueryTest with TestHiveSingleton {
  test("table name with schema") {
    // regression test for SPARK-11778
    spark.sql("create schema usrdb")
    spark.sql("create table usrdb.test(c int)")
    spark.read.table("usrdb.test")
    spark.sql("drop table usrdb.test")
    spark.sql("drop schema usrdb")
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val hiveClient = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
    assert(hiveClient.getConf("hive.in.test", "") == "true")
  }
}
