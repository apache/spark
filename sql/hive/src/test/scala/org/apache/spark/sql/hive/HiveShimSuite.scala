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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils

import org.apache.spark.SparkFunSuite

class HiveShimSuite extends SparkFunSuite {

  test("appendReadColumns") {
    val conf = new Configuration
    val ids = Seq(1, 2, 3).map(Int.box)
    val names = Seq("a", "b", "c")
    val moreIds = Seq(4, 5).map(Int.box)
    val moreNames = Seq("d", "e")

    // test when READ_COLUMN_NAMES_CONF_STR is empty
    HiveShim.appendReadColumns(conf, ids, names)
    assert(names === ColumnProjectionUtils.getReadColumnNames(conf))

    // test when READ_COLUMN_NAMES_CONF_STR is non-empty
    HiveShim.appendReadColumns(conf, moreIds, moreNames)
    assert((names ++ moreNames) === ColumnProjectionUtils.getReadColumnNames(conf))
  }
}
