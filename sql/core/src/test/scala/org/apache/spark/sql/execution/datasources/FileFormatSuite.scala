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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class FileFormatSuite extends SparkFunSuite {

  test("default file filtering") {
    val defaultPathFilter = PathFilter.defaultPathFilter
    assert(defaultPathFilter.accept(new Path("abcd")))
    assert(!defaultPathFilter.accept(new Path(".ab")))
    assert(!defaultPathFilter.accept(new Path("_cd")))
    assert(!defaultPathFilter.accept(new Path("a._COPYING_")))
  }

  test("parquet file filtering") {
    val parquetPathFilter = new ParquetFileFormat().getPathFilter(Map.empty)
    assert(parquetPathFilter.accept(new Path("abcd")))
    assert(!parquetPathFilter.accept(new Path(".ab")))
    assert(!parquetPathFilter.accept(new Path("_cd")))
    assert(!parquetPathFilter.accept(new Path("a._COPYING_")))
    assert(parquetPathFilter.accept(new Path("_metadata")))
    assert(parquetPathFilter.accept(new Path("_common_metadata")))
    assert(!parquetPathFilter.accept(new Path("_ab_metadata")))
    assert(!parquetPathFilter.accept(new Path("_cd_common_metadata")))
  }
}
