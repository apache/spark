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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructType, TimestampType}

class HiveClientImplSuite extends SparkFunSuite {

  test("SPARK-60424: Hive timestamp type is TIMESTAMP_LTZ regardless of " +
      "spark.sql.timestampType") {
    val column = new FieldSchema(
      "c", "struct<a:timestamp,b:array<timestamp>,c:map<timestamp,timestamp>>", null)
    val conf = new SQLConf()
    conf.setConf(SQLConf.TIMESTAMP_TYPE, SQLConf.TimestampTypes.TIMESTAMP_NTZ.toString)
    SQLConf.withExistingConf(conf) {
      assert(HiveClientImpl.fromHiveColumn(column).dataType === new StructType()
        .add("a", TimestampType)
        .add("b", ArrayType(TimestampType))
        .add("c", MapType(TimestampType, TimestampType)))
    }
  }
}
