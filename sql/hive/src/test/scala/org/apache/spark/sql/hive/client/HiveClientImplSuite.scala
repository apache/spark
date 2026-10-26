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
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, CharType, MapType, StringType, StructField, StructType, TimestampType, VarcharType}

class HiveClientImplSuite extends SparkFunSuite {

  test("SPARK-60424: Hive timestamp type is TIMESTAMP_LTZ regardless of " +
      "spark.sql.timestampType") {
    val column = new FieldSchema(
      "c", "struct<a:timestamp,b:array<timestamp>,c:map<timestamp,timestamp>>", null)
    val conf = new SQLConf()
    conf.setConf(SQLConf.TIMESTAMP_TYPE, SQLConf.TimestampTypes.TIMESTAMP_NTZ)
    SQLConf.withExistingConf(conf) {
      assert(HiveClientImpl.fromHiveColumn(column).dataType === new StructType()
        .add("a", TimestampType)
        .add("b", ArrayType(TimestampType))
        .add("c", MapType(TimestampType, TimestampType)))
    }
  }

  test("SPARK-60565: toHiveColumn removes collations") {
    Seq(
      StringType("UNICODE_CI") -> "string",
      StringType("UTF8_BINARY") -> "string",
      ArrayType(StringType("UNICODE_CI")) -> "array<string>",
      new StructType().add("f", StringType("UTF8_LCASE")) -> "struct<f:string>",
      MapType(StringType, StringType("UNICODE_CI")) -> "map<string,string>",
      CharType(3, "UTF8_LCASE") -> "char(3)",
      VarcharType(3, "UNICODE_CI") -> "varchar(3)"
    ).foreach { case (dataType, typeString) =>
      assert(HiveClientImpl.toHiveColumn(StructField("c", dataType)).getType === typeString)
    }

    // The raw CHAR/VARCHAR type string in the field metadata keeps the collations. Removing them
    // must not depend on spark.sql.timestampType.
    val field = CharVarcharUtils.replaceCharVarcharWithStringInSchema(new StructType()
      .add("c", new StructType().add("a", CharType(3, "UTF8_LCASE")).add("b", TimestampType)))
      .head
    val conf = new SQLConf()
    conf.setConf(SQLConf.TIMESTAMP_TYPE, SQLConf.TimestampTypes.TIMESTAMP_NTZ)
    SQLConf.withExistingConf(conf) {
      assert(HiveClientImpl.toHiveColumn(field).getType === "struct<a:char(3),b:timestamp>")
    }
  }
}
