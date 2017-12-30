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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.HiveUtils.{CONVERT_METASTORE_ORC, CONVERT_METASTORE_PARQUET}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.sources.CompressionTestUtils

class HiveCompressionSuite extends CompressionTestUtils with TestHiveSingleton {

  test("CTAS after converting hive table to data source table - parquet") {
    withSQLConf(CONVERT_METASTORE_PARQUET.key -> "true") {
      checkCTASCompression(
        formatClause = "STORED AS parquet",
        optionClause = "TBLPROPERTIES('compression' = 'gzip')",
        expectedFileNameSuffix = "gz.parquet"
      )
    }
  }

  test("INSERT after converting hive table to data source table - parquet") {
    withSQLConf(CONVERT_METASTORE_PARQUET.key -> "true") {
      checkInsertCompression(
        format = "parquet",
        isNative = false,
        optionClause = "",
        tablePropertiesClause = "TBLPROPERTIES('compression' = 'gzip')",
        isPartitioned = false,
        expectedFileNameSuffix = "gz.parquet"
      )
    }
  }

  test("CTAS after converting hive table to data source table - orc") {
    withSQLConf(CONVERT_METASTORE_ORC.key -> "true") {
      checkCTASCompression(
        formatClause = "STORED AS orc",
        optionClause = "TBLPROPERTIES('compression' = 'zlib')",
        expectedFileNameSuffix = "zlib.orc"
      )
    }
  }

  test("INSERT after converting hive table to data source table - orc") {
    withSQLConf(CONVERT_METASTORE_ORC.key -> "true") {
      checkInsertCompression(
        format = "orc",
        isNative = false,
        optionClause = "",
        tablePropertiesClause = "TBLPROPERTIES('compression' = 'zlib')",
        isPartitioned = false,
        expectedFileNameSuffix = "zlib.orc"
      )
    }
  }
}
