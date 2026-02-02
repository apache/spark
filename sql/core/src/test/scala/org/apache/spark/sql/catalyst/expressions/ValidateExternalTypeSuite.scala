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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class ValidateExternalTypeSuite extends QueryTest with SharedSparkSession {
  test("SPARK-49044 ValidateExternalType should be user visible") {
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(
            "".toCharArray.map(_.toByte)
          )
        )), new StructType().add("f3", StringType)).show()
      }.getCause.asInstanceOf[SparkRuntimeException],
      condition = "INVALID_EXTERNAL_TYPE",
      parameters = Map(
        ("externalType", "[B"),
        ("type", "\"STRING\""),
        ("expr", "\"getexternalrowfield(assertnotnull(" +
          "input[0, org.apache.spark.sql.Row, true]), 0, f3)\"")
      )
    )
  }
}
