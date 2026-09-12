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

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.types.{DateType, StructType, TimestampNTZType, TimestampType}

class JDBCRelationSuite extends SparkFunSuite {

  private val options = Map(
    "url" -> "jdbc:h2:mem:jdbcRelation",
    "driver" -> "org.h2.Driver",
    "dbtable" -> "test",
    "partitionColumn" -> "t",
    "lowerBound" -> "2020-01-01",
    "upperBound" -> "2020-01-03",
    "numPartitions" -> "2")

  for {
    dataType <- Seq(DateType, TimestampType, TimestampNTZType)
    bound <- Seq("lowerBound", "upperBound")
    value <- Seq("{bound}", "", "2020-02-30")
  } {
    test(s"invalid $bound for ${dataType.sql}: '$value'") {
      val jdbcOptions = new JDBCOptions(options.updated(bound, value))
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          JDBCRelation.columnPartition(
            new StructType().add("t", dataType), caseSensitiveResolution, "UTC", jdbcOptions)
        },
        condition = "INVALID_JDBC_PARTITION_BOUND",
        sqlState = Some("42616"),
        parameters = Map(
          "option" -> s""""$bound"""",
          "value" -> s""""$value"""",
          "dataType" -> s""""${dataType.sql}""""))
    }
  }

  for (dataType <- Seq(DateType, TimestampType, TimestampNTZType)) {
    test(s"valid ${dataType.sql} bounds generate partition predicates") {
      val partitions = JDBCRelation.columnPartition(
        new StructType().add("t", dataType),
        caseSensitiveResolution,
        "UTC",
        new JDBCOptions(options))
      val midpoint = if (dataType == DateType) "2020-01-02" else "2020-01-02 00:00:00"
      assert(partitions.map(_.asInstanceOf[JDBCPartition].whereClause).toSeq === Seq(
        s""""t" < '$midpoint' or "t" is null""",
        s""""t" >= '$midpoint'"""))
    }
  }
}
