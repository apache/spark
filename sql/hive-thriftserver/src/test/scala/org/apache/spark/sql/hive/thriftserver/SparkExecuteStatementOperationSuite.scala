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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{IntegerType, NullType, StringType, StructField, StructType}

class SparkExecuteStatementOperationSuite extends SparkFunSuite {
  test("SPARK-17112 `select null` via JDBC triggers IllegalArgumentException in ThriftServer") {
    val field1 = StructField("NULL", NullType)
    val field2 = StructField("(IF(true, NULL, NULL))", NullType)
    val tableSchema = StructType(Seq(field1, field2))
    val columns = SparkExecuteStatementOperation.getTableSchema(tableSchema).getColumnDescriptors()
    assert(columns.size() == 2)
    assert(columns.get(0).getType() == org.apache.hive.service.cli.Type.NULL_TYPE)
    assert(columns.get(1).getType() == org.apache.hive.service.cli.Type.NULL_TYPE)
  }

  test("SPARK-20146 Comment should be preserved") {
    val field1 = StructField("column1", StringType).withComment("comment 1")
    val field2 = StructField("column2", IntegerType)
    val tableSchema = StructType(Seq(field1, field2))
    val columns = SparkExecuteStatementOperation.getTableSchema(tableSchema).getColumnDescriptors()
    assert(columns.size() == 2)
    assert(columns.get(0).getType() == org.apache.hive.service.cli.Type.STRING_TYPE)
    assert(columns.get(0).getComment() == "comment 1")
    assert(columns.get(1).getType() == org.apache.hive.service.cli.Type.INT_TYPE)
    assert(columns.get(1).getComment() == "")
  }
}
