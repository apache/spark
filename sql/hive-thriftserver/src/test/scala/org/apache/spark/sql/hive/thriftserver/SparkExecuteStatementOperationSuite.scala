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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class SparkExecuteStatementOperationSuite
  extends SparkFunSuite with BeforeAndAfterAll with Logging {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder
      .master("local[1]")
      .appName("SparkExecuteStatementOperationSuite")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-17112 `select null` via JDBC triggers IllegalArgumentException in ThriftServer") {
    val df = spark.sql("select null, if(true,null,null)")
    val columns = SparkExecuteStatementOperation.getTableSchema(df).getColumnDescriptors()
    assert(columns.size() == 2)
    assert(columns.get(0).getType() == org.apache.hive.service.cli.Type.NULL_TYPE)
    assert(columns.get(1).getType() == org.apache.hive.service.cli.Type.NULL_TYPE)
  }
}
