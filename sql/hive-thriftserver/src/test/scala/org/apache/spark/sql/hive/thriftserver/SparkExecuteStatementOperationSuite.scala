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

import java.util
import java.util.concurrent.Semaphore

import scala.concurrent.duration._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.OperationState
import org.apache.hive.service.cli.session.{HiveSession, HiveSessionImpl}
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.mockito.Mockito.{doReturn, spy, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{IntegerType, NullType, StringType, StructField, StructType}

class SparkExecuteStatementOperationSuite extends SparkFunSuite with SharedThriftServer {

  override def mode: ServerMode.Value = ServerMode.binary

  test("SPARK-17112 `select null` via JDBC triggers IllegalArgumentException in ThriftServer") {
    val field1 = StructField("NULL", NullType)
    val field2 = StructField("(IF(true, NULL, NULL))", NullType)
    val tableSchema = StructType(Seq(field1, field2))
    val columns = SparkExecuteStatementOperation.getTableSchema(tableSchema).getColumnDescriptors()
    assert(columns.size() == 2)
    assert(columns.get(0).getType().getName == "VOID")
    assert(columns.get(1).getType().getName == "VOID")
  }

  test("SPARK-20146 Comment should be preserved") {
    val field1 = StructField("column1", StringType).withComment("comment 1")
    val field2 = StructField("column2", IntegerType)
    val tableSchema = StructType(Seq(field1, field2))
    val columns = SparkExecuteStatementOperation.getTableSchema(tableSchema).getColumnDescriptors()
    assert(columns.size() == 2)
    assert(columns.get(0).getType().getName == "STRING")
    assert(columns.get(0).getComment() == "comment 1")
    assert(columns.get(1).getType().getName == "INT")
    assert(columns.get(1).getComment() == "")
  }

  test("SPARK-32057 SparkExecuteStatementOperation should not transiently ERROR while cancelling") {
    val hiveSession = new HiveSessionImpl(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      "username", "password", new HiveConf(), "ip address")
    hiveSession.open(new util.HashMap[String, String]())
    val spySqlContext = spy(sqlContext)

    // When cancel() is called on the operation, cleanup causes an exception to be thrown inside of
    // execute(). This should not cause the state to become ERROR. The exception here will be
    // triggered in our custom cleanup().
    val signal = new Semaphore(0)
    val dataFrame = mock[DataFrame]
    when(dataFrame.collect()).thenAnswer((_: InvocationOnMock) => {
      signal.acquire()
      throw new RuntimeException("Operation was cancelled")
    })
    when(dataFrame.queryExecution).thenReturn(mock[QueryExecution])
    val statement = "stmt"
    doReturn(dataFrame, Nil: _*).when(spySqlContext).sql(statement)

    val executeStatementOperation = new MySparkExecuteStatementOperation(spySqlContext, hiveSession,
      statement, signal)

    val run = new Thread() {
      override def run(): Unit = executeStatementOperation.runInternal()
    }
    assert(executeStatementOperation.getStatus.getState === OperationState.INITIALIZED)
    run.start()
    eventually(timeout(5.seconds)) {
      assert(executeStatementOperation.getStatus.getState === OperationState.RUNNING)
    }
    executeStatementOperation.cancel()
    run.join()
    assert(executeStatementOperation.getStatus.getState === OperationState.CANCELED)
  }

  private class MySparkExecuteStatementOperation(
      sqlContext: SQLContext,
      hiveSession: HiveSession,
      statement: String,
      signal: Semaphore)
    extends SparkExecuteStatementOperation(sqlContext, hiveSession, statement,
      new util.HashMap[String, String](), false) {

    override def cleanup(): Unit = {
      super.cleanup()
      signal.release()
      // Allow time for the exception to propagate.
      Thread.sleep(1000)
      // State should not be ERROR
      assert(getStatus.getState === OperationState.CANCELED)
    }
  }
}
