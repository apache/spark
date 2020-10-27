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
import org.mockito.Mockito.{doReturn, mock, spy, when, RETURNS_DEEP_STUBS}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.thriftserver.ui.HiveThriftServer2EventManager
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, NullType, StringType, StructField, StructType}

class SparkExecuteStatementOperationSuite extends SparkFunSuite with SharedSparkSession {

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

  Seq(
    (OperationState.CANCELED, (_: SparkExecuteStatementOperation).cancel()),
    (OperationState.TIMEDOUT, (_: SparkExecuteStatementOperation).timeoutCancel()),
    (OperationState.CLOSED, (_: SparkExecuteStatementOperation).close())
  ).foreach { case (finalState, transition) =>
    test("SPARK-32057 SparkExecuteStatementOperation should not transiently become ERROR " +
      s"before being set to $finalState") {
      val hiveSession = new HiveSessionImpl(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      "username", "password", new HiveConf, "ip address")
      hiveSession.open(new util.HashMap)

      HiveThriftServer2.eventManager = mock(classOf[HiveThriftServer2EventManager])

      val spySqlContext = spy(sqlContext)

      // When cancel() is called on the operation, cleanup causes an exception to be thrown inside
      // of execute(). This should not cause the state to become ERROR. The exception here will be
      // triggered in our custom cleanup().
      val signal = new Semaphore(0)
      val dataFrame = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
      when(dataFrame.collect()).thenAnswer((_: InvocationOnMock) => {
        signal.acquire()
        throw new RuntimeException("Operation was cancelled by test cleanup.")
      })
      val statement = "stmt"
      doReturn(dataFrame, Nil: _*).when(spySqlContext).sql(statement)

      val executeStatementOperation = new MySparkExecuteStatementOperation(spySqlContext,
        hiveSession, statement, signal, finalState)

      val run = new Thread() {
        override def run(): Unit = executeStatementOperation.runInternal()
      }
      assert(executeStatementOperation.getStatus.getState === OperationState.INITIALIZED)
      run.start()
      eventually(timeout(5.seconds)) {
        assert(executeStatementOperation.getStatus.getState === OperationState.RUNNING)
      }
      transition(executeStatementOperation)
      run.join()
      assert(executeStatementOperation.getStatus.getState === finalState)
    }
  }

  private class MySparkExecuteStatementOperation(
      sqlContext: SQLContext,
      hiveSession: HiveSession,
      statement: String,
      signal: Semaphore,
      finalState: OperationState)
    extends SparkExecuteStatementOperation(sqlContext, hiveSession, statement,
      new util.HashMap, false, 0) {

    override def cleanup(): Unit = {
      super.cleanup()
      signal.release()
      // At this point, operation should already be in finalState (set by either close() or
      // cancel()). We want to check if it stays in finalState after the exception thrown by
      // releasing the semaphore propagates. We hence need to sleep for a short while.
      Thread.sleep(1000)
      // State should not be ERROR
      assert(getStatus.getState === finalState)
    }
  }
}
