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

package org.apache.spark.sql.connect.planner

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto

/*
 * The [[MockObserver]] class is a mock of the [[StreamObserver]] class from
 * the gRPC library, used for testing the process function of [[SparkConnectPlanner]].
 *
 * Currently, the [[MockObserver]] class is located in the test folder,
 * which is not shaded by sbt-assembly, unlike the main folder.
 *
 * This results in a compilation error when open-source libraries call the
 * [[transform(spark: SparkSession, command: proto.Command)]] function in
 * [[SparkConnectPlannerTestUtils]], which in turn calls
 * [[SparkConnectPlanner(executeHolder).process(command, new MockObserver())]].
 *
 * To resolve this, the [[MockObserver]] class should be moved to the main folder
 * to be shaded as well.
 */
private[connect] class MockObserver extends StreamObserver[proto.ExecutePlanResponse] {
  override def onNext(value: proto.ExecutePlanResponse): Unit = {}
  override def onError(t: Throwable): Unit = {}
  override def onCompleted(): Unit = {}
}
