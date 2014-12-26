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

package org.apache.spark.executor

import org.apache.spark.util.ThreadStackTrace
import org.apache.spark.{LocalSparkContext, SparkContext}
import org.scalatest.FunSuite

class ExecutorActorSuite extends FunSuite with LocalSparkContext {

   test("ExecutorActor") {
     sc = new SparkContext("local[2]", "test")
     sc.env.rpcEnv.setupEndpoint("executor-actor", new ExecutorActor(sc.env.rpcEnv, "executor-1"))
     val receiverRef = sc.env.rpcEnv.setupDriverEndpointRef("executor-actor")
     val response = receiverRef.askWithReply[Array[ThreadStackTrace]](TriggerThreadDump)
     assert(response.size > 0)
   }
 }
