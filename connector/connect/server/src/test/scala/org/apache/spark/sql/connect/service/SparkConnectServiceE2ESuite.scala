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
package org.apache.spark.sql.connect.service

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.connect.SparkConnectServerTest

class SparkConnectServiceE2ESuite extends SparkConnectServerTest {

  test("SPARK-45133 query should reach FINISHED state when results are not consumed") {
    withRawBlockingStub { stub =>
      val iter =
        stub.executePlan(buildExecutePlanRequest(buildPlan("select * from range(1000000)")))
      iter.hasNext
      val execution = eventuallyGetExecutionHolder
      Eventually.eventually(timeout(30.seconds)) {
        execution.eventsManager.status == ExecuteStatus.Finished
      }
    }
  }

  test("SPARK-45133 local relation should reach FINISHED state when results are not consumed") {
    withClient { client =>
      val iter = client.execute(buildLocalRelation((1 to 1000000).map(i => (i, i + 1))))
      iter.hasNext
      val execution = eventuallyGetExecutionHolder
      Eventually.eventually(timeout(30.seconds)) {
        execution.eventsManager.status == ExecuteStatus.Finished
      }
    }
  }
}
