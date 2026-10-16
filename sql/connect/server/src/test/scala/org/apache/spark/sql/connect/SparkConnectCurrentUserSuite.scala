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
package org.apache.spark.sql.connect

import java.util.UUID

import org.apache.spark.connect.proto

class SparkConnectCurrentUserSuite extends SparkConnectServerTest {

  test("SPARK-56891: ExecutePlan uses the session user for SQL user functions") {
    Seq("connect-user-a", "connect-user-b", "connect-user-a").foreach { user =>
      withSession(sessionId = UUID.randomUUID().toString, userId = user) { session =>
        val rows = session.sql("SELECT user(), current_user(), session_user()").collect()
        assert(rows.length == 1)
        assert(rows.head.toSeq == Seq.fill(3)(user))
      }
    }
  }

  test("SPARK-56891: AnalyzePlan uses the session user for SQL user functions") {
    withRawBlockingStub { stub =>
      Seq("connect-user-a", "connect-user-b", "connect-user-a").foreach { user =>
        val request = proto.AnalyzePlanRequest.newBuilder()
          .setSessionId(UUID.randomUUID().toString)
          .setUserContext(proto.UserContext.newBuilder().setUserId(user))
          .setExplain(proto.AnalyzePlanRequest.Explain.newBuilder()
            .setPlan(buildPlan(
              "SELECT user() AS u, current_user() AS c, session_user() AS s"))
            .setExplainMode(proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED))
          .build()
        val explanation = stub.analyzePlan(request).getExplain.getExplainString
        val optimized = explanation.split("== Optimized Logical Plan ==")(1)
          .split("== Physical Plan ==")(0)
        Seq("u", "c", "s").foreach { alias =>
          assert(optimized.contains(s"$user AS $alias#"), explanation)
        }
      }
    }
  }
}
