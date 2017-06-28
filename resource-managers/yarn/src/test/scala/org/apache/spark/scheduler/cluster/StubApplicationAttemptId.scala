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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}

/**
 * A stub application ID; can be set in constructor and/or updated later.
 * @param applicationId application ID
 * @param attempt an attempt counter
 */
class StubApplicationAttemptId(var applicationId: ApplicationId, var attempt: Int)
    extends ApplicationAttemptId {

  override def setApplicationId(appID: ApplicationId): Unit = {
    applicationId = appID
  }

  override def getAttemptId: Int = {
    attempt
  }

  override def setAttemptId(attemptId: Int): Unit = {
    attempt = attemptId
  }

  override def getApplicationId: ApplicationId = {
    applicationId
  }

  override def build(): Unit = {
  }
}
