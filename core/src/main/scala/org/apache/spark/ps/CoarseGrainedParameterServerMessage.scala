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

package org.apache.spark.ps

/**
 * CoarseGrainedParameterServerMessage
 */
private[spark] sealed trait CoarseGrainedParameterServerMessage extends Serializable

private[spark] object CoarseGrainedParameterServerMessage {

  case class SetParameter(key: String, value: Array[Double], clock: Int)
    extends CoarseGrainedParameterServerMessage

  case class GetParameter(key: String, clock: Int)
    extends CoarseGrainedParameterServerMessage

  case class UpdateParameter(key: String, value: Array[Double], clock: Int)
    extends CoarseGrainedParameterServerMessage

  case class Parameter(success: Boolean, value: Array[Double])
    extends CoarseGrainedParameterServerMessage

  case class UpdateClock(clientId: String, clock: Int)
    extends CoarseGrainedParameterServerMessage

  case class NotifyServer(executorUrl: String)
    extends CoarseGrainedParameterServerMessage

  case class InitPSClient(clientId: String)
    extends CoarseGrainedParameterServerMessage

  case class NotifyClient(message: String)
   extends CoarseGrainedParameterServerMessage

}
