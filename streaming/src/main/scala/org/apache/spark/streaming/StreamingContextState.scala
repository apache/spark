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

package org.apache.spark.streaming

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Represents the state of the StreamingContext.
 */
@DeveloperApi
class StreamingContextState private (enumValue: Int) {

  override def hashCode: Int = enumValue

  override def equals(other: Any): Boolean = {
    other match {
      case otherState: StreamingContextState =>
        otherState.hashCode == this.hashCode
      case _ =>
        false
    }
  }
}

/**
 * :: DeveloperApi ::
 *
 * Object enumerating all the states that a StreamingContext can be.
 */
@DeveloperApi
object StreamingContextState {
  /** State representing that the StreamingContext has been initialized and ready for setup */
  val INITIALIZED = new StreamingContextState(0)

  /** State representing that the StreamingContext has been started after setting it up */
  val STARTED = new StreamingContextState(1)

  /** State representing that the StreamingContext has been stopped and cannot be used any more*/
  val STOPPED = new StreamingContextState(2)
}
