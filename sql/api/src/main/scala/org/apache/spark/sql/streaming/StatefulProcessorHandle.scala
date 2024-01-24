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
package org.apache.spark.sql.streaming

import java.io.Serializable

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.Encoder

/**
 * Represents the operation handle provided to the stateful processor used in the
 * arbitrary state API v2.
 */
@Experimental
@Evolving
private[sql] trait StatefulProcessorHandle extends Serializable {

  /**
   * Function to create new or return existing single value state variable of given type
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor.
   * @param stateName - name of the state variable
   * @param keyEncoder - Spark SQL Encoder for key
   * @tparam K - type of key
   * @tparam T - type of state variable
   * @return - instance of ValueState of type T that can be used to store state persistently
   */
  def getValueState[K, T](stateName: String, keyEncoder: Encoder[K]): ValueState[T]

  /** Function to return queryInfo for currently running task */
  def getQueryInfo(): QueryInfo
}
