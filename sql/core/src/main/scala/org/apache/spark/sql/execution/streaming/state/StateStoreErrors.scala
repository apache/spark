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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.SparkRuntimeException

/**
 * Object for grouping error messages from (most) exceptions thrown from State API V2
 *
 * ERROR_CLASS has a prefix of "STV2_" representing State API V2.
 */
class StateStoreErrors {
  def implicitKeyNotFound(
      stateName: String): TransformWithStateImplicitKeyNotFound = {
    new TransformWithStateImplicitKeyNotFound(stateName)
  }

  def encoderPrefixKey(stateStoreEncoder: String): TransformWithStateEncoderPrefixKey = {
    new TransformWithStateEncoderPrefixKey(stateStoreEncoder)
  }

  def multipleValuesPerKey(): TransformWithStateMultipleValuesPerKey = {
    new TransformWithStateMultipleValuesPerKey()
  }

  def valueShouldBeNonNull(typeOfState: String): TransformWithStateValueShouldBeNonNull = {
    new TransformWithStateValueShouldBeNonNull(typeOfState)
  }
}
class TransformWithStateImplicitKeyNotFound(stateName: String)
  extends SparkRuntimeException(
    errorClass = "TWS_IMPLICIT_KEY_NOT_FOUND",
    messageParameters = Map("stateName" -> stateName),
    cause = null
  )
class TransformWithStateEncoderPrefixKey(stateStoreEncoder: String)
  extends SparkRuntimeException(
    errorClass = "TWS_ENCODER_UNSUPPORTED_PREFIX_KEY",
    messageParameters = Map("stateStoreEncoder" -> stateStoreEncoder)
  )

class TransformWithStateMultipleValuesPerKey()
  extends SparkRuntimeException(
    errorClass = "TWS_STORE_MULTIPLE_VALUES_PER_KEY",
    messageParameters = Map.empty
  )

class TransformWithStateValueShouldBeNonNull(typeOfState: String)
  extends SparkRuntimeException(
    errorClass = "TWS_VALUE_SHOULD_BE_NONNULL",
    Map("typeOfState" -> typeOfState),
    cause = null
  )
