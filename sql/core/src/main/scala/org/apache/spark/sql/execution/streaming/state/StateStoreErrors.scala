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

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}

/**
 * Object for grouping error messages from (most) exceptions thrown from State API V2
 *
 * ERROR_CLASS has a prefix of "STV2_" representing State API V2.
 */
class StateStoreErrors {
  def implicitKeyNotFound(
      stateName: String): StateV2ImplicitKeyNotFound = {
    new StateV2ImplicitKeyNotFound(stateName)
  }

  def encoderPrefixKey(): StateV2EncoderPrefixKey = {
    new StateV2EncoderPrefixKey()
  }

  def multipleValuesPerKey(): StateV2MultipleValuesPerKey = {
    new StateV2MultipleValuesPerKey()
  }

  def valueShouldBeNonNull(typeOfState: String): StateV2ValueShouldBeNonNull = {
    new StateV2ValueShouldBeNonNull(typeOfState)
  }
}
class StateV2ImplicitKeyNotFound(stateName: String)
  extends SparkRuntimeException(
    "STV2_IMPLICIT_KEY_NOT_FOUND",
    Map("stateName" -> stateName),
    cause = null
  )
class StateV2EncoderPrefixKey()
  extends SparkUnsupportedOperationException(
    "STV2_ENCODER_UNSUPPORTED_PREFIX_KEY",
    Map.empty
  )

class StateV2MultipleValuesPerKey()
  extends SparkUnsupportedOperationException(
    "STV2_STORE_MULTIPLE_VALUES_PER_KEY",
    Map.empty
  )

class StateV2ValueShouldBeNonNull(typeOfState: String)
  extends SparkRuntimeException(
    "STV2_VALUE_SHOULD_BE_NONNULL",
    Map("typeOfState" -> typeOfState),
    cause = null
  )
