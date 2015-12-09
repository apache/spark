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

package org.apache.spark.streaming.api.java

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.dstream.TrackStateDStream

/**
 * :: Experimental ::
 * [[JavaDStream]] representing the stream of records emitted by the tracking function in the
 * `trackStateByKey` operation on a [[JavaPairDStream]]. Additionally, it also gives access to the
 * stream of state snapshots, that is, the state data of all keys after a batch has updated them.
 *
 * @tparam KeyType Class of the state key
 * @tparam ValueType Class of the state value
 * @tparam StateType Class of the state
 * @tparam EmittedType Class of the emitted records
 */
@Experimental
class JavaTrackStateDStream[KeyType, ValueType, StateType, EmittedType](
    dstream: TrackStateDStream[KeyType, ValueType, StateType, EmittedType])
  extends JavaDStream[EmittedType](dstream)(JavaSparkContext.fakeClassTag) {

  def stateSnapshots(): JavaPairDStream[KeyType, StateType] =
    new JavaPairDStream(dstream.stateSnapshots())(
      JavaSparkContext.fakeClassTag,
      JavaSparkContext.fakeClassTag)
}
