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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore

class MapGroupsWithStateSuite extends StreamTest with BeforeAndAfterAll {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("basics") {
    val inputData = MemoryStream[String]

    val stateFunc = (data: String, state: State[Int]) => {
      val count = state.getOption().getOrElse(0) + 1
      state.update(count)
      (data, count.toString)
    }
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState[Int, (String, String)](stateFunc)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      AddData(inputData, "a"),
      CheckLastBatch(("a", "2"))
    )
  }
}
