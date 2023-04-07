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

package org.apache.spark

import java.util.{Collections => JCollections}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, mock, when}

import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents}

/**
 * A test shuffle data IO implementation, which allows both executor and driver component to
 * be mocked.
 * Note: cannot intercept initialization of executor/driver currently.
 *
 */
class TestShuffleDataIOWithMockedComponents(val conf: SparkConf) extends ShuffleDataIO {

  // ShuffleDataIO must be initialized only after spark.app.id has been configured
  assert(conf.getOption("spark.app.id").isDefined)

  private val executorMock = {
    val m = mock(classOf[ShuffleExecutorComponents])
    doNothing().when(m).initializeExecutor(any(), any(), any())
    m
  }

  private val driverMock = {
    val m = mock(classOf[ShuffleDriverComponents])
    when(m.initializeApplication()).thenReturn(JCollections.emptyMap[String, String]())
    m
  }

  def executor: ShuffleExecutorComponents = executorMock

  def driver: ShuffleDriverComponents = driverMock
}
