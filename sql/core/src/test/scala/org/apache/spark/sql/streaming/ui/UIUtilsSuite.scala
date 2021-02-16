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

package org.apache.spark.sql.streaming.ui

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.StreamingQueryProgress

class UIUtilsSuite extends SparkFunSuite with Matchers {
  test("streaming query started with no batch completed") {
    val query = mock(classOf[StreamingQueryUIData], RETURNS_SMART_NULLS)
    when(query.lastProgress).thenReturn(null)

    assert(0 == UIUtils.withNoProgress(query, 1, 0))
  }

  test("streaming query started with at least one batch completed") {
    val query = mock(classOf[StreamingQueryUIData], RETURNS_SMART_NULLS)
    val progress = mock(classOf[StreamingQueryProgress], RETURNS_SMART_NULLS)
    when(query.lastProgress).thenReturn(progress)

    assert(1 == UIUtils.withNoProgress(query, 1, 0))
  }
}
