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

import java.util.UUID

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.StreamQueryStore

class StreamQueryStoreSuite extends SparkFunSuite with Matchers {
  test("cache streaming query") {
    val store = new StreamQueryStore
    val query1 = mock(classOf[StreamingQuery], RETURNS_SMART_NULLS)
    val query2 = mock(classOf[StreamingQuery], RETURNS_SMART_NULLS)
    val id1 = UUID.randomUUID()
    when(query1.id).thenReturn(id1)
    when(query2.id).thenReturn(id1)
    when(query1.name).thenReturn("query1")
    when(query2.name).thenReturn("query2")
    store.addStreamQuery(query1)
    store.addStreamQuery(query2)

    assert(store.existingStreamQueries.size === 1)
    assert(store.existingStreamQueries.map(_._1.name).sorted === Seq("query1"))

    val id2 = UUID.randomUUID()
    when(query2.id).thenReturn(id2)
    store.addStreamQuery(query2)
    assert(store.existingStreamQueries.size === 2)
    assert(store.existingStreamQueries.map(_._1.name).sorted === Seq("query1", "query2"))
  }
}
