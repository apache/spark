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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.checkpointing.SequentialUnionOffset

class SequentialUnionOffsetSuite extends SparkFunSuite {

  test("SequentialUnionOffset - creation and basic properties") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source2",
      completedSources = Set("source1"),
      sourceNames = Seq("source1", "source2", "source3"),
      version = 1
    )

    assert(offset.activeSourceName === "source2")
    assert(offset.completedSources === Set("source1"))
    assert(offset.sourceNames === Seq("source1", "source2", "source3"))
    assert(offset.version === 1)
  }

  test("SequentialUnionOffset - JSON serialization and deserialization") {
    val offset = SequentialUnionOffset(
      activeSourceName = "kafka-live",
      completedSources = Set("delta-historical", "delta-backfill"),
      sourceNames = Seq("delta-historical", "delta-backfill", "kafka-live"),
      version = 1
    )

    val json = offset.json()
    val deserialized = SequentialUnionOffset(json)

    assert(deserialized.activeSourceName === offset.activeSourceName)
    assert(deserialized.completedSources === offset.completedSources)
    assert(deserialized.sourceNames === offset.sourceNames)
    assert(deserialized.version === offset.version)
  }

  test("SequentialUnionOffset - JSON roundtrip with special characters") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source-with-dashes_and_underscores",
      completedSources = Set("source.with.dots", "source/with/slashes"),
      sourceNames = Seq("source.with.dots", "source/with/slashes",
        "source-with-dashes_and_underscores"),
      version = 1
    )

    val json = offset.json()
    val deserialized = SequentialUnionOffset(json)

    assert(deserialized === offset)
  }

  test("SequentialUnionOffset - validation: empty sourceNames") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "source1",
        completedSources = Set.empty,
        sourceNames = Seq.empty,
        version = 1
      )
    }
    assert(ex.getMessage.contains("sourceNames must not be empty"))
  }

  test("SequentialUnionOffset - validation: active not in sourceNames") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "nonexistent",
        completedSources = Set.empty,
        sourceNames = Seq("source1", "source2"),
        version = 1
      )
    }
    assert(ex.getMessage.contains("activeSourceName"))
    assert(ex.getMessage.contains("must be in sourceNames"))
  }

  test("SequentialUnionOffset - validation: completedSources not subset") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "source2",
        completedSources = Set("source1", "nonexistent"),
        sourceNames = Seq("source1", "source2", "source3"),
        version = 1
      )
    }
    assert(ex.getMessage.contains("completedSources must be a subset"))
  }

  test("SequentialUnionOffset - validation: active in completed") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "source2",
        completedSources = Set("source1", "source2"),
        sourceNames = Seq("source1", "source2", "source3"),
        version = 1
      )
    }
    assert(ex.getMessage.contains("activeSourceName"))
    assert(ex.getMessage.contains("cannot be in completedSources"))
  }

  test("SequentialUnionOffset - initial state (first source active)") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source1",
      completedSources = Set.empty,
      sourceNames = Seq("source1", "source2", "source3"),
      version = 1
    )

    assert(offset.activeSourceName === "source1")
    assert(offset.completedSources.isEmpty)
  }

  test("SequentialUnionOffset - middle state (second source active)") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source2",
      completedSources = Set("source1"),
      sourceNames = Seq("source1", "source2", "source3"),
      version = 1
    )

    assert(offset.activeSourceName === "source2")
    assert(offset.completedSources === Set("source1"))
  }

  test("SequentialUnionOffset - final source active") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source3",
      completedSources = Set("source1", "source2"),
      sourceNames = Seq("source1", "source2", "source3"),
      version = 1
    )

    assert(offset.activeSourceName === "source3")
    assert(offset.completedSources === Set("source1", "source2"))
    assert(offset.completedSources.size === offset.sourceNames.size - 1)
  }

  test("SequentialUnionOffset - multiple completed sources") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source5",
      completedSources = Set("source1", "source2", "source3", "source4"),
      sourceNames = Seq("source1", "source2", "source3", "source4", "source5",
        "source6"),
      version = 1
    )

    val json = offset.json()
    val deserialized = SequentialUnionOffset(json)

    assert(deserialized.completedSources.size === 4)
    assert(deserialized.completedSources ===
      Set("source1", "source2", "source3", "source4"))
  }

  test("SequentialUnionOffset - version field preserved") {
    val offset1 = SequentialUnionOffset(
      activeSourceName = "source1",
      completedSources = Set.empty,
      sourceNames = Seq("source1", "source2"),
      version = 1
    )

    val offset2 = SequentialUnionOffset(
      activeSourceName = "source1",
      completedSources = Set.empty,
      sourceNames = Seq("source1", "source2"),
      version = 2
    )

    assert(offset1.version === 1)
    assert(offset2.version === 2)

    val json1 = offset1.json()
    val json2 = offset2.json()

    assert(SequentialUnionOffset(json1).version === 1)
    assert(SequentialUnionOffset(json2).version === 2)
  }
}
