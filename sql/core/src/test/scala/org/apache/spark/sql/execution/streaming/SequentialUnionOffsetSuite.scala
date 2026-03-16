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

/**
 * Test suite for [[SequentialUnionOffset]], which tracks the state of sequential source
 * processing in streaming queries. Tests cover offset creation, JSON serialization/deserialization,
 * parameter validation, and state transitions through the sequential processing lifecycle.
 */
class SequentialUnionOffsetSuite extends SparkFunSuite {

  test("SequentialUnionOffset - creation and basic properties") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source2",
      allSourceNames = Seq("source1", "source2", "source3"),
      completedSourceNames = Set("source1")
    )

    assert(offset.activeSourceName === "source2")
    assert(offset.allSourceNames === Seq("source1", "source2", "source3"))
    assert(offset.completedSourceNames === Set("source1"))
  }

  test("SequentialUnionOffset - JSON serialization and deserialization") {
    val offset = SequentialUnionOffset(
      activeSourceName = "kafka-live",
      allSourceNames = Seq("delta-historical", "delta-backfill", "kafka-live"),
      completedSourceNames = Set("delta-historical", "delta-backfill")
    )

    val json = offset.json()
    val deserialized = SequentialUnionOffset(json)

    assert(deserialized.activeSourceName === offset.activeSourceName)
    assert(deserialized.allSourceNames === offset.allSourceNames)
    assert(deserialized.completedSourceNames === offset.completedSourceNames)
  }

  test("SequentialUnionOffset - JSON roundtrip with special characters") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source-with-dashes_and_underscores",
      allSourceNames = Seq("source.with.dots", "source/with/slashes",
        "source-with-dashes_and_underscores"),
      completedSourceNames = Set("source.with.dots", "source/with/slashes")
    )

    val json = offset.json()
    val deserialized = SequentialUnionOffset(json)

    assert(deserialized === offset)
  }

  test("SequentialUnionOffset - validation: empty sourceNames") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "source1",
        allSourceNames = Seq.empty,
        completedSourceNames = Set.empty
      )
    }
    assert(ex.getMessage.contains("allSourceNames must not be empty"))
  }

  test("SequentialUnionOffset - validation: active not in sourceNames") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "nonexistent",
        allSourceNames = Seq("source1", "source2"),
        completedSourceNames = Set.empty
      )
    }
    assert(ex.getMessage.contains("activeSourceName"))
    assert(ex.getMessage.contains("must be in allSourceNames"))
  }

  test("SequentialUnionOffset - validation: completedSources not subset") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "source2",
        allSourceNames = Seq("source1", "source2", "source3"),
        completedSourceNames = Set("source1", "nonexistent")
      )
    }
    assert(ex.getMessage.contains("completedSourceNames must be a subset"))
  }

  test("SequentialUnionOffset - validation: active in completed") {
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "source2",
        allSourceNames = Seq("source1", "source2", "source3"),
        completedSourceNames = Set("source1", "source2")
      )
    }
    assert(ex.getMessage.contains("activeSourceName"))
    assert(ex.getMessage.contains("cannot be in completedSourceNames"))
  }

  test("SequentialUnionOffset - initial state (first source active)") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source1",
      allSourceNames = Seq("source1", "source2", "source3"),
      completedSourceNames = Set.empty
    )

    assert(offset.activeSourceName === "source1")
    assert(offset.completedSourceNames.isEmpty)
  }

  test("SequentialUnionOffset - middle state (second source active)") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source2",
      allSourceNames = Seq("source1", "source2", "source3"),
      completedSourceNames = Set("source1")
    )

    assert(offset.activeSourceName === "source2")
    assert(offset.completedSourceNames === Set("source1"))
  }

  test("SequentialUnionOffset - final source active") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source3",
      allSourceNames = Seq("source1", "source2", "source3"),
      completedSourceNames = Set("source1", "source2")
    )

    assert(offset.activeSourceName === "source3")
    assert(offset.completedSourceNames === Set("source1", "source2"))
    assert(offset.completedSourceNames.size === offset.allSourceNames.size - 1)
  }

  test("SequentialUnionOffset - multiple completed sources") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source5",
      allSourceNames = Seq("source1", "source2", "source3", "source4", "source5",
        "source6"),
      completedSourceNames = Set("source1", "source2", "source3", "source4")
    )

    val json = offset.json()
    val deserialized = SequentialUnionOffset(json)

    assert(deserialized.completedSourceNames.size === 4)
    assert(deserialized.completedSourceNames ===
      Set("source1", "source2", "source3", "source4"))
  }
}
