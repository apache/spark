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

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, SequentialStreamingUnion}
import org.apache.spark.sql.connector.read.streaming.{SparkDataStream, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.checkpointing.SequentialUnionOffset
import org.apache.spark.sql.types.IntegerType

/**
 * Test suite for [[SequentialUnionManager]], which manages the lifecycle and state
 * transitions of sequential source processing in streaming queries.
 */
class SequentialUnionManagerSuite extends SparkFunSuite with MockitoSugar {

  /**
   * Helper to create a mock SparkDataStream with SupportsTriggerAvailableNow.
   */
  private def createMockSource(name: String): SparkDataStream with SupportsTriggerAvailableNow = {
    val source = mock[SparkDataStream with SupportsTriggerAvailableNow]
    when(source.toString).thenReturn(name)
    source
  }

  /**
   * Helper to create a SequentialStreamingUnion with the specified number of children.
   */
  private def createSequentialUnion(numChildren: Int): SequentialStreamingUnion = {
    val children = (1 to numChildren).map { i =>
      LocalRelation(Seq(AttributeReference("id", IntegerType)()), isStreaming = true)
    }
    SequentialStreamingUnion(children, byName = false, allowMissingCol = false)
  }

  test("SequentialUnionManager - initialization with valid inputs") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    assert(manager.activeSourceName === "source1")
    assert(manager.activeSourceIndex === 0)
    assert(manager.completedSources.isEmpty)
    assert(!manager.isOnFinalSource)
  }

  test("SequentialUnionManager - activeSource returns correct source") {
    val sourceNames = Seq("delta-historical", "kafka-live")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(2)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    assert(manager.activeSource === sources.head)
    assert(manager.activeSourceName === "delta-historical")
  }

  test("SequentialUnionManager - isOnFinalSource detection") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    assert(!manager.isOnFinalSource) // First source

    // Transition to second source
    manager.transitionToNextSource()
    assert(!manager.isOnFinalSource) // Second source

    // Transition to final source
    manager.transitionToNextSource()
    assert(manager.isOnFinalSource) // Final source
  }

  test("SequentialUnionManager - transitionToNextSource updates state") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    assert(manager.activeSourceName === "source1")
    assert(manager.completedSources.isEmpty)

    // Transition to source2
    val offset1 = manager.transitionToNextSource()
    assert(manager.activeSourceName === "source2")
    assert(manager.completedSources === Set("source1"))
    assert(offset1.activeSourceName === "source2")
    assert(offset1.completedSourceNames === Set("source1"))

    // Transition to source3
    val offset2 = manager.transitionToNextSource()
    assert(manager.activeSourceName === "source3")
    assert(manager.completedSources === Set("source1", "source2"))
    assert(offset2.activeSourceName === "source3")
    assert(offset2.completedSourceNames === Set("source1", "source2"))
  }

  test("SequentialUnionManager - transitionToNextSource fails on final source") {
    val sourceNames = Seq("source1", "source2")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(2)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Transition to final source
    manager.transitionToNextSource()
    assert(manager.isOnFinalSource)

    // Should fail when trying to transition past final source
    val ex = intercept[IllegalArgumentException] {
      manager.transitionToNextSource()
    }
    assert(ex.getMessage.contains("Cannot transition past final source"))
  }

  test("SequentialUnionManager - currentOffset reflects state") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    val offset = manager.currentOffset
    assert(offset.activeSourceName === "source1")
    assert(offset.allSourceNames === sourceNames)
    assert(offset.completedSourceNames.isEmpty)

    // After transition
    manager.transitionToNextSource()
    val offset2 = manager.currentOffset
    assert(offset2.activeSourceName === "source2")
    assert(offset2.completedSourceNames === Set("source1"))
  }

  test("SequentialUnionManager - initialOffset creates correct state") {
    val sourceNames = Seq("source1", "source2")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(2)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    val initialOffset = manager.initialOffset
    assert(initialOffset.activeSourceName === "source1")
    assert(initialOffset.allSourceNames === sourceNames)
    assert(initialOffset.completedSourceNames.isEmpty)
  }

  test("SequentialUnionManager - restoreFromOffset with valid offset") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Create offset representing middle state
    val offset = SequentialUnionOffset(
      activeSourceName = "source2",
      allSourceNames = sourceNames,
      completedSourceNames = Set("source1")
    )

    manager.restoreFromOffset(offset)

    assert(manager.activeSourceName === "source2")
    assert(manager.activeSourceIndex === 1)
    assert(manager.completedSources === Set("source1"))
  }

  test("SequentialUnionManager - restoreFromOffset with final source") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Create offset representing final source state
    val offset = SequentialUnionOffset(
      activeSourceName = "source3",
      allSourceNames = sourceNames,
      completedSourceNames = Set("source1", "source2")
    )

    manager.restoreFromOffset(offset)

    assert(manager.activeSourceName === "source3")
    assert(manager.activeSourceIndex === 2)
    assert(manager.isOnFinalSource)
    assert(manager.completedSources === Set("source1", "source2"))
  }

  test("SequentialUnionManager - restoreFromOffset fails with mismatched source names") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Offset with different source names
    val offset = SequentialUnionOffset(
      activeSourceName = "different2",
      allSourceNames = Seq("different1", "different2", "different3"),
      completedSourceNames = Set("different1")
    )

    val ex = intercept[IllegalArgumentException] {
      manager.restoreFromOffset(offset)
    }
    assert(ex.getMessage.contains("Source names in offset"))
    assert(ex.getMessage.contains("do not match"))
  }

  test("SequentialUnionManager - restoreFromOffset validates offset activeSource") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Note: SequentialUnionOffset validates activeSource in its constructor,
    // so we can't create an invalid offset. This test confirms that behavior.
    val ex = intercept[IllegalArgumentException] {
      SequentialUnionOffset(
        activeSourceName = "nonexistent",
        allSourceNames = sourceNames,
        completedSourceNames = Set.empty
      )
    }
    assert(ex.getMessage.contains("activeSourceName"))
    assert(ex.getMessage.contains("must be in allSourceNames"))
  }

  test("SequentialUnionManager - isSourceActive checks") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    assert(manager.isSourceActive("source1"))
    assert(!manager.isSourceActive("source2"))
    assert(!manager.isSourceActive("source3"))

    manager.transitionToNextSource()

    assert(!manager.isSourceActive("source1"))
    assert(manager.isSourceActive("source2"))
    assert(!manager.isSourceActive("source3"))
  }

  test("SequentialUnionManager - isSourceCompleted checks") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(3)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    assert(!manager.isSourceCompleted("source1"))
    assert(!manager.isSourceCompleted("source2"))
    assert(!manager.isSourceCompleted("source3"))

    manager.transitionToNextSource()

    assert(manager.isSourceCompleted("source1"))
    assert(!manager.isSourceCompleted("source2"))
    assert(!manager.isSourceCompleted("source3"))

    manager.transitionToNextSource()

    assert(manager.isSourceCompleted("source1"))
    assert(manager.isSourceCompleted("source2"))
    assert(!manager.isSourceCompleted("source3"))
  }

  test("SequentialUnionManager - prepareActiveSourceForAvailableNow calls source") {
    val sourceNames = Seq("source1", "source2")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(2)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Prepare first source
    manager.prepareActiveSourceForAvailableNow()
    verify(sources(0), times(1)).prepareForTriggerAvailableNow()

    // Transition and prepare second source
    manager.transitionToNextSource()

    // Should fail on final source
    val ex = intercept[IllegalArgumentException] {
      manager.prepareActiveSourceForAvailableNow()
    }
    assert(ex.getMessage.contains("Final source should not use AvailableNow preparation"))
  }

  test("SequentialUnionManager - validation: empty sourceNames") {
    val ex = intercept[IllegalArgumentException] {
      new SequentialUnionManager(
        createSequentialUnion(0),
        Seq.empty,
        Map.empty
      )
    }
    assert(ex.getMessage.contains("sourceNames must not be empty"))
  }

  test("SequentialUnionManager - validation: minimum 2 sources required") {
    val ex = intercept[IllegalArgumentException] {
      val source = createMockSource("source1")
      new SequentialUnionManager(
        createSequentialUnion(1),
        Seq("source1"),
        Map("source1" -> source)
      )
    }
    assert(ex.getMessage.contains("requires at least 2 sources"))
  }

  test("SequentialUnionManager - validation: sourceNames count mismatch") {
    val sourceNames = Seq("source1", "source2")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap

    val ex = intercept[IllegalArgumentException] {
      new SequentialUnionManager(
        createSequentialUnion(3), // 3 children
        sourceNames,               // 2 names
        sourceMap
      )
    }
    assert(ex.getMessage.contains("Number of source names"))
    assert(ex.getMessage.contains("must match number of children"))
  }

  test("SequentialUnionManager - validation: missing source in map") {
    val sourceNames = Seq("source1", "source2", "source3")
    val sources = sourceNames.take(2).map(createMockSource)
    val sourceMap = sourceNames.take(2).zip(sources).toMap // Missing source3

    val ex = intercept[IllegalArgumentException] {
      new SequentialUnionManager(
        createSequentialUnion(3),
        sourceNames,
        sourceMap
      )
    }
    assert(ex.getMessage.contains("All source names must have corresponding entries"))
    assert(ex.getMessage.contains("Missing"))
  }

  test("SequentialUnionManager - multiple sources lifecycle") {
    val sourceNames = Seq("delta-2023", "delta-2024", "delta-2025", "kafka-live")
    val sources = sourceNames.map(createMockSource)
    val sourceMap = sourceNames.zip(sources).toMap
    val sequentialUnion = createSequentialUnion(4)

    val manager = new SequentialUnionManager(sequentialUnion, sourceNames, sourceMap)

    // Initial state
    assert(manager.activeSourceName === "delta-2023")
    assert(manager.completedSources.isEmpty)
    assert(!manager.isOnFinalSource)

    // Transition through all sources
    manager.prepareActiveSourceForAvailableNow()
    verify(sources(0), times(1)).prepareForTriggerAvailableNow()
    manager.transitionToNextSource()

    assert(manager.activeSourceName === "delta-2024")
    assert(manager.completedSources === Set("delta-2023"))
    manager.prepareActiveSourceForAvailableNow()
    verify(sources(1), times(1)).prepareForTriggerAvailableNow()
    manager.transitionToNextSource()

    assert(manager.activeSourceName === "delta-2025")
    assert(manager.completedSources === Set("delta-2023", "delta-2024"))
    manager.prepareActiveSourceForAvailableNow()
    verify(sources(2), times(1)).prepareForTriggerAvailableNow()
    manager.transitionToNextSource()

    // Final source
    assert(manager.activeSourceName === "kafka-live")
    assert(manager.completedSources === Set("delta-2023", "delta-2024", "delta-2025"))
    assert(manager.isOnFinalSource)

    // Final source should not be prepared with AvailableNow
    intercept[IllegalArgumentException] {
      manager.prepareActiveSourceForAvailableNow()
    }
  }
}
