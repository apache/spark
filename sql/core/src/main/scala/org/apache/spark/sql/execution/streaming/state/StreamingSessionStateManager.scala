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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, Predicate}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.StructType

// FIXME: javadoc!
sealed trait StreamingSessionStateManager extends Serializable {
  def getKey(row: UnsafeRow): UnsafeRow

  def getStateValueSchema: StructType

  def get(key: UnsafeRow): Iterator[UnsafeRow]

  def append(session: UnsafeRow): Boolean

  def doFinalize(): Unit

  def getAll(): Iterator[UnsafeRow]

  def evictSessionsByWatermark(): Iterator[UnsafeRow]

  def doEvictSessionsByWatermark(): Unit
}

// FIXME: javadoc!
trait MultiValuesStateManagerInjectable {
  def setMultiValuesStateManager(manager: MultiValuesStateManager): Unit
}

object StreamingSessionStateManager extends Logging {
  val supportedVersions = Seq(1)

  def createStateManager(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute],
      watermarkPredicateForData: Option[Predicate],
      stateFormatVersion: Int): StreamingSessionStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingSessionStateManagerImplV1(keyExpressions, inputRowAttributes,
        watermarkPredicateForData)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

abstract class StreamingSessionStateManagerBaseImpl(
    protected val keyExpressions: Seq[Attribute],
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingSessionStateManager {

  @transient protected lazy val keyProjector =
    GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

  override def getKey(row: UnsafeRow): UnsafeRow = keyProjector(row)
}

// FIXME: javadoc!
class StreamingSessionStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute],
    watermarkPredicateForData: Option[Predicate])
  extends StreamingSessionStateManagerBaseImpl(keyExpressions, inputRowAttributes)
  with MultiValuesStateManagerInjectable {

  var stateManager: MultiValuesStateManager = _
  var currentKey: UnsafeRow = _
  var previousSessions: List[UnsafeRow] = _

  @transient protected lazy val keyOrdering = TypeUtils.getInterpretedOrdering(
    keyExpressions.toStructType)
  @transient protected lazy val valueOrdering = TypeUtils.getInterpretedOrdering(
    inputRowAttributes.toStructType)

  override def setMultiValuesStateManager(manager: MultiValuesStateManager): Unit = {
    stateManager = manager
  }

  override def getStateValueSchema: StructType = inputRowAttributes.toStructType

  override def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    assertAvailability()

    stateManager.get(key)
  }

  override def append(session: UnsafeRow): Boolean = {
    assertAvailability()

    val key = keyProjector(session)

    if (currentKey == null || !keyOrdering.equiv(currentKey, key)) {
      currentKey = key.copy()

      // This is necessary because MultiValuesStateManager doesn't guarantee
      // stable ordering.
      // The number of values for the given key is expected to be likely small,
      // so listing it here doesn't hurt.
      previousSessions = stateManager.get(key).toList

      stateManager.removeKey(key)
    }

    stateManager.append(key, session)

    !previousSessions.exists(p => valueOrdering.equiv(session, p))
  }

  override def doFinalize(): Unit = {
    assertAvailability()

    // do nothing
  }

  override def getAll(): Iterator[UnsafeRow] = {
    assertAvailability()

    stateManager.getAllRowPairs.map(_.value)
  }

  override def evictSessionsByWatermark(): Iterator[UnsafeRow] = {
    assertAvailability()

    stateManager.removeByValueCondition { row => watermarkPredicateForData match {
        case Some(predicate) => predicate.eval(row)
        case None => false
      }
    }.map(_.value)
  }

  override def doEvictSessionsByWatermark(): Unit = {
    assertAvailability()

    // consume all elements to let removal take effect
    evictSessionsByWatermark().toList
  }

  private def assertAvailability(): Unit = {
    require(stateManager != null, "MultiValuesStateManager should be set before calling methods!")
  }
}
