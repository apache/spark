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

package org.apache.spark.deploy.history.yarn.testtools

import java.util.{Collection => JCollection}

import org.apache.hadoop.service.Service
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.scalatest.Assertions

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._

/**
 * Miscellaneous extra assertions
 */
trait ExtraAssertions extends Logging with Assertions {

  /**
   * Assert that an exception's toString value contains the supplied text.
   * If not, an error is logged and the exception is rethrown
   *
   * @param ex exception
   * @param text text
   */
  def assertExceptionMessageContains(ex: Exception, text: String): Unit = {
    if (!ex.toString.contains(text)) {
      logError(s"Did not find text $text in $ex", ex)
      throw ex
    }
  }

  /**
   * Check the exception message text and toString check
   *
   * @param ex exception to examine
   * @param messageCheck string to check in the `Exception.getMessage()` string
   * @param stringCheck string to look for in the `Exception.toString()` string;
   *                    if empty the check is skipped.
   */
  def assertExceptionDetails(ex: Throwable, messageCheck: String, stringCheck: String): Unit = {
    assertNotNull(ex.getMessage, s"Exception message in $ex")
    if (!ex.getMessage.contains(messageCheck)) {
      throw ex
    }
    if (!stringCheck.isEmpty && !ex.toString.contains(stringCheck)) {
      throw ex
    }
  }

  /**
   * Assert that a value is not null
   *
   * @param value value
   * @param text text for assertion
   */
  def assertNotNull(value: Any, text: String): Unit = {
    assert(value !== null, s"Null value; $text")
  }

  /**
   * Assert that an optional value is not `None`.
   *
   * @param value value
   * @param text text for assertion
   */
  def assertSome(value: Option[Any], text: String): Unit = {
    assert(value.nonEmpty, s"optional value is None; $text")
  }

  /**
   * Assert that an optional value is `None`
   *
   * @param value value
   * @param text text for assertion
   */
  def assertNone(value: Option[Any], text: String): Unit = {
    assert(value.isEmpty, s"Optional value is $value.get; $text")
  }

  /**
   * Assert that a Spark traversable instance is not empty
   *
   * @param traversable the traversable to check
   * @param text text for assertion
   * @tparam T traversable type
   */
  def assertNotEmpty[T](traversable: Traversable[T], text: String): Unit = {
    assert(traversable.nonEmpty, s"Empty traversable; $text")
  }

  /**
   * Assert that a java collection is not empty
   *
   * @param collection the collection to check
   * @param text text for assertion
   * @tparam T collection type
   */
  def assertNotEmpty[T](collection: JCollection[T], text: String): Unit = {
    assert (!collection.isEmpty, s"Empty collection; $text")
  }

  /**
   * Assert the list is of the given size. if not all elements are logged @ error,
   * then the method raises a failure.
   *
   * @param list list to examine
   * @param expectedSize expected size
   * @param message error message
   * @tparam T list type
   */
  def assertListSize[T](list: Seq[T], expectedSize: Int, message: String): Unit = {
    assertNotNull(list, message)
    if (list.size != expectedSize) {
      // no match
      val errorText = s"Wrong list size: expected=$expectedSize actual=${list.size}: $message"
      logError(errorText)
      list.foreach { e => logError(e.toString) }
      fail(errorText)
    }
  }

  /**
   * Assert that a list is Nil (and implicitly, not null)
   *
   * If not, an assertion is raised that contains the message and the list
   * @param list list to check
   * @param message message to raise
   * @tparam T list type
   */
  def assertNil[T](list: Seq[T], message: String): Unit = {
    assertNotNull(list, message)
    if (list != Nil) {
      fail(message + " " + list)
    }
  }

  /**
   * Assert that a service is not listening
   *
   * @param historyService history service
   */
  def assertNotListening(historyService: YarnHistoryService): Unit = {
    assert(!historyService.listening, s"history service is listening for events: $historyService")
  }

  /**
   * Assert that the number of events processed matches the number expected
   *
   * @param historyService history service
   * @param expected expected number
   * @param details text to include in error messages
   */
  def assertEventsProcessed(historyService: YarnHistoryService,
      expected: Int, details: String): Unit = {
    assertResult(expected, "wrong number of events processed " + details) {
      historyService.eventsProcessed
    }
  }

  /**
   * Assert that two timeline entities are non-null and equal
   *
   * @param expected expected entry
   * @param actual actual
   */
  def assertEntitiesEqual(expected: TimelineEntity, actual: TimelineEntity): Unit = {
    require(expected != null)
    require(actual != null)
    assert(expected === actual,
      s"Expected ${describeEntity(expected)};  got ${describeEntity(actual)}}")
  }

  /**
   * Assert that a service is in a specific state
   *
   * @param service service
   * @param state required state
   */
  def assertInState(service: Service, state: Service.STATE): Unit = {
    assertNotNull(service, "null service")
    assert(service.isInState(state), s"not in state $state: $service")
  }

  /**
   * Assert that a source string contains the `contained` substring.
   * (This is not a check for a proper subset; equality is also acceptable)
   * @param source source string
   * @param contained string to look for
   */
  def assertContains(source: String, contained: String, text: String = ""): Unit = {
    assertNotNull(source, s"$text null source")
    assertNotNull(contained, s"$text null `contained`")
    if (!source.contains(contained)) {
      fail(s"$text -Did not find '$contained' in '$source'")
    }
  }

  /**
   * Assert that a source string does contains the `contained` substring.
   * @param source source string
   * @param contained string to look for
   */
  def assertDoesNotContain(source: String, contained: String, text: String = ""): Unit = {
    assertNotNull(source, s"$text null source")
    assertNotNull(contained, s"$text null `contained`")
    if (source.contains(contained)) {
      fail(s"$text -Found '$contained' in '$source'")
    }
  }

  /**
   * Assert that a `[String, String]` map contains a `key:value` mapping,
   * and that the value contains the specified text.
   * @param map map to query
   * @param key key to retrieve
   * @param text text to look for in the resolved value
   */
  protected def assertMapValueContains(map: Map[String, String],
      key: String, text: String): Unit = {
    map.get(key) match {
      case Some(s) =>
        if (!text.isEmpty && !s.contains(text)) {
          fail(s"Did not find '$text' in key[$key] = '$s'")
        }

      case None =>
        fail(s"No entry for key $key")
    }
  }
}
