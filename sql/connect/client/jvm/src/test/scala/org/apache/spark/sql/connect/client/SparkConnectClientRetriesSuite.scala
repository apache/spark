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
package org.apache.spark.sql.connect.client

import scala.concurrent.duration.FiniteDuration

import com.google.protobuf.{Any, Duration}
import com.google.rpc
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
import org.scalatest.concurrent.Eventually

import org.apache.spark.sql.connect.test.ConnectFunSuite

class SparkConnectClientRetriesSuite extends ConnectFunSuite with Eventually {

  private class DummyFn(e: => Throwable, numFails: Int = 3) {
    var counter = 0
    def fn(): Int = {
      if (counter < numFails) {
        counter += 1
        throw e
      } else {
        42
      }
    }
  }

  /** Tracks sleep times in milliseconds for testing purposes. */
  private class SleepTimeTracker {
    private val data = scala.collection.mutable.ListBuffer[Long]()
    def sleep(t: Long): Unit = data.append(t)
    def times: List[Long] = data.toList
    def totalSleep: Long = data.sum
  }

  /** Injectable clock for testing elapsed-time bounds without real waits. */
  private class FakeClock {
    private var current: Long = 0L
    def advanceMillis(ms: Long): Unit = current += ms * 1000000L
    def nowNanos(): Long = current
  }

  /** Helper function for creating a test exception with retry_delay */
  private def createTestExceptionWithDetails(
      msg: String,
      code: Status.Code = Status.Code.INTERNAL,
      retryDelay: FiniteDuration = FiniteDuration(0, "s")): StatusRuntimeException = {
    // In grpc-java, RetryDelay should be specified as seconds: Long + nanos: Int
    val seconds = retryDelay.toSeconds
    val nanos = (retryDelay - FiniteDuration(seconds, "s")).toNanos.toInt
    val retryDelayMsg = Duration
      .newBuilder()
      .setSeconds(seconds)
      .setNanos(nanos)
      .build()
    val retryInfo = rpc.RetryInfo
      .newBuilder()
      .setRetryDelay(retryDelayMsg)
      .build()
    val status = rpc.Status
      .newBuilder()
      .setMessage(msg)
      .setCode(code.value())
      .addDetails(Any.pack(retryInfo))
      .build()
    StatusProto.toStatusRuntimeException(status)
  }

  /** helper function for comparing two sequences of sleep times */
  private def assertLongSequencesAlmostEqual(
      first: Seq[Long],
      second: Seq[Long],
      delta: Long): Unit = {
    assert(first.length == second.length, "Lists have different lengths.")
    for ((a, b) <- first.zip(second)) {
      assert(math.abs(a - b) <= delta, s"Elements $a and $b differ by more than $delta.")
    }
  }

  test("SPARK-44721: Retries run for a minimum period") {
    // repeat test few times to avoid random flakes
    for (_ <- 1 to 10) {
      val st = new SleepTimeTracker()
      val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE), numFails = 100)
      val retryHandler = new GrpcRetryHandler(RetryPolicy.defaultPolicies(), st.sleep)

      assertThrows[StatusRuntimeException] {
        retryHandler.retry {
          dummyFn.fn()
        }
      }

      assert(st.totalSleep >= 10 * 60 * 1000) // waited at least 10 minutes
    }
  }

  test("SPARK-44275: retry actually retries") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE))
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = _ => {})
    val result = retryHandler.retry { dummyFn.fn() }

    assert(result == 42)
    assert(dummyFn.counter == 3)
  }

  test("SPARK-44275: default retryException retries only on UNAVAILABLE") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.ABORTED))
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = _ => {})

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 1)
  }

  test("SPARK-44275: retry uses canRetry to filter exceptions") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE))
    val retryPolicy = RetryPolicy(canRetry = _ => false, name = "TestPolicy")
    val retryHandler = new GrpcRetryHandler(retryPolicy)

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 1)
  }

  test("SPARK-44275: retry does not exceed maxRetries") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE))
    val retryPolicy = RetryPolicy(canRetry = _ => true, maxRetries = Some(1), name = "TestPolicy")
    val retryHandler = new GrpcRetryHandler(retryPolicy, sleep = _ => {})

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 2)
  }

  def testPolicySpecificError(maxRetries: Int, status: Status): RetryPolicy = {
    RetryPolicy(
      maxRetries = Some(maxRetries),
      name = s"Policy for ${status.getCode}",
      canRetry = {
        case e: StatusRuntimeException => e.getStatus.getCode == status.getCode
        case _ => false
      })
  }

  test("Test multiple policies") {
    val policy1 = testPolicySpecificError(maxRetries = 2, status = Status.UNAVAILABLE)
    val policy2 = testPolicySpecificError(maxRetries = 4, status = Status.INTERNAL)

    // Tolerate 2 UNAVAILABLE errors and 4 INTERNAL errors

    val errors = (List.fill(2)(Status.UNAVAILABLE) ++ List.fill(4)(Status.INTERNAL)).iterator

    new GrpcRetryHandler(List(policy1, policy2), sleep = _ => {}).retry({
      val e = errors.nextOption()
      if (e.isDefined) {
        throw e.get.asRuntimeException()
      }
    })

    assert(!errors.hasNext)
  }

  test("Test multiple policies exceed") {
    val policy1 = testPolicySpecificError(maxRetries = 2, status = Status.INTERNAL)
    val policy2 = testPolicySpecificError(maxRetries = 4, status = Status.INTERNAL)

    val errors = List.fill(10)(Status.INTERNAL).iterator
    var countAttempted = 0

    assertThrows[StatusRuntimeException](
      new GrpcRetryHandler(List(policy1, policy2), sleep = _ => {}).retry({
        countAttempted += 1
        val e = errors.nextOption()
        if (e.isDefined) {
          throw e.get.asRuntimeException()
        }
      }))

    assert(countAttempted == 3)
  }

  test("DefaultPolicy retries exceptions with RetryInfo") {
    // Error contains RetryInfo with retry_delay set to 0
    val dummyFn =
      new DummyFn(createTestExceptionWithDetails(msg = "Some error message"), numFails = 100)
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = _ => {})
    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }

    // Should be retried by DefaultPolicy
    val policy = retryPolicies.find(_.name == "DefaultPolicy").get
    assert(dummyFn.counter == policy.maxRetries.get + 1)
  }

  test("retry_delay overrides maxBackoff") {
    val st = new SleepTimeTracker()
    val retryDelay = FiniteDuration(5, "min")
    val dummyFn = new DummyFn(
      createTestExceptionWithDetails(msg = "Some error message", retryDelay = retryDelay),
      numFails = 100)
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = st.sleep)

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }

    // Should be retried by DefaultPolicy
    val policy = retryPolicies.find(_.name == "DefaultPolicy").get
    // sleep times are higher than maxBackoff and are equal to retryDelay + jitter
    st.times.foreach(t => assert(t > policy.maxBackoff.get.toMillis + policy.jitter.toMillis))
    val expectedSleeps = List.fill(policy.maxRetries.get)(retryDelay.toMillis)
    assertLongSequencesAlmostEqual(st.times, expectedSleeps, policy.jitter.toMillis)
  }

  test("maxServerRetryDelay limits retry_delay") {
    val st = new SleepTimeTracker()
    val retryDelay = FiniteDuration(5, "d")
    val dummyFn = new DummyFn(
      createTestExceptionWithDetails(msg = "Some error message", retryDelay = retryDelay),
      numFails = 100)
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = st.sleep)

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }

    // Should be retried by DefaultPolicy
    val policy = retryPolicies.find(_.name == "DefaultPolicy").get
    val expectedSleeps = List.fill(policy.maxRetries.get)(policy.maxServerRetryDelay.get.toMillis)
    assertLongSequencesAlmostEqual(st.times, expectedSleeps, policy.jitter.toMillis)
  }

  test("Policy uses to exponential backoff after retry_delay is unset") {
    val st = new SleepTimeTracker()
    val retryDelay = FiniteDuration(5, "min")
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = st.sleep)
    val errors = (
      List.fill(2)(
        createTestExceptionWithDetails(
          msg = "Some error message",
          retryDelay = retryDelay)) ++ List.fill(3)(
        createTestExceptionWithDetails(
          msg = "Some error message",
          code = Status.Code.UNAVAILABLE))
    ).iterator

    retryHandler.retry({
      if (errors.hasNext) {
        throw errors.next()
      }
    })
    assert(!errors.hasNext)

    // Should be retried by DefaultPolicy
    val policy = retryPolicies.find(_.name == "DefaultPolicy").get
    val expectedSleeps = List.fill(2)(retryDelay.toMillis) ++ List.tabulate(3)(i =>
      policy.initialBackoff.toMillis * math.pow(policy.backoffMultiplier, i + 2).toLong)
    assertLongSequencesAlmostEqual(st.times, expectedSleeps, delta = policy.jitter.toMillis)
  }

  test("DEADLINE_EXCEEDED is not retryable by defaultPolicy") {
    // DEADLINE_EXCEEDED must not be retried via canRetry. The reattachable execute path
    // handles it separately by converting it to RetryException in the iterator.
    val exception = new StatusRuntimeException(Status.DEADLINE_EXCEEDED)
    val canRetry = RetryPolicy.defaultPolicy().canRetry
    assert(canRetry(exception) == false)
  }

  test("DEADLINE_EXCEEDED is not retried by retry handler") {
    // Verify the function is called exactly once and the exception propagates immediately.
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.DEADLINE_EXCEEDED), numFails = 1)
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = _ => {})
    intercept[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 1)
  }

  test("RetryException retries are bounded by elapsed time") {
    // Each attempt simulates a full 10-min reattach deadline elapsing before RetryException is
    // raised. The elapsed clock starts on the first RetryException, after that attempt already
    // advanced the clock, so with a 1-hour bound and a 10-min-per-attempt advance the loop gives
    // up after bound / increment + 1 = 7 attempts: attempts 1-6 leave elapsed < 1h, and attempt
    // 7 leaves elapsed = 6 * 10min = 1h >= bound, tripping the throw.
    val clock = new FakeClock()
    val cause = new StatusRuntimeException(Status.DEADLINE_EXCEEDED)
    var tries = 0
    val retryHandler = new GrpcRetryHandler(
      RetryPolicy.defaultPolicies(),
      sleep = _ => {},
      maxRetryExceptionElapsedTime = FiniteDuration(1, "hour"),
      nowNanos = clock.nowNanos)

    val thrown = intercept[StatusRuntimeException] {
      retryHandler.retry {
        tries += 1
        clock.advanceMillis(10 * 60 * 1000)
        val error = new GrpcRetryHandler.RetryException()
        error.addSuppressed(cause)
        throw error
      }
    }
    assert(thrown eq cause)
    assert(tries == 7)
  }

  test("RetryException below the elapsed bound keeps retrying") {
    // Non-regression: well under the elapsed-time budget, RetryException retries should keep
    // behaving as before (immediate retry, no policy consulted) until success.
    val clock = new FakeClock()
    var tries = 0
    val retryHandler = new GrpcRetryHandler(
      RetryPolicy.defaultPolicies(),
      sleep = _ => {},
      maxRetryExceptionElapsedTime = FiniteDuration(1, "hour"),
      nowNanos = clock.nowNanos)

    val result = retryHandler.retry {
      tries += 1
      if (tries <= 3) {
        clock.advanceMillis(10 * 1000)
        throw new GrpcRetryHandler.RetryException()
      }
      42
    }
    assert(result == 42)
    assert(tries == 4)
  }

  test("RetryException with no suppressed cause throws itself once bound exceeded") {
    val clock = new FakeClock()
    val retryHandler = new GrpcRetryHandler(
      RetryPolicy.defaultPolicies(),
      sleep = _ => {},
      maxRetryExceptionElapsedTime = FiniteDuration(100, "s"),
      nowNanos = clock.nowNanos)

    intercept[GrpcRetryHandler.RetryException] {
      retryHandler.retry {
        clock.advanceMillis(30 * 1000)
        throw new GrpcRetryHandler.RetryException()
      }
    }
  }

  test("elapsed-time clock for RetryException starts only at first occurrence") {
    // The elapsed-time clock should start only when the first RetryException is observed, not
    // at Retrying construction, and should be unaffected by interleaved ordinary
    // (policy-governed) retries that happen first.
    val clock = new FakeClock()
    val cause = new StatusRuntimeException(Status.DEADLINE_EXCEEDED)
    val st = new SleepTimeTracker()
    var tries = 0
    val retryHandler = new GrpcRetryHandler(
      RetryPolicy.defaultPolicies(),
      sleep = st.sleep,
      maxRetryExceptionElapsedTime = FiniteDuration(1, "hour"),
      nowNanos = clock.nowNanos)

    val thrown = intercept[StatusRuntimeException] {
      retryHandler.retry {
        tries += 1
        if (tries <= 2) {
          // Ordinary, policy-governed retries -- must not consume any of the RetryException
          // elapsed-time budget.
          throw new StatusRuntimeException(Status.UNAVAILABLE)
        }
        clock.advanceMillis(10 * 60 * 1000)
        val error = new GrpcRetryHandler.RetryException()
        error.addSuppressed(cause)
        throw error
      }
    }
    assert(thrown eq cause)
    // 2 ordinary retries + 7 RetryException retries (same count as the isolated test above).
    assert(tries == 2 + 7)
  }

  test("default maxRetryExceptionElapsedTime is 1 hour") {
    // Uses the default maxRetryExceptionElapsedTime (not passed) -- if this default ever
    // changes, update this assertion together with the doc comment on GrpcRetryHandler's
    // constructor.
    val clock = new FakeClock()
    var tries = 0
    val retryHandler = new GrpcRetryHandler(
      RetryPolicy.defaultPolicies(),
      sleep = _ => {},
      nowNanos = clock.nowNanos)

    intercept[GrpcRetryHandler.RetryException] {
      retryHandler.retry {
        tries += 1
        clock.advanceMillis(10 * 60 * 1000)
        throw new GrpcRetryHandler.RetryException()
      }
    }
    assert(tries == 7)
  }

  test("RpcDeadlines.disabled creates an instance with all deadlines set to None") {
    val disabled = RpcDeadlines.disabled
    assert(disabled.reattachableExecutePlan.isEmpty)
    assert(disabled.reattachExecute.isEmpty)
    assert(disabled.analyzePlan.isEmpty)
    assert(disabled.addArtifacts.isEmpty)
    assert(disabled.config.isEmpty)
    assert(disabled.interrupt.isEmpty)
    assert(disabled.releaseSession.isEmpty)
    assert(disabled.artifactStatus.isEmpty)
    assert(disabled.cloneSession.isEmpty)
    assert(disabled.getStatus.isEmpty)
    assert(disabled.fetchErrorDetails.isEmpty)
  }
}
