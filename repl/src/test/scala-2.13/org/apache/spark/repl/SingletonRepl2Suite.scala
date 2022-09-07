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

package org.apache.spark.repl

import java.io._

import org.apache.spark.SparkFunSuite

/**
 * A special test suite for REPL that all test cases share one REPL instance.
 */
class SingletonRepl2Suite extends SparkFunSuite {

  private val out = new StringWriter()
  private val in = new PipedOutputStream()
  private var thread: Thread = _

  private val CONF_EXECUTOR_CLASSPATH = "spark.executor.extraClassPath"
  private val oldExecutorClasspath = System.getProperty(CONF_EXECUTOR_CLASSPATH)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val classpath = System.getProperty("java.class.path")
    System.setProperty(CONF_EXECUTOR_CLASSPATH, classpath)

    Main.conf.set("spark.master", "local-cluster[2,1,1024]")
    val interp = new SparkILoop(
      new BufferedReader(new InputStreamReader(new PipedInputStream(in))),
      new PrintWriter(out))

    // Forces to create new SparkContext
    Main.sparkContext = null
    Main.sparkSession = null

    // Starts a new thread to run the REPL interpreter, so that we won't block.
    thread = new Thread(() => Main.doMain(Array("-classpath", classpath), interp))
    thread.setDaemon(true)
    thread.start()

    waitUntil(() => out.toString.contains("Type :help for more information"))
  }

  override def afterAll(): Unit = {
    in.close()
    thread.join()
    if (oldExecutorClasspath != null) {
      System.setProperty(CONF_EXECUTOR_CLASSPATH, oldExecutorClasspath)
    } else {
      System.clearProperty(CONF_EXECUTOR_CLASSPATH)
    }
    super.afterAll()
  }

  private def waitUntil(cond: () => Boolean): Unit = {
    import scala.concurrent.duration._
    import org.scalatest.concurrent.Eventually._

    eventually(timeout(50.seconds), interval(500.millis)) {
      assert(cond(), "current output: " + out.toString)
    }
  }

  /**
   * Run the given commands string in a globally shared interpreter instance. Note that the given
   * commands should not crash the interpreter, to not affect other test cases.
   */
  def runInterpreter(input: String): String = {
    val currentOffset = out.getBuffer.length()
    // append a special statement to the end of the given code, so that we can know what's
    // the final output of this code snippet and rely on it to wait until the output is ready.
    val timestamp = System.currentTimeMillis()
    in.write((input + s"\nval _result_$timestamp = 1\n").getBytes)
    in.flush()
    val stopMessage = s"_result_$timestamp: Int = 1"
    waitUntil(() => out.getBuffer.substring(currentOffset).contains(stopMessage))
    out.getBuffer.substring(currentOffset)
  }

  def assertContains(message: String, output: String): Unit = {
    val isContain = output.contains(message)
    assert(isContain,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String): Unit = {
    val isContain = output.contains(message)
    assert(!isContain,
      "Interpreter output contained '" + message + "':\n" + output)
  }

  test("SPARK-31399: should clone+clean line object w/ non-serializable state in ClosureCleaner") {
    // Test ClosureCleaner when a closure captures the enclosing `this` REPL line object, and that
    // object contains an unused non-serializable field.
    // Specifically, the closure in this test case contains a directly nested closure, and the
    // capture is triggered by the inner closure.
    // `ns` should be nulled out, but `topLevelValue` should stay intact.

    // Can't use :paste mode because PipedOutputStream/PipedInputStream doesn't work well with the
    // EOT control character (i.e. Ctrl+D).
    // Just write things on a single line to emulate :paste mode.

    // NOTE: in order for this test case to trigger the intended scenario, the following three
    //       variables need to be in the same "input", which will make the REPL pack them into the
    //       same REPL line object:
    //         - ns: a non-serializable state, not accessed by the closure;
    //         - topLevelValue: a serializable state, accessed by the closure;
    //         - closure: the starting closure, captures the enclosing REPL line object.
    val output = runInterpreter(
      """
        |class NotSerializableClass(val x: Int)
        |val ns = new NotSerializableClass(42); val topLevelValue = "someValue"; val closure =
        |(j: Int) => {
        |  (1 to j).flatMap { x =>
        |    (1 to x).map { y => y + topLevelValue }
        |  }
        |}
        |val r = sc.parallelize(0 to 2).map(closure).collect
      """.stripMargin)
//    assertContains("r: Array[scala.collection.immutable.IndexedSeq[String]] = " +
//      "Array(Vector(), Vector(1someValue), Vector(1someValue, 1someValue, 2someValue))", output)
    assertContains("r: Array[IndexedSeq[String]] = " +
      "Array(Vector(), Vector(1someValue), Vector(1someValue, 1someValue, 2someValue))", output)
    assertDoesNotContain("Exception", output)
  }

  test("SPARK-31399: ClosureCleaner should discover indirectly nested closure in inner class") {
    // Similar to the previous test case, but with indirect closure nesting instead.
    // There's still nested closures involved, but the inner closure is indirectly nested in the
    // outer closure, with a level of inner class in between them.
    // This changes how the inner closure references/captures the outer closure/enclosing `this`
    // REPL line object, and covers a different code path in inner closure discovery.

    // `ns` should be nulled out, but `topLevelValue` should stay intact.

    val output = runInterpreter(
      """
        |class NotSerializableClass(val x: Int)
        |val ns = new NotSerializableClass(42); val topLevelValue = "someValue"; val closure =
        |(j: Int) => {
        |  class InnerFoo {
        |    val innerClosure = (x: Int) => (1 to x).map { y => y + topLevelValue }
        |  }
        |  val innerFoo = new InnerFoo
        |  (1 to j).flatMap(innerFoo.innerClosure)
        |}
        |val r = sc.parallelize(0 to 2).map(closure).collect
      """.stripMargin)
//    assertContains("r: Array[scala.collection.immutable.IndexedSeq[String]] = " +
//       "Array(Vector(), Vector(1someValue), Vector(1someValue, 1someValue, 2someValue))", output)
    assertContains("r: Array[IndexedSeq[String]] = " +
       "Array(Vector(), Vector(1someValue), Vector(1someValue, 1someValue, 2someValue))", output)
    assertDoesNotContain("Array(Vector(), Vector(1null), Vector(1null, 1null, 2null)", output)
    assertDoesNotContain("Exception", output)
  }
}
