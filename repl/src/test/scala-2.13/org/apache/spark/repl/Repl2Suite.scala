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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkContext, SparkFunSuite}

class Repl2Suite extends SparkFunSuite with BeforeAndAfterAll {
  test("propagation of local properties") {
    // A mock ILoop that doesn't install the SIGINT handler.
    class ILoop(out: PrintWriter) extends SparkILoop(null, out)

    val out = new StringWriter()
    Main.interp = new ILoop(new PrintWriter(out))
    Main.sparkContext = new SparkContext("local", "repl-test")
    val settings = new scala.tools.nsc.Settings
    settings.usejavacp.value = true
    Main.interp.createInterpreter(settings)

    Main.sparkContext.setLocalProperty("someKey", "someValue")

    // Make sure the value we set in the caller to interpret is propagated in the thread that
    // interprets the command.
    Main.interp.interpret("org.apache.spark.repl.Main.sparkContext.getLocalProperty(\"someKey\")")
    assert(out.toString.contains("someValue"))

    Main.sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }
}
