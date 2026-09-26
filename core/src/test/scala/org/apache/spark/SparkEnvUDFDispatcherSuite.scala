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
package org.apache.spark

import org.apache.spark.internal.config.UDF
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{UDFDispatcherFactory, WorkerDispatcher, WorkerLogger}

class SparkEnvUDFDispatcherSuite extends SparkFunSuite {

  private def spec = UDFWorkerSpecification.getDefaultInstance

  test("no factory configured: dispatcher creation fails with an actionable message") {
    val factory = SparkEnv.resolveUDFDispatcherFactory(new SparkConf(false), isDriver = true)
    // Resolution itself must succeed: applications that never run an external UDF should not
    // fail at SparkEnv creation just because no dispatcher is configured.
    val e = intercept[UnsupportedOperationException] {
      factory.createDispatcher(spec, WorkerLogger.NoOp)
    }
    assert(e.getMessage.contains(UDF.DISPATCHER_FACTORY.key))
  }

  test("configured factory is instantiated and used") {
    val conf = new SparkConf(false)
      .set(UDF.DISPATCHER_FACTORY, classOf[TestNoArgDispatcherFactory].getName)
    val factory = SparkEnv.resolveUDFDispatcherFactory(conf, isDriver = true)
    assert(factory.isInstanceOf[TestNoArgDispatcherFactory])
    assert(factory.createDispatcher(spec, WorkerLogger.NoOp) === null)
  }

  test("factory receives the SparkConf and isDriver when it declares them") {
    val conf = new SparkConf(false)
      .set(UDF.DISPATCHER_FACTORY, classOf[TestConfDispatcherFactory].getName)
      .set("spark.test.marker", "set-by-conf")
    Seq(true, false).foreach { isDriver =>
      val factory = SparkEnv.resolveUDFDispatcherFactory(conf, isDriver)
        .asInstanceOf[TestConfDispatcherFactory]
      assert(factory.conf.get("spark.test.marker") === "set-by-conf")
      assert(factory.isDriver === isDriver)
    }
  }

  test("a class that is not a UDFDispatcherFactory is rejected against the config key") {
    val conf = new SparkConf(false)
      .set(UDF.DISPATCHER_FACTORY, classOf[NotAFactory].getName)
    val e = intercept[SparkException] {
      SparkEnv.resolveUDFDispatcherFactory(conf, isDriver = true)
    }
    assert(e.getMessage.contains(UDF.DISPATCHER_FACTORY.key))
    assert(e.getMessage.contains(classOf[NotAFactory].getName))
  }

  test("an unknown class name fails to resolve") {
    val conf = new SparkConf(false).set(UDF.DISPATCHER_FACTORY, "not.a.real.Factory")
    intercept[ClassNotFoundException] {
      SparkEnv.resolveUDFDispatcherFactory(conf, isDriver = true)
    }
  }
}

// Declared as top-level classes so that they have constructors Spark can find reflectively.

class TestNoArgDispatcherFactory extends UDFDispatcherFactory {
  override def createDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger): WorkerDispatcher = null
}

class TestConfDispatcherFactory(val conf: SparkConf, val isDriver: Boolean)
  extends UDFDispatcherFactory {
  override def createDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger): WorkerDispatcher = null
}

class NotAFactory
