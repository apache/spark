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

class ScalaInitializationCheckSuite extends SparkFunSuite {

  test("SPARK-58867: -Xcheckinit detects eager reads of uninitialized vals") {
    assume(ScalaInitializationCheckSuite.scalaInitializationCheckEnabled)
    intercept[UninitializedFieldError] {
      new ScalaInitializationCheckSuite.BrokenInitialization
    }
  }
}

private object ScalaInitializationCheckSuite {

  private val scalaInitializationCheckEnabled =
    sys.env.get("SPARK_SCALA_CHECKINIT").exists(ScalaInitializationCheckSuite.isTruthy) ||
      sys.props.get("spark.scala.checkinit").exists(ScalaInitializationCheckSuite.isTruthy)

  private def isTruthy(value: String): Boolean = {
    Set("1", "true", "yes").contains(value.toLowerCase(java.util.Locale.ROOT))
  }

  private trait EagerTraitInitializer {
    protected val eagerlyReadValue: String

    val eagerlyComputedValue: Int = eagerlyReadValue.length
  }

  class BrokenInitialization extends EagerTraitInitializer {
    override protected val eagerlyReadValue: String = "initialized"
  }
}
