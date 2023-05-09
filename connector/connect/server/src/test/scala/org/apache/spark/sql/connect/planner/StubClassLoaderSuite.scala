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
package org.apache.spark.sql.connect.planner

import org.apache.spark.SparkFunSuite

/**
 * This suite is based on connect DSL and test that given same dataframe operations, whether
 * connect could construct a proto plan that can be translated back, and after analyzed, be the
 * same as Spark dataframe's generated plan.
 */
class StubClassLoaderSuite extends SparkFunSuite {

  test("load") {
    val cl = new StubClassLoader(getClass().getClassLoader(), _ => true)
    val cls = cl.findClass("my.name.HelloWorld")
    assert(cls.getName === "my.name.HelloWorld")
  }

  test("class for name") {
    val cl = new StubClassLoader(getClass().getClassLoader(), _ => true)
    // scalastyle:off classforname
    val cls = Class.forName("my.name.HelloWorld", false, cl)
    // scalastyle:on classforname
    assert(cls.getName === "my.name.HelloWorld")
  }
}
