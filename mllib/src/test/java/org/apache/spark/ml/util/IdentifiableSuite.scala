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

package org.apache.spark.ml.util

import org.scalatest.FunSuite

class IdentifiableSuite extends FunSuite {

  import IdentifiableSuite._

  test("Identifiable") {
    val test0 = new Test0
    assert(test0.uid.startsWith(classOf[Test0].getSimpleName + "_"))

    val test1 = new Test1
    assert(test1.uid.startsWith("test_"),
      "simpleClassName should be the first part of the generated UID.")
    val copied = test1.copy
    assert(copied.uid === test1.uid, "Copied objects should be able to use the same UID.")
  }
}

object IdentifiableSuite {

  class Test0 extends Identifiable

  class Test1 extends Identifiable {

    override def simpleClassName: String = "test"

    def copy: Test1 = {
      new Test1().setUID(uid)
    }
  }
}
