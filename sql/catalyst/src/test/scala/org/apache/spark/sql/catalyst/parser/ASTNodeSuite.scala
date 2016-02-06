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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite

class ASTNodeSuite extends SparkFunSuite {
  test("SPARK-13157 - remainder must return all input chars") {
    val inputs = Seq(
      ("add jar", "file:///tmp/ab/TestUDTF.jar"),
      ("add jar", "file:///tmp/a@b/TestUDTF.jar"),
      ("add jar", "c:\\windows32\\TestUDTF.jar"),
      ("add jar", "some \nbad\t\tfile\r\n.\njar"),
      ("ADD JAR", "@*#&@(!#@$^*!@^@#(*!@#"),
      ("SET", "foo=bar"),
      ("SET", "foo*)(@#^*@&!#^=bar")
    )
    inputs.foreach {
      case (command, arguments) =>
        val node = ParseDriver.parsePlan(s"$command $arguments", null)
        assert(node.remainder === arguments)
    }
  }
}
