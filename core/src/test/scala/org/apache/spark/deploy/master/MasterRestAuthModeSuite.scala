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

package org.apache.spark.deploy.master

class MasterRestAuthModeSuite extends MasterSuiteBase {
  test("SPARK-38862: defaults to NoneOption when set differently rejects alt auth method") {
      var mode = MasterRestAuthMode.fromStringOrNone("Unknown")
      assert(mode === MasterRestAuthMode.NoneOption)
      assert(mode.toString === "None")
      assert(MasterRestAuthMode.serverSecuredByAlternativeMethod(mode) === false)
  }

  test("SPARK-38862: SecureGatewayOption resolved via SecureGateway allows alt auth method") {
      var mode = MasterRestAuthMode.
        fromStringOrNone(MasterRestAuthMode.SecureGatewayOption.toString)
      assert(mode === MasterRestAuthMode.SecureGatewayOption)
      assert(mode.toString === "SecureGateway")
      assert(MasterRestAuthMode.serverSecuredByAlternativeMethod(mode) === true)
  }
}
