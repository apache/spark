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
package org.apache.spark.status.api.v1

import jakarta.ws.rs.WebApplicationException
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite

class SimpleDateParamSuite extends SparkFunSuite with Matchers {

  test("date parsing") {
    new SimpleDateParam("2015-02-20T23:21:17.190GMT").timestamp should be (1424474477190L)
    // don't use EST, it is ambiguous, use -0500 instead, see SPARK-15723
    new SimpleDateParam("2015-02-20T17:21:17.190-0500").timestamp should be (1424470877190L)
    new SimpleDateParam("2015-02-20").timestamp should be (1424390400000L) // GMT
    intercept[WebApplicationException] {
      new SimpleDateParam("invalid date")
    }
  }

}
