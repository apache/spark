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

import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, Table}

import org.apache.spark.HostState.HostState

class HostStateSuite extends SparkFunSuite with Matchers {

  test("Contract for the conversion between YARN NodeState and HostState") {
    val mappings =
      Table(
        ("yarnNodeState", "hostState"),
        (HostState.toYarnState(HostState.New), HostState.New),
        (HostState.toYarnState(HostState.Running), HostState.Running),
        (HostState.toYarnState(HostState.Decommissioned), HostState.Decommissioned),
        (HostState.toYarnState(HostState.Decommissioning), HostState.Decommissioning),
        (HostState.toYarnState(HostState.Unhealthy), HostState.Unhealthy),
        (HostState.toYarnState(HostState.Rebooted), HostState.Rebooted))

    forAll (mappings) { (yarnNodeState: Option[String], hostState: HostState) =>
      assert(yarnNodeState.isDefined)
      val hostStateOpt = HostState.fromYarnState(yarnNodeState.get)
      assert(hostStateOpt.isDefined)
      hostStateOpt.get should be (hostState)
    }
  }

}
