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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class StateStoreCoordinatorSuite extends SparkFunSuite with SharedSparkContext {

  test("report, verify, getLocation") {
    withCoordinator { coordinator =>
      val id = StateStoreId(0, 0)

      assert(coordinator.verifyIfInstanceActive(id, "exec1") === false)
      assert(coordinator.getLocation(id) === None)

      assert(coordinator.reportActiveInstance(id, "hostX", "exec1") === true)
      assert(coordinator.verifyIfInstanceActive(id, "exec1") === true)
      assert(coordinator.getLocation(id) ===
        Some(ExecutorCacheTaskLocation("hostX", "exec1").toString))

      assert(coordinator.reportActiveInstance(id, "hostX", "exec2") === true)
      assert(coordinator.verifyIfInstanceActive(id, "exec1") === false)
      assert(coordinator.verifyIfInstanceActive(id, "exec2") === true)

      assert(
        coordinator.getLocation(id) ===
          Some(ExecutorCacheTaskLocation("hostX", "exec2").toString))
    }
  }

  test("make inactive") {
    withCoordinator { coordinator =>
      val id1 = StateStoreId(0, 0)
      val id2 = StateStoreId(1, 0)
      val id3 = StateStoreId(0, 1)
      val host = "hostX"
      val exec = "exec1"

      assert(coordinator.reportActiveInstance(id1, host, exec) === true)
      assert(coordinator.reportActiveInstance(id2, host, exec) === true)
      assert(coordinator.reportActiveInstance(id3, host, exec) === true)

      assert(coordinator.verifyIfInstanceActive(id1, exec) === true)
      assert(coordinator.verifyIfInstanceActive(id2, exec) === true)
      assert(coordinator.verifyIfInstanceActive(id3, exec) === true)

      coordinator.makeInstancesInactive(Set(0))

      assert(coordinator.verifyIfInstanceActive(id1, exec) === false)
      assert(coordinator.verifyIfInstanceActive(id2, exec) === true)
      assert(coordinator.verifyIfInstanceActive(id3, exec) === false)

      assert(coordinator.getLocation(id1) === None)
      assert(
        coordinator.getLocation(id2) ===
          Some(ExecutorCacheTaskLocation(host, exec).toString))
      assert(coordinator.getLocation(id3) === None)

      coordinator.makeInstancesInactive(Set(1))
      assert(coordinator.verifyIfInstanceActive(id2, exec) === false)
      assert(coordinator.getLocation(id2) === None)
    }
  }

  test("communication") {
    withCoordinator { coordinator =>
      import StateStoreCoordinator._
      val id = StateStoreId(0, 0)
      val host = "hostX"

      val ref = RpcUtils.makeDriverRef("StateStoreCoordinator", sc.env.conf, sc.env.rpcEnv)

      assert(ask(VerifyIfInstanceActive(id, "exec1")) === Some(false))

      ask(ReportActiveInstance(id, host, "exec1"))
      assert(ask(VerifyIfInstanceActive(id, "exec1")) === Some(true))
      assert(
        coordinator.getLocation(id) ===
          Some(ExecutorCacheTaskLocation(host, "exec1").toString))

      ask(ReportActiveInstance(id, host, "exec2"))
      assert(ask(VerifyIfInstanceActive(id, "exec1")) === Some(false))
      assert(ask(VerifyIfInstanceActive(id, "exec2")) === Some(true))
      assert(
        coordinator.getLocation(id) ===
          Some(ExecutorCacheTaskLocation(host, "exec2").toString))
    }
  }

  private def withCoordinator(body: StateStoreCoordinator => Unit): Unit = {
    var coordinator: StateStoreCoordinator = null
    try {
      coordinator = new StateStoreCoordinator(sc.env.rpcEnv)
      body(coordinator)
    } finally {
      if (coordinator != null) coordinator.stop()
    }
  }
}
