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

class NettyNoAuthMasterNetSuite extends AbstractNettyMasterNet {

  masterConf.set("spark.authenticate", "false")

  // scheduler secret

  test("registering app with secret") {
    // because in Netty mode mutual authentication is required, but Master uses
    // no authentication
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(schedulerSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testAppRegistration(clientEnv, masterRef)
    }
  }

  test("registering app with secret - master failure scenario") {
    // because Master needs to connect back to the client after
    // restarting and needs to authenticate with the secret used by the client but Master uses
    // no authentication
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(schedulerSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testAppRegistrationWithMasterFailure(clientEnv, masterRef)
    }
  }

  test("sending scheduling msg with secret") {
    // because in Netty mode mutual authentication is required, but Master uses
    // no authentication
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(schedulerSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testSchedulingMsgAsk(masterRef)
    }
  }

  test("sending submission msg with secret") {
    // because in Netty mode mutual authentication is required, but Master uses
    // no authentication
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(schedulerSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testSubmissionMsgAsk(masterRef)
    }
  }

  // no secret

  test("registering app with no secret") {
    val clientEnv = makeEnv(noSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testAppRegistration(clientEnv, masterRef)
  }

  test("registering app with no secret - master failure scenario") {
    val clientEnv = makeEnv(noSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testAppRegistrationWithMasterFailure(clientEnv, masterRef)
  }

  test("sending scheduling msg with no secret") {
    val clientEnv = makeEnv(noSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testSchedulingMsgAsk(masterRef)
  }

  test("sending submission msg with no secret") {
    val clientEnv = makeEnv(noSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testSubmissionMsgAsk(masterRef)
  }

}
