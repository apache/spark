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

class NettySharedSecretMasterNetSuite extends AbstractNettyMasterNet {

  masterConf.set("spark.authenticate", "true")
  masterConf.set("spark.authenticate.secret", "1q2w3e")
  masterConf.set("spark.authenticate.enableSaslEncryption", "true")

  // scheduler secret

  test("registering app with scheduler secret") {
    val clientEnv = makeEnv(schedulerSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testAppRegistration(clientEnv, masterRef)
  }

  test("registering app with scheduler secret - master failure scenario") {
    val clientEnv = makeEnv(schedulerSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testAppRegistrationWithMasterFailure(clientEnv, masterRef)
  }

  test("sending scheduling msg with scheduler secret") {
    val clientEnv = makeEnv(schedulerSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testSchedulingMsgAsk(masterRef)
  }

  test("sending submission msg with scheduler secret") {
    val clientEnv = makeEnv(schedulerSecretConf)
    val masterRef = connectToMaster(clientEnv)
    testSubmissionMsgAsk(masterRef)
  }

  // bad secret

  test("registering app with bad secret") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(badSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testAppRegistration(clientEnv, masterRef)
    }
  }

  test("registering app with bad secret - master failure scenario") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(badSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testAppRegistrationWithMasterFailure(clientEnv, masterRef)
    }
  }

  test("sending scheduling msg with bad secret") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(badSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testSchedulingMsgAsk(masterRef)
    }
  }

  test("sending submission msg with bad secret") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(badSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testSubmissionMsgAsk(masterRef)
    }
  }

  // no secret

  test("registering app with no secret") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(noSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testAppRegistration(clientEnv, masterRef)
    }
  }

  test("registering app with no secret - master failure scenario") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(noSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testAppRegistrationWithMasterFailure(clientEnv, masterRef)
    }
  }

  test("sending scheduling msg with no secret") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(noSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testSchedulingMsgAsk(masterRef)
    }
  }

  test("sending submission msg with no secret") {
    shouldFailBecauseOfWrongSecret {
      val clientEnv = makeEnv(noSecretConf)
      val masterRef = connectToMaster(clientEnv)
      testSubmissionMsgAsk(masterRef)
    }
  }

}
