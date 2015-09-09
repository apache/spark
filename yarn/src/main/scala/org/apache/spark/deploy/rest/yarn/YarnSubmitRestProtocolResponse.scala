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

package org.apache.spark.deploy.rest.yarn

import com.fasterxml.jackson.annotation.{JsonProperty, JsonIgnore}

import org.apache.spark.deploy.rest.{SubmitRestProtocolException, SubmitRestProtocolMessage}

private[rest] abstract class YarnSubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  @JsonIgnore
  override val action: String = null

  protected override def doValidate(): Unit = {
    if (action != null) {
      throw new SubmitRestProtocolException("action field should be null for Yarn related rest " +
        "protocols")
    }
  }
}

/** Yarn REST protocol of application state */
private[rest] class AppState extends YarnSubmitRestProtocolResponse {
  var state: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(state, "state")
  }
}

/** Yarn REST protocol of new application response */
private[rest] class NewApplication extends YarnSubmitRestProtocolResponse {
  @JsonProperty("application-id")
  var applicationId: String = null

  @JsonProperty("maximum-resource-capability")
  var maxResourceCapability: ResourceInfo = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(applicationId, "application-id")
    assertFieldIsSet(maxResourceCapability, "maximum-resource-capability")
  }
}

/** Yarn REST protocol of remote exception */
private[rest] class RemoteExceptionData extends YarnSubmitRestProtocolResponse {
  @JsonProperty("RemoteException")
  var remoteException: RemoteException = null

  class RemoteException {
    var exception: String = null
    var message: String = null
    var javaClassName: String = null
  }

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(remoteException, "RemoteException")
  }
}
