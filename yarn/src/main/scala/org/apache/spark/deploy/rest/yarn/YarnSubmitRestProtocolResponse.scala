package org.apache.spark.deploy.rest.yarn

import com.fasterxml.jackson.annotation.{JsonRootName, JsonProperty, JsonIgnore}
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.spark.deploy.rest.{SubmitRestProtocolException, SubmitRestProtocolMessage}

private[rest] class YarnSubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  @JsonIgnore
  override val action: String = null

  protected override def doValidate(): Unit = {
    if (action != null) {
      throw new SubmitRestProtocolException("action field should be null for Yarn related rest " +
        "protocols")
    }
  }
}

private[rest] class AppState extends YarnSubmitRestProtocolResponse {
  var state: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(state, "state")
  }
}

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
