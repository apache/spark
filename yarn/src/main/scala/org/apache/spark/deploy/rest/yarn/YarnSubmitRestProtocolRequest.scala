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

import java.io.{DataInputStream, ByteArrayInputStream}
import java.net.URI
import java.util

import scala.collection.JavaConversions._

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.TokenIdentifier
import org.apache.hadoop.yarn.api.records._

import com.fasterxml.jackson.annotation.{JsonProperty, JsonIgnore}
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.security.Credentials

import org.apache.spark.deploy.rest.{SubmitRestProtocolException, SubmitRestProtocolMessage}

private[rest] abstract class YarnSubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  @JsonIgnore
  override val action: String = null

  protected override def doValidate(): Unit = {
    if (action != null) {
      throw new SubmitRestProtocolException("action field should be null for Yarn related rest " +
        "protocols")
    }
  }
}

/** Yarn REST application submission protocol */
private[rest] class ApplicationSubmissionContextInfo extends YarnSubmitRestProtocolRequest {
  @JsonProperty("application-id")
  var applicationId: String = null

  @JsonProperty("application-name")
  var applicationName: String = null

  var queue: String = null

  var priority: Int = -1

  @JsonProperty("am-container-spec")
  var containerInfo: ContainerLaunchContextInfo = null

  @JsonProperty("unmanaged-AM")
  var isUnmanagedAM: Boolean = false

  @JsonProperty("cancel-tokens-when-complete")
  var cancelTokensWhenComplete: Boolean = true

  @JsonProperty("max-app-attempts")
  var maxAppAttempts: Int = -1

  @JsonProperty("resource")
  var resource: ResourceInfo = null

  @JsonProperty("application-type")
  var applicationType: String = "SPARK"

  @JsonProperty("keep-containers-across-application-attempts")
  var keepContainers: Boolean = false

  class Tag { var tag: String = null }
  @JsonProperty("application-tags")
  var tags: Set[Tag] = null

  @JsonProperty("app-node-label-expression")
  var appNodeLabelExpression: String = null

  @JsonProperty("am-container-node-label-expression")
  var amContainerNodeLabelExpression: String = null

  def buildFrom(
      applicationId: String,
      applicationName: String,
      queue: String,
      containerContext: ContainerLaunchContext,
      resource: Resource,
      tags: Set[String],
      maxAppAttempts: Int = 2,
      priority: Int = 1): ApplicationSubmissionContextInfo = {
    this.applicationId = applicationId
    this.applicationName = applicationName
    this.queue = queue
    this.priority = priority
    this.containerInfo = new ContainerLaunchContextInfo().buildFrom(containerContext)
    this.isUnmanagedAM = isUnmanagedAM
    this.cancelTokensWhenComplete = cancelTokensWhenComplete
    this.maxAppAttempts = maxAppAttempts
    this.resource = new ResourceInfo().buildFrom(resource)

    if (!tags.isEmpty) {
      this.tags = tags.map { t =>
        val a = new Tag
        a.tag = t
        a
      }
    }

    this
  }
}

/** Yarn REST protocol of container launch context */
private[rest] class ContainerLaunchContextInfo extends YarnSubmitRestProtocolRequest {
  @JsonProperty("local-resources")
  var localResources: JaxbMapWrapper[String, LocalResourceInfo] = null

  var environment: JaxbMapWrapper[String, String] = null

  class Command { var command: String = null }
  var commands: List[Command] = null

  @JsonProperty("service-data")
  var serviceData: JaxbMapWrapper[String, String] = null

  var credentials: CredentialsInfo = null

  @JsonProperty("application-acls")
  var acls: JaxbMapWrapper[ApplicationAccessType, String] = null

  def buildFrom(containerContext: ContainerLaunchContext): ContainerLaunchContextInfo = {
    if (!containerContext.getLocalResources.isEmpty) {
      localResources = new JaxbMapWrapper[String, LocalResourceInfo].buildFrom(
        containerContext.getLocalResources
          .mapValues(r => new LocalResourceInfo().buildFrom(r)).toMap)
    }

    if (!containerContext.getEnvironment.isEmpty) {
      environment = new JaxbMapWrapper[String, String]().buildFrom(
        containerContext.getEnvironment.toMap)
    }

    if (!containerContext.getCommands.isEmpty) {
      val commandStr = containerContext.getCommands.mkString(" ")
      val t = new Command
      t.command = commandStr
      commands = List(t)
    }

    if (!containerContext.getServiceData.isEmpty) {
      serviceData = new JaxbMapWrapper[String, String]().buildFrom {
        containerContext.getServiceData.mapValues { bytes =>
          Base64.encodeBase64URLSafeString(bytes.array())
        }.toMap
      }
    }

    val in = new DataInputStream(new ByteArrayInputStream(containerContext.getTokens.array()))
    val cred = new Credentials()
    cred.readTokenStorageStream(in)
    credentials = new CredentialsInfo().buildFrom(cred)

    if (!containerContext.getApplicationACLs.isEmpty) {
      acls = new JaxbMapWrapper[ApplicationAccessType, String]().buildFrom(
        containerContext.getApplicationACLs.toMap)
    }

    this
  }
}

/** Yarn REST protocol of local resource */
private[rest] class LocalResourceInfo extends YarnSubmitRestProtocolRequest {
  @JsonProperty("resource")
  var url: URI = null

  @JsonProperty("type")
  var tpe: LocalResourceType = null

  var visibility: LocalResourceVisibility = null

  var size: Long = -1

  var timestamp: Long = -1

  var pattern: String = null

  def buildFrom(localResource: LocalResource): LocalResourceInfo = {
    val resUrl = localResource.getResource
    url =
      new URI(resUrl.getScheme, null, resUrl.getHost, resUrl.getPort, resUrl.getFile, null, null)

    tpe = localResource.getType
    visibility = localResource.getVisibility
    size = localResource.getSize
    timestamp = localResource.getTimestamp
    pattern = localResource.getPattern

    this
  }
}

/** Yarn REST protocol of credentials */
private[rest] class CredentialsInfo extends YarnSubmitRestProtocolRequest {
  var tokens: JaxbMapWrapper[String, String] = null
  var secrets: JaxbMapWrapper[String, String] = null

  def buildFrom(credentials: Credentials): CredentialsInfo = {
    import org.apache.hadoop.security.token.Token

    if (!credentials.getAllSecretKeys.isEmpty) {
      secrets = new JaxbMapWrapper[String, String]().buildFrom {
        credentials.getAllSecretKeys.map { key =>
          (key.toString, Base64.encodeBase64String(credentials.getSecretKey(key)))
        }.toMap
      }
    }

    // Using reflections to get hadoop tokens in Credentials
    val hadoopTokens = try {
      val field = credentials.getClass.getDeclaredField("tokenMap")
      field.setAccessible(true)
      field.get(credentials).asInstanceOf[util.Map[Text, Token[_ <: TokenIdentifier]]]
    } catch {
      case e: Exception =>
        new util.HashMap[Text, Token[_ <: TokenIdentifier]]()
    }

    if (!hadoopTokens.isEmpty) {
      tokens = new JaxbMapWrapper[String, String]().buildFrom {
        hadoopTokens.map { case (k, v) => (k.toString, v.encodeToUrlString()) }.toMap
      }
    }

    this
  }
}

/** Yarn REST protocol of resource */
private[rest] class ResourceInfo extends YarnSubmitRestProtocolRequest {
  var memory: Int = 0
  var vCores: Int = 0

  def buildFrom(resource: Resource): ResourceInfo = {
    memory = resource.getMemory
    vCores = resource.getVirtualCores
    this
  }
}

private[rest] class JaxbMapWrapper[K, V] {
  var entry: List[Entry] = null

  def buildFrom(mapObj: Map[K, V]): JaxbMapWrapper[K, V] = {
    entry = mapObj.map { case (k, v) =>
      val e = new Entry()
      e.key = k
      e.value = v
      e
    }.toList

    this
  }

  class Entry {
    var key: K = null.asInstanceOf[K]
    var value: V = null.asInstanceOf[V]
  }
}

