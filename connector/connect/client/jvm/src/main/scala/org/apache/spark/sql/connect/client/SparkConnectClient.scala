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

package org.apache.spark.sql.connect.client

import io.grpc.{CallCredentials, CallOptions, Channel, ClientCall, ClientInterceptor, CompositeChannelCredentials, ForwardingClientCall, Grpc, InsecureChannelCredentials, ManagedChannel, ManagedChannelBuilder, Metadata, MethodDescriptor, Status, TlsChannelCredentials}
import java.net.URI
import java.util.UUID
import java.util.concurrent.Executor
import scala.language.existentials

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.UserContext
import org.apache.spark.sql.connect.common.config.ConnectCommon

/**
 * Conceptually the remote spark session that communicates with the server.
 */
private[sql] class SparkConnectClient(
    private val userContext: proto.UserContext,
    private val channelBuilder: ManagedChannelBuilder[_],
    private[client] val userAgent: String) {

  private[this] lazy val channel: ManagedChannel = channelBuilder.build()

  private[this] val stub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)

  private[client] val artifactManager: ArtifactManager = new ArtifactManager(userContext, channel)

  /**
   * Placeholder method.
   * @return
   *   User ID.
   */
  private[client] def userId: String = userContext.getUserId()

  // Generate a unique session ID for this client. This UUID must be unique to allow
  // concurrent Spark sessions of the same user. If the channel is closed, creating
  // a new client will create a new session ID.
  private[client] val sessionId: String = UUID.randomUUID.toString

  /**
   * Dispatch the [[proto.AnalyzePlanRequest]] to the Spark Connect server.
   * @return
   *   A [[proto.AnalyzePlanResponse]] from the Spark Connect server.
   */
  def analyze(request: proto.AnalyzePlanRequest): proto.AnalyzePlanResponse =
    stub.analyzePlan(request)

  def execute(plan: proto.Plan): java.util.Iterator[proto.ExecutePlanResponse] = {
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(plan)
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .build()
    stub.executePlan(request)
  }

  /**
   * Dispatch the [[proto.ConfigRequest]] to the Spark Connect server.
   * @return
   *   A [[proto.ConfigResponse]] from the Spark Connect server.
   */
  def config(operation: proto.ConfigRequest.Operation): proto.ConfigResponse = {
    val request = proto.ConfigRequest
      .newBuilder()
      .setOperation(operation)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .setUserContext(userContext)
      .build()
    stub.config(request)
  }

  /**
   * Builds a [[proto.AnalyzePlanRequest]] from `plan` and dispatched it to the Spark Connect
   * server.
   * @return
   *   A [[proto.AnalyzePlanResponse]] from the Spark Connect server.
   */
  def analyze(
      method: proto.AnalyzePlanRequest.AnalyzeCase,
      plan: Option[proto.Plan] = None,
      explainMode: Option[proto.AnalyzePlanRequest.Explain.ExplainMode] = None)
      : proto.AnalyzePlanResponse = {
    val builder = proto.AnalyzePlanRequest.newBuilder()
    method match {
      case proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA =>
        assert(plan.isDefined)
        builder.setSchema(
          proto.AnalyzePlanRequest.Schema
            .newBuilder()
            .setPlan(plan.get)
            .build())
      case proto.AnalyzePlanRequest.AnalyzeCase.EXPLAIN =>
        if (explainMode.isEmpty) {
          throw new IllegalArgumentException(s"ExplainMode is required in Explain request")
        }
        assert(plan.isDefined)
        builder.setExplain(
          proto.AnalyzePlanRequest.Explain
            .newBuilder()
            .setPlan(plan.get)
            .setExplainMode(explainMode.get)
            .build())
      case proto.AnalyzePlanRequest.AnalyzeCase.IS_LOCAL =>
        assert(plan.isDefined)
        builder.setIsLocal(
          proto.AnalyzePlanRequest.IsLocal
            .newBuilder()
            .setPlan(plan.get)
            .build())
      case proto.AnalyzePlanRequest.AnalyzeCase.IS_STREAMING =>
        assert(plan.isDefined)
        builder.setIsStreaming(
          proto.AnalyzePlanRequest.IsStreaming
            .newBuilder()
            .setPlan(plan.get)
            .build())
      case proto.AnalyzePlanRequest.AnalyzeCase.INPUT_FILES =>
        assert(plan.isDefined)
        builder.setInputFiles(
          proto.AnalyzePlanRequest.InputFiles
            .newBuilder()
            .setPlan(plan.get)
            .build())
      case proto.AnalyzePlanRequest.AnalyzeCase.SPARK_VERSION =>
        builder.setSparkVersion(proto.AnalyzePlanRequest.SparkVersion.newBuilder().build())
      case other => throw new IllegalArgumentException(s"Unknown Analyze request $other")
    }
    analyze(builder)
  }

  def sameSemantics(plan: proto.Plan, otherPlan: proto.Plan): proto.AnalyzePlanResponse = {
    val builder = proto.AnalyzePlanRequest.newBuilder()
    builder.setSameSemantics(
      proto.AnalyzePlanRequest.SameSemantics
        .newBuilder()
        .setTargetPlan(plan)
        .setOtherPlan(otherPlan))
    analyze(builder)
  }

  def semanticHash(plan: proto.Plan): proto.AnalyzePlanResponse = {
    val builder = proto.AnalyzePlanRequest.newBuilder()
    builder.setSemanticHash(
      proto.AnalyzePlanRequest.SemanticHash
        .newBuilder()
        .setPlan(plan))
    analyze(builder)
  }

  private[sql] def analyze(
      builder: proto.AnalyzePlanRequest.Builder): proto.AnalyzePlanResponse = {
    val request = builder
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .build()
    analyze(request)
  }

  def copy(): SparkConnectClient = {
    new SparkConnectClient(userContext, channelBuilder, userAgent)
  }

  /**
   * Add a single artifact to the client session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   */
  def addArtifact(path: String): Unit = artifactManager.addArtifact(path)

  /**
   * Add a single artifact to the client session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   */
  def addArtifact(uri: URI): Unit = artifactManager.addArtifact(uri)

  /**
   * Add multiple artifacts to the session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   */
  def addArtifacts(uri: Seq[URI]): Unit = artifactManager.addArtifacts(uri)

  /**
   * Shutdown the client's connection to the server.
   */
  def shutdown(): Unit = {
    channel.shutdownNow()
  }
}

object SparkConnectClient {

  private val SPARK_REMOTE: String = "SPARK_REMOTE"

  private val DEFAULT_USER_AGENT: String = "_SPARK_CONNECT_SCALA"

  private val AUTH_TOKEN_META_DATA_KEY: Metadata.Key[String] =
    Metadata.Key.of("Authentication", Metadata.ASCII_STRING_MARSHALLER)

  private val AUTH_TOKEN_ON_INSECURE_CONN_ERROR_MSG: String =
    "Authentication token cannot be passed over insecure connections. " +
      "Either remove 'token' or set 'use_ssl=true'"

  // for internal tests
  private[sql] def apply(
      userContext: UserContext,
      builder: ManagedChannelBuilder[_]): SparkConnectClient =
    new SparkConnectClient(userContext, builder, DEFAULT_USER_AGENT)

  def builder(): Builder = new Builder()

  /**
   * This is a helper class that is used to create a GRPC channel based on either a set host and
   * port or a NameResolver-compliant URI connection string.
   */
  class Builder() {
    private val userContextBuilder = proto.UserContext.newBuilder()
    private var _userAgent: String = DEFAULT_USER_AGENT

    private var _host: String = "localhost"
    private var _port: Int = ConnectCommon.CONNECT_GRPC_BINDING_PORT

    private var _token: Option[String] = None
    // If no value specified for isSslEnabled, default to false
    private var isSslEnabled: Option[Boolean] = None

    private var metadata: Map[String, String] = Map.empty

    def userId(id: String): Builder = {
      // TODO this is not an optional field!
      require(id != null && id.nonEmpty)
      userContextBuilder.setUserId(id)
      this
    }

    def userId: Option[String] = Option(userContextBuilder.getUserId).filter(_.nonEmpty)

    def userName(name: String): Builder = {
      require(name != null && name.nonEmpty)
      userContextBuilder.setUserName(name)
      this
    }

    def userName: Option[String] = Option(userContextBuilder.getUserName).filter(_.nonEmpty)

    def host(inputHost: String): Builder = {
      require(inputHost != null)
      _host = inputHost
      this
    }

    def host: String = _host

    def port(inputPort: Int): Builder = {
      _port = inputPort
      this
    }

    def port: Int = _port

    /**
     * Setting the token implicitly sets the use_ssl=true. All the following examples yield the
     * same results:
     *
     * {{{
     * sc://localhost/;token=aaa
     * sc://localhost/;use_ssl=true;token=aaa
     * sc://localhost/;token=aaa;use_ssl=true
     * }}}
     *
     * Throws exception if the token is set but use_ssl=false.
     *
     * @param inputToken
     *   the user token.
     * @return
     *   this builder.
     */
    def token(inputToken: String): Builder = {
      require(inputToken != null && inputToken.nonEmpty)
      _token = Some(inputToken)
      // Only set the isSSlEnabled if it is not yet set
      isSslEnabled match {
        case None => isSslEnabled = Some(true)
        case Some(false) =>
          throw new IllegalArgumentException(AUTH_TOKEN_ON_INSECURE_CONN_ERROR_MSG)
        case Some(true) => // Good, the ssl is enabled
      }
      this
    }

    def token: Option[String] = _token

    def enableSsl(): Builder = {
      isSslEnabled = Some(true)
      this
    }

    /**
     * Disables the SSL. Throws exception if the token has been set.
     *
     * @return
     *   this builder.
     */
    def disableSsl(): Builder = {
      require(_token.isEmpty, AUTH_TOKEN_ON_INSECURE_CONN_ERROR_MSG)
      isSslEnabled = Some(false)
      this
    }

    def sslEnabled: Boolean = isSslEnabled.contains(true)

    private object URIParams {
      val PARAM_USER_ID = "user_id"
      val PARAM_USE_SSL = "use_ssl"
      val PARAM_TOKEN = "token"
      val PARAM_USER_AGENT = "user_agent"
    }

    private def verifyURI(uri: URI): Unit = {
      if (uri.getScheme != "sc") {
        throw new IllegalArgumentException("Scheme for connection URI must be 'sc'.")
      }
      if (uri.getHost == null) {
        throw new IllegalArgumentException(s"Host for connection URI must be defined.")
      }
      // Java URI considers everything after the authority segment as "path" until the
      // ? (query)/# (fragment) components as shown in the regex
      // [scheme:][//authority][path][?query][#fragment].
      // However, with the Spark Connect definition, configuration parameter are passed in the
      // style of the HTTP URL Path Parameter Syntax (e.g
      // sc://hostname:port/;param1=value;param2=value).
      // Thus, we manually parse the "java path" to get the "correct path" and configuration
      // parameters.
      val pathAndParams = uri.getPath.split(';')
      if (pathAndParams.nonEmpty && (pathAndParams(0) != "/" && pathAndParams(0) != "")) {
        throw new IllegalArgumentException(
          s"Path component for connection URI must be empty: " +
            s"${pathAndParams(0)}")
      }
    }

    def userAgent(value: String): Builder = {
      require(value != null)
      _userAgent = value
      this
    }

    def userAgent: String = _userAgent

    def option(key: String, value: String): Builder = {
      metadata += ((key, value))
      this
    }

    def options: Map[String, String] = metadata

    private def parseURIParams(uri: URI): Unit = {
      val params = uri.getPath.split(';').drop(1).filter(_ != "")
      params.foreach { kv =>
        val (key, value) = {
          val arr = kv.split('=')
          if (arr.length != 2) {
            throw new IllegalArgumentException(
              s"Parameter $kv is not a valid parameter" +
                s" key-value pair")
          }
          (arr(0), arr(1))
        }
        key match {
          case URIParams.PARAM_USER_ID => userId(value)
          case URIParams.PARAM_USER_AGENT => userAgent(value)
          case URIParams.PARAM_TOKEN => token(value)
          case URIParams.PARAM_USE_SSL =>
            if (java.lang.Boolean.valueOf(value)) enableSsl() else disableSsl()
          case _ => this.metadata = this.metadata + (key -> value)
        }
      }
    }

    /**
     * Configure the builder using the env SPARK_REMOTE environment variable.
     */
    def loadFromEnvironment(): Builder = {
      sys.env.get("SPARK_REMOTE").foreach(connectionString)
      this
    }

    /**
     * Creates the channel with a target connection string, per the documentation of Spark
     * Connect.
     *
     * Note: The connection string, if used, will override any previous host/port settings.
     */
    def connectionString(connectionString: String): Builder = {
      val uri = new URI(connectionString)
      verifyURI(uri)
      parseURIParams(uri)
      _host = uri.getHost
      val inputPort = uri.getPort
      if (inputPort != -1) {
        _port = inputPort
      }
      this
    }

    /**
     * Configure the builder with the given CLI arguments.
     */
    def parse(args: Array[String]): Builder = {
      SparkConnectClientParser.parse(args.toList, this)
      this
    }

    def build(): SparkConnectClient = {
      val creds = isSslEnabled match {
        case Some(false) | None => InsecureChannelCredentials.create()
        case Some(true) =>
          _token match {
            case Some(t) =>
              // With access token added in the http header.
              CompositeChannelCredentials.create(
                TlsChannelCredentials.create,
                new AccessTokenCallCredentials(t))
            case None =>
              TlsChannelCredentials.create
          }
      }

      val channelBuilder = Grpc.newChannelBuilderForAddress(_host, _port, creds)
      if (metadata.nonEmpty) {
        channelBuilder.intercept(new MetadataHeaderClientInterceptor(metadata))
      }
      channelBuilder.maxInboundMessageSize(ConnectCommon.CONNECT_GRPC_MAX_MESSAGE_SIZE)
      new SparkConnectClient(userContextBuilder.build(), channelBuilder, _userAgent)
    }
  }

  /**
   * A [[CallCredentials]] created from an access token.
   *
   * @param token
   *   A string to place directly in the http request authorization header, for example
   *   "authorization: Bearer <access_token>".
   */
  private[client] class AccessTokenCallCredentials(token: String) extends CallCredentials {
    override def applyRequestMetadata(
        requestInfo: CallCredentials.RequestInfo,
        appExecutor: Executor,
        applier: CallCredentials.MetadataApplier): Unit = {
      appExecutor.execute(() => {
        try {
          val headers = new Metadata()
          headers.put(AUTH_TOKEN_META_DATA_KEY, s"Bearer $token");
          applier.apply(headers)
        } catch {
          case e: Throwable =>
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
        }
      })
    }

    override def thisUsesUnstableApi(): Unit = {
      // Marks this API is not stable. Left empty on purpose.
    }
  }

  /**
   * A client interceptor to pass extra parameters in http request header.
   *
   * @param metadata
   *   extra metadata placed in the http request header, for example "key: value".
   */
  private[client] class MetadataHeaderClientInterceptor(metadata: Map[String, String])
      extends ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel): ClientCall[ReqT, RespT] = {
      new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
        next.newCall(method, callOptions)) {
        override def start(
            responseListener: ClientCall.Listener[RespT],
            headers: Metadata): Unit = {
          metadata.foreach { case (key, value) =>
            headers.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value)
          }
          super.start(responseListener, headers)
        }
      }
    }
  }
}
