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

import java.net.URI
import java.util.{Locale, UUID}
import java.util.concurrent.Executor

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Properties

import com.google.protobuf.ByteString
import io.grpc._

import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.UserContext
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.sql.connect.common.config.ConnectCommon

/**
 * Conceptually the remote spark session that communicates with the server.
 */
private[sql] class SparkConnectClient(
    private[sql] val configuration: SparkConnectClient.Configuration,
    private[sql] val channel: ManagedChannel) {

  private val userContext: UserContext = configuration.userContext

  private[this] val stubState = new SparkConnectStubState(channel, configuration.retryPolicies)
  private[this] val bstub =
    new CustomSparkConnectBlockingStub(channel, stubState)
  private[this] val stub =
    new CustomSparkConnectStub(channel, stubState)

  private[client] def userAgent: String = configuration.userAgent

  /**
   * Placeholder method.
   * @return
   *   User ID.
   */
  private[sql] def userId: String = userContext.getUserId

  // Generate a unique session ID for this client. This UUID must be unique to allow
  // concurrent Spark sessions of the same user. If the channel is closed, creating
  // a new client will create a new session ID.
  private[sql] val sessionId: String = configuration.sessionId.getOrElse(UUID.randomUUID.toString)

  /**
   * Hijacks the stored server side session ID with the given suffix. Used for testing to make
   * sure that server is validating the session ID.
   */
  private[sql] def hijackServerSideSessionIdForTesting(suffix: String) = {
    stubState.responseValidator.hijackServerSideSessionIdForTesting(suffix)
  }

  /**
   * Returns true if the session is valid on both the client and the server. A session becomes
   * invalid if the server side information about the client, e.g., session ID, does not
   * correspond to the actual client state.
   */
  private[sql] def isSessionValid: Boolean = {
    // The last known state of the session is store in `responseValidator`, because it is where the
    // client gets responses from the server.
    stubState.responseValidator.isSessionValid
  }

  private[sql] val artifactManager: ArtifactManager = {
    new ArtifactManager(configuration, sessionId, bstub, stub)
  }

  /**
   * Manually triggers upload of all classfile artifacts to the Spark Connect Server
   */
  private[sql] def uploadAllClassFileArtifacts(): Unit =
    artifactManager.uploadAllClassFileArtifacts()

  /**
   * Returns the server-side session id obtained from the first request, if there was a request
   * already.
   */
  private def serverSideSessionId: Option[String] = {
    stubState.responseValidator.getServerSideSessionId
  }

  /**
   * Dispatch the [[proto.AnalyzePlanRequest]] to the Spark Connect server.
   * @return
   *   A [[proto.AnalyzePlanResponse]] from the Spark Connect server.
   */
  def analyze(request: proto.AnalyzePlanRequest): proto.AnalyzePlanResponse = {
    artifactManager.uploadAllClassFileArtifacts()
    bstub.analyzePlan(request)
  }

  /**
   * Execute the plan and return response iterator.
   *
   * It returns CloseableIterator. For resource management it is better to close it once you are
   * done. If you don't close it, it and the underlying data will be cleaned up once the iterator
   * is garbage collected.
   */
  def execute(plan: proto.Plan): CloseableIterator[proto.ExecutePlanResponse] = {
    artifactManager.uploadAllClassFileArtifacts()
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(plan)
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .addAllTags(tags.get.toSeq.asJava)
    serverSideSessionId.foreach(session => request.setClientObservedServerSideSessionId(session))
    if (configuration.useReattachableExecute) {
      bstub.executePlanReattachable(request.build())
    } else {
      bstub.executePlan(request.build())
    }
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
    serverSideSessionId.foreach(session => request.setClientObservedServerSideSessionId(session))
    bstub.config(request.build())
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
    serverSideSessionId.foreach(session => request.setClientObservedServerSideSessionId(session))
    analyze(request.build())
  }

  private[sql] def interruptAll(): proto.InterruptResponse = {
    val builder = proto.InterruptRequest.newBuilder()
    val request = builder
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
    serverSideSessionId.foreach(session => request.setClientObservedServerSideSessionId(session))
    bstub.interrupt(request.build())
  }

  private[sql] def interruptTag(tag: String): proto.InterruptResponse = {
    val builder = proto.InterruptRequest.newBuilder()
    val request = builder
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG)
      .setOperationTag(tag)
    serverSideSessionId.foreach(session => request.setClientObservedServerSideSessionId(session))
    bstub.interrupt(request.build())
  }

  private[sql] def interruptOperation(id: String): proto.InterruptResponse = {
    val builder = proto.InterruptRequest.newBuilder()
    val request = builder
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
      .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID)
      .setOperationId(id)
    serverSideSessionId.foreach(session => request.setClientObservedServerSideSessionId(session))
    bstub.interrupt(request.build())
  }

  private[sql] def releaseSession(): proto.ReleaseSessionResponse = {
    val builder = proto.ReleaseSessionRequest.newBuilder()
    val request = builder
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setClientType(userAgent)
    bstub.releaseSession(request.build())
  }

  private[this] val tags = new InheritableThreadLocal[mutable.Set[String]] {
    override def childValue(parent: mutable.Set[String]): mutable.Set[String] = {
      // Note: make a clone such that changes in the parent tags aren't reflected in
      // those of the children threads.
      parent.clone()
    }
    override protected def initialValue(): mutable.Set[String] = new mutable.HashSet[String]()
  }

  private[sql] def addTag(tag: String): Unit = {
    // validation is also done server side, but this will give error earlier.
    ProtoUtils.throwIfInvalidTag(tag)
    tags.get += tag
  }

  private[sql] def removeTag(tag: String): Unit = {
    // validation is also done server side, but this will give error earlier.
    ProtoUtils.throwIfInvalidTag(tag)
    tags.get.remove(tag)
  }

  private[sql] def getTags(): Set[String] = {
    tags.get.toSet
  }

  private[sql] def clearTags(): Unit = {
    tags.get.clear()
  }

  def copy(): SparkConnectClient = configuration.toSparkConnectClient

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
   * Add a single in-memory artifact to the session while preserving the directory structure
   * specified by `target` under the session's working directory of that particular file
   * extension.
   *
   * Supported target file extensions are .jar and .class.
   *
   * ==Example==
   * {{{
   *  addArtifact(bytesBar, "foo/bar.class")
   *  addArtifact(bytesFlat, "flat.class")
   *  // Directory structure of the session's working directory for class files would look like:
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/flat.class
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/foo/bar.class
   * }}}
   */
  def addArtifact(bytes: Array[Byte], target: String): Unit =
    artifactManager.addArtifact(bytes, target)

  /**
   * Add a single artifact to the session while preserving the directory structure specified by
   * `target` under the session's working directory of that particular file extension.
   *
   * Supported target file extensions are .jar and .class.
   *
   * ==Example==
   * {{{
   *  addArtifact("/Users/dummyUser/files/foo/bar.class", "foo/bar.class")
   *  addArtifact("/Users/dummyUser/files/flat.class", "flat.class")
   *  // Directory structure of the session's working directory for class files would look like:
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/flat.class
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/foo/bar.class
   * }}}
   */
  def addArtifact(source: String, target: String): Unit =
    artifactManager.addArtifact(source, target)

  /**
   * Add multiple artifacts to the session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   */
  def addArtifacts(uri: Seq[URI]): Unit = artifactManager.addArtifacts(uri)

  /**
   * Register a [[ClassFinder]] for dynamically generated classes.
   */
  def registerClassFinder(finder: ClassFinder): Unit = artifactManager.registerClassFinder(finder)

  /**
   * Shutdown the client's connection to the server.
   */
  def shutdown(): Unit = {
    channel.shutdownNow()
  }

  /**
   * Cache the given local relation at the server, and return its key in the remote cache.
   */
  def cacheLocalRelation(data: ByteString, schema: String): String = {
    val localRelation = proto.Relation
      .newBuilder()
      .getLocalRelationBuilder
      .setSchema(schema)
      .setData(data)
      .build()
    artifactManager.cacheArtifact(localRelation.toByteArray)
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
  private[sql] def apply(channel: ManagedChannel): SparkConnectClient = {
    new SparkConnectClient(Configuration(), channel)
  }

  def builder(): Builder = new Builder()

  /**
   * This is a helper class that is used to create a GRPC channel based on either a set host and
   * port or a NameResolver-compliant URI connection string.
   */
  class Builder(private var _configuration: Configuration) {
    def this() = this(Configuration())

    def configuration: Configuration = _configuration

    def userId(id: String): Builder = {
      // TODO this is not an optional field!
      require(id != null && id.nonEmpty)
      _configuration = _configuration.copy(userId = id)
      this
    }

    def userId: Option[String] = Option(_configuration.userId)

    def userName(name: String): Builder = {
      require(name != null && name.nonEmpty)
      _configuration = _configuration.copy(userName = name)
      this
    }

    def userName: Option[String] = Option(_configuration.userName)

    def host(inputHost: String): Builder = {
      require(inputHost != null)
      _configuration = _configuration.copy(host = inputHost)
      this
    }

    def host: String = _configuration.host

    def port(inputPort: Int): Builder = {
      _configuration = _configuration.copy(port = inputPort)
      this
    }

    def port: Int = _configuration.port

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
      if (_configuration.isSslEnabled.contains(false)) {
        throw new IllegalArgumentException(AUTH_TOKEN_ON_INSECURE_CONN_ERROR_MSG)
      }
      _configuration =
        _configuration.copy(token = Option(inputToken), isSslEnabled = Option(true))
      this
    }

    def token: Option[String] = _configuration.token

    def enableSsl(): Builder = {
      _configuration = _configuration.copy(isSslEnabled = Option(true))
      this
    }

    /**
     * Disables the SSL. Throws exception if the token has been set.
     *
     * @return
     *   this builder.
     */
    def disableSsl(): Builder = {
      require(token.isEmpty, AUTH_TOKEN_ON_INSECURE_CONN_ERROR_MSG)
      _configuration = _configuration.copy(isSslEnabled = Option(false))
      this
    }

    def sslEnabled: Boolean = _configuration.isSslEnabled.contains(true)

    def retryPolicy(policies: Seq[RetryPolicy]): Builder = {
      _configuration = _configuration.copy(retryPolicies = policies)
      this
    }

    def retryPolicy(policy: RetryPolicy): Builder = {
      retryPolicy(List(policy))
    }

    private object URIParams {
      val PARAM_USER_ID = "user_id"
      val PARAM_USE_SSL = "use_ssl"
      val PARAM_TOKEN = "token"
      val PARAM_USER_AGENT = "user_agent"
      val PARAM_SESSION_ID = "session_id"
      val PARAM_GRPC_MAX_MESSAGE_SIZE = "grpc_max_message_size"
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
      _configuration = _configuration.copy(userAgent = genUserAgent(value))
      this
    }

    def sessionId(value: String): Builder = {
      try {
        UUID.fromString(value).toString
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(
            "Parameter value 'session_id' must be a valid UUID format.",
            e)
      }
      _configuration = _configuration.copy(sessionId = Some(value))
      this
    }

    def sessionId: Option[String] = _configuration.sessionId

    def userAgent: String = _configuration.userAgent

    def grpcMaxMessageSize(messageSize: Int): Builder = {
      _configuration = _configuration.copy(grpcMaxMessageSize = messageSize)
      this
    }

    def grpcMaxMessageSize: Int = _configuration.grpcMaxMessageSize

    def grpcMaxRecursionLimit(recursionLimit: Int): Builder = {
      _configuration = _configuration.copy(grpcMaxRecursionLimit = recursionLimit)
      this
    }

    def grpcMaxRecursionLimit: Int = _configuration.grpcMaxRecursionLimit

    def option(key: String, value: String): Builder = {
      _configuration = _configuration.copy(metadata = _configuration.metadata + ((key, value)))
      this
    }

    def options: Map[String, String] = _configuration.metadata

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
          case URIParams.PARAM_SESSION_ID => sessionId(value)
          case URIParams.PARAM_GRPC_MAX_MESSAGE_SIZE => grpcMaxMessageSize(value.toInt)
          case _ => option(key, value)
        }
      }
    }

    /**
     * Configure the builder using the env SPARK_REMOTE environment variable.
     */
    def loadFromEnvironment(): Builder = {
      Option(System.getProperty("spark.remote")) // Set from Spark Submit
        .orElse(sys.env.get(SparkConnectClient.SPARK_REMOTE))
        .foreach(connectionString)
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
      host(uri.getHost)
      val inputPort = uri.getPort
      if (inputPort != -1) {
        port(uri.getPort)
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

    /**
     * Add an interceptor to be used during channel creation.
     *
     * Note that interceptors added last are executed first by gRPC.
     */
    def interceptor(interceptor: ClientInterceptor): Builder = {
      val interceptors = _configuration.interceptors ++ List(interceptor)
      _configuration = _configuration.copy(interceptors = interceptors)
      this
    }

    /**
     * Disable reattachable execute.
     */
    def disableReattachableExecute(): Builder = {
      _configuration = _configuration.copy(useReattachableExecute = false)
      this
    }

    /**
     * Enable reattachable execute.
     *
     * It makes client more robust, enabling reattaching to an ExecutePlanResponse stream in case
     * of intermittent connection errors.
     */
    def enableReattachableExecute(): Builder = {
      _configuration = _configuration.copy(useReattachableExecute = true)
      this
    }

    def build(): SparkConnectClient = _configuration.toSparkConnectClient
  }

  /**
   * Appends the Spark, Scala & JVM version, and the used OS to the user-provided user agent.
   */
  private def genUserAgent(value: String): String = {
    val scalaVersion = Properties.versionNumberString
    val jvmVersion = System.getProperty("java.version").split("_")(0)
    val osName = {
      val os = System.getProperty("os.name").toLowerCase(Locale.ROOT)
      if (os.contains("mac")) "darwin"
      else if (os.contains("linux")) "linux"
      else if (os.contains("win")) "windows"
      else "unknown"
    }
    List(
      value,
      s"spark/$SPARK_VERSION",
      s"scala/$scalaVersion",
      s"jvm/$jvmVersion",
      s"os/$osName").mkString(" ")
  }

  /**
   * Helper class that fully captures the configuration for a [[SparkConnectClient]].
   */
  private[sql] case class Configuration(
      userId: String = null,
      userName: String = null,
      host: String = "localhost",
      port: Int = ConnectCommon.CONNECT_GRPC_BINDING_PORT,
      token: Option[String] = None,
      isSslEnabled: Option[Boolean] = None,
      metadata: Map[String, String] = Map.empty,
      userAgent: String = genUserAgent(
        sys.env.getOrElse("SPARK_CONNECT_USER_AGENT", DEFAULT_USER_AGENT)),
      retryPolicies: Seq[RetryPolicy] = RetryPolicy.defaultPolicies(),
      useReattachableExecute: Boolean = true,
      interceptors: List[ClientInterceptor] = List.empty,
      sessionId: Option[String] = None,
      grpcMaxMessageSize: Int = ConnectCommon.CONNECT_GRPC_MAX_MESSAGE_SIZE,
      grpcMaxRecursionLimit: Int = ConnectCommon.CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT) {

    def userContext: proto.UserContext = {
      val builder = proto.UserContext.newBuilder()
      if (userId != null) {
        builder.setUserId(userId)
      }
      if (userName != null) {
        builder.setUserName(userName)
      }
      builder.build()
    }

    def credentials: ChannelCredentials = {
      if (isSslEnabled.contains(true)) {
        token match {
          case Some(t) =>
            // With access token added in the http header.
            CompositeChannelCredentials.create(
              TlsChannelCredentials.create,
              new AccessTokenCallCredentials(t))
          case None =>
            TlsChannelCredentials.create()
        }
      } else {
        InsecureChannelCredentials.create()
      }
    }

    def createChannel(): ManagedChannel = {
      val channelBuilder = Grpc.newChannelBuilderForAddress(host, port, credentials)

      if (metadata.nonEmpty) {
        channelBuilder.intercept(new MetadataHeaderClientInterceptor(metadata))
      }

      interceptors.foreach(channelBuilder.intercept(_))

      channelBuilder.maxInboundMessageSize(grpcMaxMessageSize)
      channelBuilder.build()
    }

    def toSparkConnectClient: SparkConnectClient = new SparkConnectClient(this, createChannel())
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
          headers.put(AUTH_TOKEN_META_DATA_KEY, s"Bearer $token")
          applier.apply(headers)
        } catch {
          case e: Throwable =>
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
        }
      })
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
