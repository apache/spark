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

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.grpc.{CallOptions, Channel, ClientCall, ClientInterceptor, MethodDescriptor, Server, Status, StatusRuntimeException}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse, ArtifactStatusesRequest, ArtifactStatusesResponse, ExecutePlanRequest, ExecutePlanResponse, Relation, SparkConnectServiceGrpc, SQL}
import org.apache.spark.sql.connect.SparkSession
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.sql.connect.test.ConnectFunSuite

class SparkConnectClientSuite extends ConnectFunSuite with BeforeAndAfterEach {

  private var client: SparkConnectClient = _
  private var service: DummySparkConnectService = _
  private var server: Server = _

  private def startDummyServer(port: Int): Unit = {
    service = new DummySparkConnectService
    server = NettyServerBuilder
      .forPort(port)
      .addService(service)
      .build()
    server.start()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    client = null
    server = null
    service = null
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }

    if (client != null) {
      client.shutdown()
    }
  }

  test("SPARK-51391: Use 'user.name' by default") {
    client = SparkConnectClient.builder().build()
    assert(client.userId == System.getProperty("user.name"))
  }

  test("Placeholder test: Create SparkConnectClient") {
    client = SparkConnectClient.builder().userId("abc123").build()
    assert(client.userId == "abc123")
  }

  // Use 0 to start the server at a random port
  private def testClientConnection(serverPort: Int = 0)(
      clientBuilder: Int => SparkConnectClient): Unit = {
    startDummyServer(serverPort)
    client = clientBuilder(server.getPort)
    val request = AnalyzePlanRequest
      .newBuilder()
      .setSessionId("abc123")
      .build()

    val response = client.analyze(request)
    assert(response.getSessionId === "abc123")
  }

  private def withEnvs(pairs: (String, String)*)(f: => Unit): Unit = {
    val readonlyEnv = System.getenv()
    val field = readonlyEnv.getClass.getDeclaredField("m")
    field.setAccessible(true)
    val modifiableEnv = field.get(readonlyEnv).asInstanceOf[java.util.Map[String, String]]
    try {
      for ((k, v) <- pairs) {
        assert(!modifiableEnv.containsKey(k))
        modifiableEnv.put(k, v)
      }
      f
    } finally {
      for ((k, _) <- pairs) {
        modifiableEnv.remove(k)
      }
    }
  }

  test("Test connection") {
    testClientConnection() { testPort => SparkConnectClient.builder().port(testPort).build() }
  }

  test("Test connection string") {
    testClientConnection() { testPort =>
      SparkConnectClient.builder().connectionString(s"sc://localhost:$testPort").build()
    }
  }

  test("Test encryption") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}/;use_ssl=true")
      .retryPolicy(RetryPolicy(maxRetries = Some(0), canRetry = _ => false, name = "TestPolicy"))
      .build()

    val request = AnalyzePlanRequest.newBuilder().setSessionId("abc123").build()

    // Failed the ssl handshake as the dummy server does not have any server credentials installed.
    assertThrows[SparkException] {
      client.analyze(request)
    }
  }

  test("SparkSession create with SPARK_REMOTE") {
    startDummyServer(0)

    withEnvs("SPARK_REMOTE" -> s"sc://localhost:${server.getPort}") {
      val session = SparkSession.builder().create()
      val df = session.range(10)
      df.analyze // Trigger RPC
      assert(df.plan === service.getAndClearLatestInputPlan())

      val session2 = SparkSession.builder().create()
      assert(session != session2)
    }
  }

  test("SparkSession getOrCreate with SPARK_REMOTE") {
    startDummyServer(0)

    withEnvs("SPARK_REMOTE" -> s"sc://localhost:${server.getPort}") {
      val session = SparkSession.builder().getOrCreate()

      val df = session.range(10)
      df.analyze // Trigger RPC
      assert(df.plan === service.getAndClearLatestInputPlan())

      val session2 = SparkSession.builder().getOrCreate()
      assert(session === session2)
    }
  }

  test("Builder.remote takes precedence over SPARK_REMOTE") {
    startDummyServer(0)
    val incorrectUrl = s"sc://localhost:${server.getPort + 1}"

    withEnvs("SPARK_REMOTE" -> incorrectUrl) {
      val session =
        SparkSession.builder().remote(s"sc://localhost:${server.getPort}").getOrCreate()

      val df = session.range(10)
      df.analyze // Trigger RPC
      assert(df.plan === service.getAndClearLatestInputPlan())
    }
  }

  test("SparkSession initialisation with connection string") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .build()

    val session = SparkSession.builder().client(client).create()
    val df = session.range(10)
    df.analyze // Trigger RPC
    assert(df.plan === service.getAndClearLatestInputPlan())
  }

  test("Custom Interceptor") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .interceptor(new ClientInterceptor {
        override def interceptCall[ReqT, RespT](
            methodDescriptor: MethodDescriptor[ReqT, RespT],
            callOptions: CallOptions,
            channel: Channel): ClientCall[ReqT, RespT] = {
          throw new RuntimeException("Blocked")
        }
      })
      .build()

    val session = SparkSession.builder().client(client).create()

    assertThrows[RuntimeException] {
      session.range(10).count()
    }
  }

  test("error framework parameters") {
    val errors = GrpcExceptionConverter.errorFactory
    for ((name, constructor) <- errors if name.startsWith("org.apache.spark")) {
      withClue(name) {
        val testParams = GrpcExceptionConverter.ErrorParams(
          message = "",
          cause = None,
          errorClass = Some("DUPLICATE_KEY"),
          messageParameters = Map("keyColumn" -> "`abc`"),
          queryContext = Array.empty)
        val error = constructor(testParams).asInstanceOf[Throwable with SparkThrowable]
        assert(error.getMessage.contains(testParams.message))
        assert(error.getCause == null)
        assert(error.getCondition == testParams.errorClass.get)
        assert(error.getMessageParameters.asScala == testParams.messageParameters)
        assert(error.getQueryContext.isEmpty)
      }
    }

    for ((name, constructor) <- errors if !name.startsWith("org.apache.spark")) {
      withClue(name) {
        val testParams = GrpcExceptionConverter.ErrorParams(
          message = "Found duplicate keys `abc`",
          cause = None,
          errorClass = None,
          messageParameters = Map.empty,
          queryContext = Array.empty)
        val error = constructor(testParams)
        assert(error.getMessage.contains(testParams.message))
        assert(error.getCause == null)
      }
    }
  }

  private case class TestPackURI(
      connectionString: String,
      isCorrect: Boolean,
      extraChecks: SparkConnectClient => Unit = _ => {})

  private val URIs = Seq[TestPackURI](
    TestPackURI("sc://host", isCorrect = true),
    TestPackURI(
      "sc://localhost/",
      isCorrect = true,
      client => testClientConnection(ConnectCommon.CONNECT_GRPC_BINDING_PORT)(_ => client)),
    TestPackURI(
      "sc://localhost:1234/",
      isCorrect = true,
      client => {
        assert(client.configuration.host == "localhost")
        assert(client.configuration.port == 1234)
        assert(client.sessionId != null)
        // Must be able to parse the UUID
        assert(UUID.fromString(client.sessionId) != null)
      }),
    TestPackURI(
      "sc://localhost/;",
      isCorrect = true,
      client => {
        assert(client.configuration.host == "localhost")
        assert(client.configuration.port == ConnectCommon.CONNECT_GRPC_BINDING_PORT)
      }),
    TestPackURI("sc://host:123", isCorrect = true),
    TestPackURI(
      "sc://host:123/;user_id=a94",
      isCorrect = true,
      client => assert(client.userId == "a94")),
    TestPackURI(
      "sc://host:123/;user_agent=a945",
      isCorrect = true,
      client => assert(client.userAgent.contains("a945"))),
    TestPackURI("scc://host:12", isCorrect = false),
    TestPackURI("http://host", isCorrect = false),
    TestPackURI("sc:/host:1234/path", isCorrect = false),
    TestPackURI("sc://host/path", isCorrect = false),
    TestPackURI("sc://host/;parm1;param2", isCorrect = false),
    TestPackURI("sc://host:123;user_id=a94", isCorrect = false),
    TestPackURI("sc:///user_id=123", isCorrect = false),
    TestPackURI("sc://host:-4", isCorrect = false),
    TestPackURI("sc://:123/", isCorrect = false),
    TestPackURI("sc://host:123/;use_ssl=true", isCorrect = true),
    TestPackURI("sc://host:123/;token=mySecretToken", isCorrect = true),
    TestPackURI("sc://host:123/;token=", isCorrect = false),
    TestPackURI("sc://host:123/;session_id=", isCorrect = false),
    TestPackURI("sc://host:123/;session_id=abcdefgh", isCorrect = false),
    TestPackURI(s"sc://host:123/;session_id=${UUID.randomUUID().toString}", isCorrect = true),
    TestPackURI("sc://host:123/;use_ssl=true;token=mySecretToken", isCorrect = true),
    TestPackURI("sc://host:123/;token=mySecretToken;use_ssl=true", isCorrect = true),
    TestPackURI("sc://host:123/;use_ssl=false;token=mySecretToken", isCorrect = true),
    TestPackURI("sc://host:123/;token=mySecretToken;use_ssl=false", isCorrect = true),
    TestPackURI("sc://host:123/;param1=value1;param2=value2", isCorrect = true),
    TestPackURI(
      "sc://SPARK-45486",
      isCorrect = true,
      client => {
        assert(client.userAgent.contains("spark/"))
        assert(client.userAgent.contains("scala/"))
        assert(client.userAgent.contains("jvm/"))
        assert(client.userAgent.contains("os/"))
      }),
    TestPackURI(
      "sc://SPARK-47694:123/;grpc_max_message_size=1860",
      isCorrect = true,
      client => {
        assert(client.configuration.grpcMaxMessageSize == 1860)
      }),
    TestPackURI("sc://SPARK-47694:123/;grpc_max_message_size=abc", isCorrect = false))

  private def checkTestPack(testPack: TestPackURI): Unit = {
    val client = SparkConnectClient.builder().connectionString(testPack.connectionString).build()
    testPack.extraChecks(client)
  }

  URIs.foreach { testPack =>
    test(s"Check URI: ${testPack.connectionString}, isCorrect: ${testPack.isCorrect}") {
      if (!testPack.isCorrect) {
        assertThrows[IllegalArgumentException](checkTestPack(testPack))
      } else {
        checkTestPack(testPack)
      }
    }
  }

  private class DummyFn(e: => Throwable, numFails: Int = 3) {
    var counter = 0
    def fn(): Int = {
      if (counter < numFails) {
        counter += 1
        throw e
      } else {
        42
      }
    }
  }

  test("SPARK-44721: Retries run for a minimum period") {
    // repeat test few times to avoid random flakes
    for (_ <- 1 to 10) {
      var totalSleepMs: Long = 0

      def sleep(t: Long): Unit = {
        totalSleepMs += t
      }

      val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE), numFails = 100)
      val retryHandler = new GrpcRetryHandler(RetryPolicy.defaultPolicies(), sleep)

      assertThrows[RetriesExceeded] {
        retryHandler.retry {
          dummyFn.fn()
        }
      }

      assert(totalSleepMs >= 10 * 60 * 1000) // waited at least 10 minutes
    }
  }

  test("SPARK-44275: retry actually retries") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE))
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = _ => {})
    val result = retryHandler.retry { dummyFn.fn() }

    assert(result == 42)
    assert(dummyFn.counter == 3)
  }

  test("SPARK-44275: default retryException retries only on UNAVAILABLE") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.ABORTED))
    val retryPolicies = RetryPolicy.defaultPolicies()
    val retryHandler = new GrpcRetryHandler(retryPolicies, sleep = _ => {})

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 1)
  }

  test("SPARK-44275: retry uses canRetry to filter exceptions") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE))
    val retryPolicy = RetryPolicy(canRetry = _ => false, name = "TestPolicy")
    val retryHandler = new GrpcRetryHandler(retryPolicy)

    assertThrows[StatusRuntimeException] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 1)
  }

  test("SPARK-44275: retry does not exceed maxRetries") {
    val dummyFn = new DummyFn(new StatusRuntimeException(Status.UNAVAILABLE))
    val retryPolicy = RetryPolicy(canRetry = _ => true, maxRetries = Some(1), name = "TestPolicy")
    val retryHandler = new GrpcRetryHandler(retryPolicy, sleep = _ => {})

    assertThrows[RetriesExceeded] {
      retryHandler.retry { dummyFn.fn() }
    }
    assert(dummyFn.counter == 2)
  }

  def testPolicySpecificError(maxRetries: Int, status: Status): RetryPolicy = {
    RetryPolicy(
      maxRetries = Some(maxRetries),
      name = s"Policy for ${status.getCode}",
      canRetry = {
        case e: StatusRuntimeException => e.getStatus.getCode == status.getCode
        case _ => false
      })
  }

  test("Test multiple policies") {
    val policy1 = testPolicySpecificError(maxRetries = 2, status = Status.UNAVAILABLE)
    val policy2 = testPolicySpecificError(maxRetries = 4, status = Status.INTERNAL)

    // Tolerate 2 UNAVAILABLE errors and 4 INTERNAL errors

    val errors = (List.fill(2)(Status.UNAVAILABLE) ++ List.fill(4)(Status.INTERNAL)).iterator

    new GrpcRetryHandler(List(policy1, policy2), sleep = _ => {}).retry({
      val e = errors.nextOption()
      if (e.isDefined) {
        throw e.get.asRuntimeException()
      }
    })

    assert(!errors.hasNext)
  }

  test("Test multiple policies exceed") {
    val policy1 = testPolicySpecificError(maxRetries = 2, status = Status.INTERNAL)
    val policy2 = testPolicySpecificError(maxRetries = 4, status = Status.INTERNAL)

    val errors = List.fill(10)(Status.INTERNAL).iterator
    var countAttempted = 0

    assertThrows[RetriesExceeded](
      new GrpcRetryHandler(List(policy1, policy2), sleep = _ => {}).retry({
        countAttempted += 1
        val e = errors.nextOption()
        if (e.isDefined) {
          throw e.get.asRuntimeException()
        }
      }))

    assert(countAttempted == 7)
  }

  test("ArtifactManager retries errors") {
    var attempt = 0

    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .interceptor(new ClientInterceptor {
        override def interceptCall[ReqT, RespT](
            methodDescriptor: MethodDescriptor[ReqT, RespT],
            callOptions: CallOptions,
            channel: Channel): ClientCall[ReqT, RespT] = {
          attempt += 1;
          if (attempt <= 3) {
            throw Status.UNAVAILABLE.withDescription("").asRuntimeException()
          }

          channel.newCall(methodDescriptor, callOptions)
        }
      })
      .build()

    val session = SparkSession.builder().client(client).create()
    val artifactFilePath = commonResourcePath.resolve("artifact-tests")
    val path = artifactFilePath.resolve("smallClassFile.class")
    assume(path.toFile.exists)
    session.addArtifact(path.toString)
  }

  private def buildPlan(query: String): proto.Plan = {
    proto.Plan
      .newBuilder()
      .setRoot(Relation.newBuilder().setSql(SQL.newBuilder().setQuery(query)).build())
      .build()
  }

  test("SPARK-45871: Client execute iterator.toSeq consumes the reattachable iterator") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .enableReattachableExecute()
      .build()

    val plan = buildPlan("select * from range(10000000)")
    val iter = client.execute(plan)
    val reattachableIter =
      ExecutePlanResponseReattachableIterator.fromIterator(iter)
    iter.toSeq
    // In several places in SparkSession, we depend on `.toSeq` to consume and close the iterator.
    // If this assertion fails, we need to double check the correctness of that.
    // In scala 2.12 `s.c.TraversableOnce#toSeq` builds an `immutable.Stream`,
    // which is a tail lazy structure and this would fail.
    // In scala 2.13 `s.c.IterableOnceOps#toSeq` builds an `immutable.Seq` which is not
    // lazy and will consume and close the iterator.
    assert(reattachableIter.resultComplete)
  }

  test("SPARK-45871: Client execute iterator.foreach consumes the reattachable iterator") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .enableReattachableExecute()
      .build()

    val plan = buildPlan("select * from range(10000000)")
    val iter = client.execute(plan)
    val reattachableIter =
      ExecutePlanResponseReattachableIterator.fromIterator(iter)
    iter.foreach(_ => ())
    assert(reattachableIter.resultComplete)
  }

  test("SPARK-48056: Client execute gets INVALID_HANDLE.SESSION_NOT_FOUND and proceeds") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .enableReattachableExecute()
      .build()
    service.errorToThrowOnExecute = Some(
      new StatusRuntimeException(
        Status.INTERNAL.withDescription("INVALID_HANDLE.SESSION_NOT_FOUND")))

    val plan = buildPlan("select * from range(1)")
    val iter = client.execute(plan)
    val reattachableIter =
      ExecutePlanResponseReattachableIterator.fromIterator(iter)
    reattachableIter.foreach(_ => ())
    assert(reattachableIter.resultComplete)
  }

  private val INVALID_HOST = "host.invalid"

  private def createUnresolvableHostChannel = {
    SparkConnectClient.Configuration(host = INVALID_HOST).createChannel()
  }

  private def assertContainsUnavailable(t: Throwable) = {
    assert(t.getMessage.contains("UNAVAILABLE: Unable to resolve host " + INVALID_HOST))
  }

  test("GRPC stub unary call throws error immediately") {
    // Spark Connect error retry handling depends on the error being returned from the unary
    // call immediately.
    val channel = createUnresolvableHostChannel
    val stub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)
    // The request is invalid, but it shouldn't even reach the server.
    val request = proto.AnalyzePlanRequest.newBuilder().build()

    // calling unary call immediately throws connection exception
    val ex = intercept[StatusRuntimeException] {
      stub.analyzePlan(request)
    }
    assertContainsUnavailable(ex)
  }

  test("GRPC stub server streaming call throws error on first next() / hasNext()") {
    // Spark Connect error retry handling depends on the error being returned from the response
    // iterator and not immediately upon iterator creation.
    val channel = createUnresolvableHostChannel
    val stub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)
    // The request is invalid, but it shouldn't even reach the server.
    val request = proto.ExecutePlanRequest.newBuilder().build()

    // creating the iterator doesn't throw exception
    val iter = stub.executePlan(request)
    // error is thrown only when the iterator is open.
    val ex = intercept[StatusRuntimeException] {
      iter.hasNext()
    }
    assertContainsUnavailable(ex)
  }

  test("GRPC stub client streaming call throws error on first client request sent") {
    // Spark Connect error retry handling depends on the error being returned from the response
    // iterator and not immediately upon iterator creation or request being sent.
    val channel = createUnresolvableHostChannel
    val stub = proto.SparkConnectServiceGrpc.newStub(channel)

    var onNextResponse: Option[proto.AddArtifactsResponse] = None
    var onErrorThrowable: Option[Throwable] = None
    var onCompletedCalled: Boolean = false

    val responseObserver = new StreamObserver[proto.AddArtifactsResponse] {
      override def onNext(value: proto.AddArtifactsResponse): Unit = {
        onNextResponse = Some(value)
      }

      override def onError(t: Throwable): Unit = {
        onErrorThrowable = Some(t)
      }

      override def onCompleted(): Unit = {
        onCompletedCalled = false
      }
    }

    // calling client streaming call doesn't throw exception
    val observer = stub.addArtifacts(responseObserver)

    // but exception will get returned on the responseObserver.
    Eventually.eventually(timeout(30.seconds)) {
      assert(onNextResponse == None)
      assert(onErrorThrowable.isDefined)
      assertContainsUnavailable(onErrorThrowable.get)
      assert(onCompletedCalled == false)
    }

    // despite that, requests can be sent to the request observer without error being thrown.
    observer.onNext(proto.AddArtifactsRequest.newBuilder().build())
    observer.onCompleted()
  }

  test("client can set a custom operation id for ExecutePlan requests") {
    startDummyServer(0)
    client = SparkConnectClient
      .builder()
      .connectionString(s"sc://localhost:${server.getPort}")
      .enableReattachableExecute()
      .build()

    val plan = buildPlan("select * from range(10000000)")
    val dummyUUID = "10a4c38e-7e87-40ee-9d6f-60ff0751e63b"
    val iter = client.execute(plan, operationId = Some(dummyUUID))
    val reattachableIter =
      ExecutePlanResponseReattachableIterator.fromIterator(iter)
    assert(reattachableIter.operationId == dummyUUID)
    while (reattachableIter.hasNext) {
      val resp = reattachableIter.next()
      assert(resp.getOperationId == dummyUUID)
    }
  }
}

class DummySparkConnectService() extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

  private var inputPlan: proto.Plan = _
  private val inputArtifactRequests: mutable.ListBuffer[AddArtifactsRequest] =
    mutable.ListBuffer.empty

  var errorToThrowOnExecute: Option[Throwable] = None

  private[sql] def getAndClearLatestInputPlan(): proto.Plan = synchronized {
    val plan = inputPlan
    inputPlan = null
    plan
  }

  private[sql] def getAndClearLatestAddArtifactRequests(): Seq[AddArtifactsRequest] =
    synchronized {
      val requests = inputArtifactRequests.toSeq
      inputArtifactRequests.clear()
      requests
    }

  override def executePlan(
      request: ExecutePlanRequest,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    synchronized {
      if (errorToThrowOnExecute.isDefined) {
        val error = errorToThrowOnExecute.get
        errorToThrowOnExecute = None
        responseObserver.onError(error)
        return
      }
    }

    // Reply with a dummy response using the same client ID
    val requestSessionId = request.getSessionId
    val operationId = if (request.hasOperationId) {
      request.getOperationId
    } else {
      UUID.randomUUID().toString
    }
    synchronized {
      inputPlan = request.getPlan
    }
    val response = ExecutePlanResponse
      .newBuilder()
      .setSessionId(requestSessionId)
      .setOperationId(operationId)
      .build()
    responseObserver.onNext(response)
    // Reattachable execute must end with ResultComplete
    if (request.getRequestOptionsList.asScala.exists { option =>
        option.hasReattachOptions && option.getReattachOptions.getReattachable
      }) {
      val resultComplete = ExecutePlanResponse
        .newBuilder()
        .setSessionId(requestSessionId)
        .setOperationId(operationId)
        .setResultComplete(proto.ExecutePlanResponse.ResultComplete.newBuilder().build())
        .build()
      responseObserver.onNext(resultComplete)
    }
    responseObserver.onCompleted()
  }

  override def analyzePlan(
      request: AnalyzePlanRequest,
      responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestSessionId = request.getSessionId
    synchronized {
      request.getAnalyzeCase match {
        case proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA =>
          inputPlan = request.getSchema.getPlan
        case proto.AnalyzePlanRequest.AnalyzeCase.EXPLAIN =>
          inputPlan = request.getExplain.getPlan
        case proto.AnalyzePlanRequest.AnalyzeCase.TREE_STRING =>
          inputPlan = request.getTreeString.getPlan
        case proto.AnalyzePlanRequest.AnalyzeCase.IS_LOCAL =>
          inputPlan = request.getIsLocal.getPlan
        case proto.AnalyzePlanRequest.AnalyzeCase.IS_STREAMING =>
          inputPlan = request.getIsStreaming.getPlan
        case proto.AnalyzePlanRequest.AnalyzeCase.INPUT_FILES =>
          inputPlan = request.getInputFiles.getPlan
        case _ => inputPlan = null
      }
    }
    val response = AnalyzePlanResponse
      .newBuilder()
      .setSessionId(requestSessionId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def addArtifacts(responseObserver: StreamObserver[AddArtifactsResponse])
      : StreamObserver[AddArtifactsRequest] = new StreamObserver[AddArtifactsRequest] {
    override def onNext(v: AddArtifactsRequest): Unit =
      synchronized(inputArtifactRequests.append(v))

    override def onError(throwable: Throwable): Unit = responseObserver.onError(throwable)

    override def onCompleted(): Unit = {
      responseObserver.onNext(proto.AddArtifactsResponse.newBuilder().build())
      responseObserver.onCompleted()
    }
  }

  override def artifactStatus(
      request: ArtifactStatusesRequest,
      responseObserver: StreamObserver[ArtifactStatusesResponse]): Unit = {
    val builder = proto.ArtifactStatusesResponse.newBuilder()
    request.getNamesList().iterator().asScala.foreach { name =>
      val status = proto.ArtifactStatusesResponse.ArtifactStatus.newBuilder()
      val exists = if (name.startsWith("cache/")) {
        synchronized {
          inputArtifactRequests.exists { artifactReq =>
            if (artifactReq.hasBatch) {
              val batch = artifactReq.getBatch
              batch.getArtifactsList.asScala.exists { singleArtifact =>
                singleArtifact.getName == name
              }
            } else false
          }
        }
      } else false
      builder.putStatuses(name, status.setExists(exists).build())
    }
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  override def interrupt(
      request: proto.InterruptRequest,
      responseObserver: StreamObserver[proto.InterruptResponse]): Unit = {
    val response = proto.InterruptResponse.newBuilder().setSessionId(request.getSessionId).build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def reattachExecute(
      request: proto.ReattachExecuteRequest,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    // Reply with a dummy response using the same client ID
    val requestSessionId = request.getSessionId
    val response = ExecutePlanResponse
      .newBuilder()
      .setSessionId(requestSessionId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  override def releaseExecute(
      request: proto.ReleaseExecuteRequest,
      responseObserver: StreamObserver[proto.ReleaseExecuteResponse]): Unit = {
    val response = proto.ReleaseExecuteResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setOperationId(request.getOperationId)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}
