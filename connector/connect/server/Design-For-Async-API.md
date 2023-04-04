# Async API Design

## Purpose

By implementing the asynchronous API, users of Spark Connect can use the asynchronous API in Spark,
thereby improving the performance and flexibility of Spark Connect.

## Ability after completion

JVM Client:

```scala

// sync
spark.read.schema(schema).csv(ds)
// async
spark.read.schema(schema).AsyncCsv(ds)

// sync
spark.createDataFrame(data.asJava, schema).collect()
// async
spark.createDataFrame(data.asJava, schema).AsyncCollect()

// sync
df.write
  .format("csv")
  .mode("overwrite")
  .options(Map("header" -> "true", "delimiter" -> ";"))
  .save(outputFolderPath.toString)

// async
df.write
  .format("csv")
  .mode("overwrite")
  .options(Map("header" -> "true", "delimiter" -> ";"))
  .AsyncSave(outputFolderPath.toString)
```

The difference is that an async prefix is added in front of all Action operations name to form a new function.
Python is the same

For the JVM client, the implementation class of the Scala Future object can be obtained by calling the method of the
Async series.

For the Python client, the implementation class of the Python Future object can be obtained by calling the method of the
Async series.

The progress and results of asynchronous calls can be queried through this class.

### protobuf definition

```protobuf
syntax = 'proto3';

// A request to be executed by the service.
message ExecutePlanRequest {
  // (Required)
  //
  // The session_id specifies a spark session for a user id (which is specified
  // by user_context.user_id). The session_id is set by the client to be able to
  // collate streaming responses from different queries within the dedicated session.
  string session_id = 1;

  // (Required) User context
  //
  // user_context.user_id and session+id both identify a unique remote spark session on the
  // server side.
  UserContext user_context = 2;

  // (Required) The logical plan to be executed / analyzed.
  Plan plan = 3;

  // Provides optional information about the client sending the request. This field
  // can be used for language or version specific information and is only intended for
  // logging purposes and will not be interpreted by the server.
  optional string client_type = 4;

  // ** NEW **
  // Add an identifier to mark whether this request is asynchronous
  bool async = 5;

  // ** NEW **
  // (Required)
  //
  // The request_id specifies a spark request for a session id. 
  // The request_id is set by the client to be able to collate streaming responses.
  // Also can check the status of the request use request_id.
  string request_id = 6;

}

// ** NEW **
// Information used to specify a single request for a single session
message ExecuteRequestInfo {

  // (Required)
  //
  // The request_id specifies a spark request for a session id. 
  // The request_id is set by the client to be able to collate streaming responses.
  // Also can check the status of the request use request_id.
  string request_id = 1;

  // (Required) User context
  //
  // user_context.user_id and session+id both identify a unique remote spark session on the
  // server side.
  UserContext user_context = 2;

  // (Required)
  //
  // The session_id specifies a spark session for a user id (which is specified
  // by user_context.user_id). The session_id is set by the client to be able to
  // collate streaming responses from different queries within the dedicated session.
  string session_id = 3;

}

message Command {
  oneof command_type {
    CommonInlineUserDefinedFunction register_function = 1;
    WriteOperation write_operation = 2;
    CreateDataFrameViewCommand create_dataframe_view = 3;
    WriteOperationV2 write_operation_v2 = 4;
    SqlCommand sql_command = 5;

    // ** NEW **
    // Add new query request status command
    QueryRequestStatus query_request_status = 6;

    // This field is used to mark extensions to the protocol. When plugins generate arbitrary
    // Commands they can add them here. During the planning the correct resolution is done.
    google.protobuf.Any extension = 999;

  }
}

// ** NEW **
// Just a command for get request status
message QueryRequestStatus {
}



service SparkConnectService {

  // ** NEW **
  rpc GetExecuteResponse(ExecuteRequestInfo) returns (stream ExecutePlanResponse) {}

}


```

## Server-side implementation

### Implementation of Spark Connect Server

The implementation of Spark Connect Server is mainly to add support for asynchronous processing logic on the basis of
the original Spark Connect Server.
Pass StreamObserver into each method to stream data at now. After adding asynchronous API,
we need to refactor the existing method and change the incoming `StreamObserver` to incoming
The abstract class similar to `Queue` is used as a parameter, and the `onNext` method of `StreamObserver` in the
original method is changed to the `put` method of `Queue`,
so that data can be passed into Queue.
At the same time, Queue is divided into two implementations. The synchronization implementation is based
on `StreamObserver`. Every time the `put` method of `Queue` is called,
the `onNext` method of `StreamObserver` will be called. Asynchronous implementation
It is to store the data passed in by `Queue` call put into the cache and wait for the client to get it.

```scala

trait ResponseQueue {

  def put(response: ExecutePlanResponse): Unit

  def done(): Unit

}

class asyncResponseQueue(sessionId: String, request_id: String) extends ResponseQueue {

  private val cacheQueue: Queue[ExecutePlanResponse]

  override def put(response: ExecutePlanResponse): Unit = {
    cacheQueue.put(response)
  }

  override def done(): Unit = {
    cacheQueue.put(null)
  }

  def getResponse: ExecutePlanResponse = {
    cacheQueue.take()
  }

}

class syncResponseQueue(sessionId: String, request_id: String) extends ResponseQueue {

  private val streamObserver: StreamObserver[ExecutePlanResponse]

  override def put(response: ExecutePlanResponse): Unit = {
    streamObserver.onNext(response)
  }

  override def done(): Unit = {
    streamObserver.onCompleted()
  }

}

```

### Data Cache

As just mentioned, asynchronous calls will have data that needs to be cached on the server side, because it is not known
when the client will come to fetch the data.
So we need an object to cache all the data,
At the same time, it supports TTL to clean up the data that has not been acquired by the client over time
to ensure the normal operation of the server

We can support cache data in memory, but can support cache into file in the future.

