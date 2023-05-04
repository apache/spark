## Summary

This folder contains Spark Connect Go client implementation. People could reference to Go module (library) in this folder
and write Spark Connect Go application connecting to a remote Spark driver (Spark Connect server).

## Spark Connect Go Application Example

A very simple example in Go looks like following:

```
func main() {
	remote := "localhost:15002"
	spark, _ := sql.SparkSession.Builder.Remote(remote).Build()
	defer spark.Stop()

	df, _ := spark.Sql("select 'apple' as word, 123 as count union all select 'orange' as word, 456 as count")
	df.Show(100, false)
}
```

## High Level Design

Following [diagram](https://textik.com/#ac299c8f32c4c342) shows main code in current prototype:

```
    +-------------------+                                                                              
    |                   |                                                                              
    |   dataFrameImpl   |                                                                              
    |                   |                                                                              
    +-------------------+                                                                              
              |                                                                                        
              |                                                                                        
              +                                                                                        
    +-------------------+                                                                              
    |                   |                                                                              
    | sparkSessionImpl  |                                                                              
    |                   |                                                                              
    +-------------------+                                                                              
              |                                                                                        
              |                                                                                        
              +                                                                                        
+---------------------------+               +----------------+                                         
|                           |               |                |                                         
| SparkConnectServiceClient |--------------+|  Spark Driver  |                                         
|                           |               |                |                                         
+---------------------------+               +----------------+

```

`SparkConnectServiceClient` is GRPC client which talks to Spark Driver. `sparkSessionImpl` generates `dataFrameImpl`
instances. `dataFrameImpl` uses the GRPC client in `sparkSessionImpl` to communicate with Spark Driver.

We will mimic the logic in Spark Connect Scala implementation, and adopt Go common practices, e.g. returning `error` object for
error handling.

## Misc Helper Information

- Prepare your environment to generate proto Go files:

See https://grpc.io/docs/languages/go/quickstart/

- Command example to generate protobuf Go files:

```
gen_package=connector/connect/client/go/generated/proto
protoc -I=./connector/connect/common/src/main/protobuf/ \
  --go_out=./ \
  --go_opt=Mspark/connect/base.proto=$gen_package \
  --go_opt=Mspark/connect/catalog.proto=$gen_package \
  --go_opt=Mspark/connect/commands.proto=$gen_package \
  --go_opt=Mspark/connect/common.proto=$gen_package \
  --go_opt=Mspark/connect/example_plugins.proto=$gen_package \
  --go_opt=Mspark/connect/expressions.proto=$gen_package \
  --go_opt=Mspark/connect/relations.proto=$gen_package \
  --go_opt=Mspark/connect/types.proto=$gen_package \
  --go-grpc_out=./ \
  --go-grpc_opt=Mspark/connect/base.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/catalog.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/commands.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/common.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/example_plugins.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/expressions.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/relations.proto=$gen_package \
  --go-grpc_opt=Mspark/connect/types.proto=$gen_package \
  ./connector/connect/common/src/main/protobuf/spark/connect/*.proto
```

- Command example to start Spark connect server:

```
sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
```
