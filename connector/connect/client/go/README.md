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

## How to Run Spark Connect Go Application

1. Install Golang: https://go.dev/doc/install.

2. Download Spark distribution (3.4.0+), unzip the folder.

3. Start Spark Connect server by running command:

```
sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
```

4. In this repo, `cd connector/connect/client/go`, run Go application:

```
go run examples/spark-connect-example-spark-session/main.go
```

## How to Generate protobuf Go Binding

```
make internal/generated.out
```

## Inner Development Loop

Build all targets:

```
make
```

Run all tests:

```
make test
```


Run all tests with code coverage:

```
make fulltest
```
