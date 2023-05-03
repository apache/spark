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
