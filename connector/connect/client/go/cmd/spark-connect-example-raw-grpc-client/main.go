//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/apache/spark/go/v_3_4/pkg/generated/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	remote = flag.String("remote", "localhost:15002", "the remote address of Spark Connect server to connect to")
)

func main() {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial(*remote, opts...)
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}
	defer conn.Close()

	client := proto.NewSparkConnectServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	configRequest := proto.ConfigRequest{
		SessionId: uuid.NewString(),
		Operation: &proto.ConfigRequest_Operation{
			OpType: &proto.ConfigRequest_Operation_GetAll{
				GetAll: &proto.ConfigRequest_GetAll{},
			},
		},
	}
	configResponse, err := client.Config(ctx, &configRequest)
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	log.Printf("configResponse: %v", configResponse)
}
