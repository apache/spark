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

package sql

import (
	"context"
	"fmt"

	proto "github.com/apache/spark/go/v_3_4/internal/generated"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var SparkSession sparkSessionBuilderEntrypoint

type sparkSession interface {
	Sql(query string) (DataFrame, error)
	Stop() error
}

type sparkSessionBuilderEntrypoint struct {
	Builder SparkSessionBuilder
}

type SparkSessionBuilder struct {
	connectionString string
}

func (s SparkSessionBuilder) Remote(connectionString string) SparkSessionBuilder {
	copy := s
	copy.connectionString = connectionString
	return copy
}

func (s SparkSessionBuilder) Build() (sparkSession, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial(s.connectionString, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err)
	}

	client := proto.NewSparkConnectServiceClient(conn)
	return &sparkSessionImpl{
		sessionId: uuid.NewString(),
		client:    client,
	}, nil
}

type sparkSessionImpl struct {
	sessionId string
	client    proto.SparkConnectServiceClient
}

func (s *sparkSessionImpl) Sql(query string) (DataFrame, error) {
	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_SqlCommand{
					SqlCommand: &proto.SqlCommand{
						Sql: query,
					},
				},
			},
		},
	}
	responseClient, err := s.executePlan(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql: %s: %w", query, err)
	}
	for {
		response, err := responseClient.Recv()
		if err != nil {
			return nil, fmt.Errorf("failed to receive ExecutePlan response: %w", err)
		}
		sqlCommandResult := response.GetSqlCommandResult()
		if sqlCommandResult == nil {
			continue
		}
		return &dataFrameImpl{
			sparkSession: s,
			relation:     sqlCommandResult.GetRelation(),
		}, nil
	}
	return nil, fmt.Errorf("failed to get SqlCommandResult in ExecutePlan response")
}

func (s *sparkSessionImpl) Stop() error {
	return nil
}

func (s *sparkSessionImpl) executePlan(plan *proto.Plan) (proto.SparkConnectService_ExecutePlanClient, error) {
	request := proto.ExecutePlanRequest{
		SessionId: s.sessionId,
		Plan:      plan,
	}
	executePlanClient, err := s.client.ExecutePlan(context.TODO(), &request)
	if err != nil {
		return nil, fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err)
	}
	return executePlanClient, nil
}

func (s *sparkSessionImpl) analyzePlan(plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	request := proto.AnalyzePlanRequest{
		SessionId: s.sessionId,
		Analyze: &proto.AnalyzePlanRequest_Schema_{
			Schema: &proto.AnalyzePlanRequest_Schema{
				Plan: plan,
			},
		},
	}
	response, err := s.client.AnalyzePlan(context.TODO(), &request)
	if err != nil {
		return nil, fmt.Errorf("failed to call AnalyzePlan in session %s: %w", s.sessionId, err)
	}
	return response, nil
}
