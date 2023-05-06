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
	"bytes"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/spark/go/v_3_4/generated/proto"
	"io"
)

type DataFrame interface {
	Show(numRows int, truncate bool) error
	Collect() ([]Row, error)
}

type dataFrameImpl struct {
	sparkSession *sparkSessionImpl
	relation     *proto.Relation // TODO change to proto.Plan?
}

func (df *dataFrameImpl) Show(numRows int, truncate bool) error {
	truncateValue := 0
	if truncate {
		truncateValue = 20
	}
	vertical := false

	plan := &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: &proto.Relation_ShowString{
					ShowString: &proto.ShowString{
						Input:    df.relation,
						NumRows:  int32(numRows),
						Truncate: int32(truncateValue),
						Vertical: vertical,
					},
				},
			},
		},
	}

	responseClient, err := df.sparkSession.executePlan(plan)
	if err != nil {
		return fmt.Errorf("failed to show dataframe: %w", err)
	}

	for {
		response, err := responseClient.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive show response: %w", err)
		}
		arrowBatch := response.GetArrowBatch()
		if arrowBatch == nil {
			continue
		}
		err = showArrowBatch(arrowBatch)
		if err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("did not get arrow batch in response")
}

func (df *dataFrameImpl) Collect() ([]Row, error) {
	plan := &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: df.relation.RelType,
			},
		},
	}

	responseClient, err := df.sparkSession.executePlan(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute plan in Collect: %w", err)
	}

	for {
		response, err := responseClient.Recv()
		if err != nil {
			return nil, fmt.Errorf("failed to receive plan execution response: %w", err)
		}
		arrowBatch := response.GetArrowBatch()
		if arrowBatch == nil {
			continue
		}

		// TODO convert arrowBatch to []Row

		return nil, nil
	}

	return nil, fmt.Errorf("did not get arrow batch in response")
}

func showArrowBatch(arrowBatch *proto.ExecutePlanResponse_ArrowBatch) error {
	return showArrowBatchData(arrowBatch.Data)
}

func showArrowBatchData(data []byte) error {
	reader := bytes.NewReader(data)
	arrowReader, err := ipc.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create arrow reader: %w", err)
	}
	defer arrowReader.Release()

	record, err := arrowReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("failed to read arrow: %w", err)
	}
	numRows := record.NumRows()
	arrayData := record.Column(0).Data()
	typeId := arrayData.DataType().ID()
	if typeId != arrow.STRING {
		return fmt.Errorf("arrow column type is not string")
	}
	stringData := array.NewStringData(arrayData)
	for i := int64(0); i < numRows; i++ {
		v := stringData.Value(int(i))
		fmt.Println(v)
	}

	// TODO handle next?
	for arrowReader.Next() {
	}

	return nil
}
