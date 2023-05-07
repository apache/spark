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
	proto "github.com/apache/spark/go/v_3_4/internal/generated"
	"io"
)

type DataFrame interface {
	Show(numRows int, truncate bool) error
	Schema() (*StructType, error)
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

func (df *dataFrameImpl) Schema() (*StructType, error) {
	response, err := df.sparkSession.analyzePlan(df.createPlan())
	if err != nil {
		return nil, fmt.Errorf("failed to analyze plan: %w", err)
	}

	responseSchema := response.GetSchema().Schema
	result := convertProtoDataTypeToStructType(responseSchema)
	return result, nil
}

func (df *dataFrameImpl) Collect() ([]Row, error) {
	responseClient, err := df.sparkSession.executePlan(df.createPlan())
	if err != nil {
		return nil, fmt.Errorf("failed to execute plan: %w", err)
	}

	var schema *StructType
	var allRows []Row

	for {
		response, err := responseClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return allRows, nil
			} else {
				return nil, fmt.Errorf("failed to receive plan execution response: %w", err)
			}
		}

		dataType := response.GetSchema()
		if dataType != nil {
			schema = convertProtoDataTypeToStructType(dataType)
			continue
		}

		arrowBatch := response.GetArrowBatch()
		if arrowBatch == nil {
			continue
		}

		rowBatch, err := readArrowBatchData(arrowBatch.Data, schema)
		if err != nil {
			return nil, err
		}

		if allRows == nil {
			allRows = make([]Row, 0, len(rowBatch))
		}
		allRows = append(allRows, rowBatch...)
	}

	return allRows, nil
}

func (df *dataFrameImpl) createPlan() *proto.Plan {
	return &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: df.relation.RelType,
			},
		},
	}
}

func showArrowBatch(arrowBatch *proto.ExecutePlanResponse_ArrowBatch) error {
	return showArrowBatchData(arrowBatch.Data)
}

func showArrowBatchData(data []byte) error {
	rows, err := readArrowBatchData(data, nil)
	if err != nil {
		return err
	}
	for _, row := range rows {
		values, err := row.Values()
		if err != nil {
			return fmt.Errorf("failed to get values in the row: %w", err)
		}
		fmt.Println(values...)
	}
	return nil
}

func readArrowBatchData(data []byte, schema *StructType) ([]Row, error) {
	reader := bytes.NewReader(data)
	arrowReader, err := ipc.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}
	defer arrowReader.Release()

	var rows []Row

	for {
		record, err := arrowReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return rows, nil
			} else {
				return nil, fmt.Errorf("failed to read arrow: %w", err)
			}
		}
		numColumns := len(arrowReader.Schema().Fields())
		numRows := int(record.NumRows())
		if rows == nil {
			rows = make([]Row, 0, numRows)
		}
		values := make([][]any, numRows)
		for i := range values {
			values[i] = make([]any, numColumns)
		}
		for columnIndex := 0; columnIndex < numColumns; columnIndex++ {
			columnData := record.Column(columnIndex).Data()
			dataTypeId := columnData.DataType().ID()
			switch dataTypeId {
			case arrow.STRING:
				vector := array.NewStringData(columnData)
				for rowIndex := 0; rowIndex < numRows; rowIndex++ {
					values[rowIndex][columnIndex] = vector.Value(rowIndex)
				}
			case arrow.INT32:
				vector := array.NewInt32Data(columnData)
				for rowIndex := 0; rowIndex < numRows; rowIndex++ {
					values[rowIndex][columnIndex] = vector.Value(rowIndex)
				}
			case arrow.INT64:
				vector := array.NewInt64Data(columnData)
				for rowIndex := 0; rowIndex < numRows; rowIndex++ {
					values[rowIndex][columnIndex] = vector.Value(rowIndex)
				}
			default:
				return nil, fmt.Errorf("unsupported arrow data type %s in column %d", dataTypeId.String(), columnIndex)
			}
		}

		for _, v := range values {
			row := &GenericRowWithSchema{
				schema: schema,
				values: v,
			}
			rows = append(rows, row)
		}

		hasNext := arrowReader.Next()
		if !hasNext {
			break
		}
	}

	return rows, nil
}

func convertProtoDataTypeToStructType(input *proto.DataType) *StructType {
	dataTypeStruct := input.GetStruct()
	if dataTypeStruct == nil {
		panic("dataType.GetStruct() is nil")
	}
	return &StructType{
		Fields: convertProtoStructFields(dataTypeStruct.Fields),
	}
}

func convertProtoStructFields(input []*proto.DataType_StructField) []StructField {
	result := make([]StructField, len(input))
	for i, f := range input {
		result[i] = convertProtoStructField(f)
	}
	return result
}

func convertProtoStructField(field *proto.DataType_StructField) StructField {
	return StructField{
		Name:     field.Name,
		DataType: convertProtoDataTypeToDataType(field.DataType),
	}
}

func convertProtoDataTypeToDataType(input *proto.DataType) DataType {
	switch v := input.GetKind().(type) {
	case *proto.DataType_Integer_:
		return IntegerType{}
	case *proto.DataType_String_:
		return StringType{}
	default:
		return UnsupportedType{
			TypeInfo: v,
		}
	}
}
