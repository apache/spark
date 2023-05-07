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
	"flag"
	"log"

	"github.com/apache/spark/go/v_3_4/pkg/spark/sql"
)

var (
	remote = flag.String("remote", "localhost:15002",
		"the remote address of Spark Connect server to connect to")
)

func main() {
	spark, err := sql.SparkSession.Builder.Remote(*remote).Build()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}
	defer spark.Stop()

	df, err := spark.Sql("select 'apple' as word, 123 as count union all select 'orange' as word, 456 as count")
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	err = df.Show(100, false)
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	schema, err := df.Schema()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	for _, f := range schema.Fields {
		log.Printf("Field in dataframe schema: %s - %s", f.Name, f.DataType.TypeName())
	}

	rows, err := df.Collect()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	schema, err = rows[0].Schema()
	if err != nil {
		log.Fatalf("Failed: %s", err.Error())
	}

	for _, f := range schema.Fields {
		log.Printf("Field in row: %s - %s", f.Name, f.DataType.TypeName())
	}

	for _, row := range rows {
		log.Printf("Row: %v", row)
	}
}
