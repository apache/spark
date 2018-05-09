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

package org.apache.spark.examples.ml;

// $example on$

import org.apache.spark.ml.clustering.PowerIterationClustering;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

// $example off$

/**
 * An example demonstrating PowerIterationClusteringExample.
 * Run with
 * <pre>
 * bin/run-example ml.JavaPowerIterationClusteringExample
 * </pre>
 */
public class JavaPowerIterationClusteringExample {

    public static void main(String[] args) {
        // Create a SparkSession.
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPowerIterationClustering")
                .getOrCreate();

        // $example on$
        // Creates data.
        List<Row> data = Arrays.asList(
                RowFactory.create(0L, Arrays.asList(1L), Arrays.asList(0.9)),
                RowFactory.create(1L, Arrays.asList(2L), Arrays.asList(0.9)),
                RowFactory.create(2L, Arrays.asList(3L), Arrays.asList(0.9)),
                RowFactory.create(3L, Arrays.asList(4L), Arrays.asList(0.1)),
                RowFactory.create(4L, Arrays.asList(5L), Arrays.asList(0.9))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("neighbors", DataTypes.createArrayType(DataTypes.LongType, false), false, Metadata.empty()),
                new StructField("similarities", DataTypes.createArrayType(DataTypes.DoubleType, false), false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        PowerIterationClustering pic = new PowerIterationClustering()
                .setK(2)
                .setMaxIter(10);

        Dataset <Row> result = pic.transform(df).select("id", "prediction");

        // printing results
        System.out.println("Clustering results [id , cluster]");
        for (Row row : result.collectAsList()) {
            System.out.println("[" + row.get(0) + " , " + row.get(1) + "]");
        }

        // $example off$
        spark.stop();
    }
}
