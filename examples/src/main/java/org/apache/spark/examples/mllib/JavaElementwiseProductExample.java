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

package org.apache.spark.examples.mllib;

// $example on$
import java.util.Arrays;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

public class JavaElementwiseProductExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaElementwiseProductExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // Create some vector data; also works for sparse vectors
    JavaRDD<Vector> data = jsc.parallelize(Arrays.asList(
      Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0)));
    Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);
    ElementwiseProduct transformer = new ElementwiseProduct(transformingVector);

    // Batch transform and per-row transform give the same results:
    JavaRDD<Vector> transformedData = transformer.transform(data);
    JavaRDD<Vector> transformedData2 = data.map(transformer::transform);
    // $example off$

    System.out.println("transformedData: ");
    transformedData.foreach(System.out::println);

    System.out.println("transformedData2: ");
    transformedData2.foreach(System.out::println);

    jsc.stop();
  }
}
