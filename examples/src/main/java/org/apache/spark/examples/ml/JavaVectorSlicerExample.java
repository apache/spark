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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaVectorSlicerExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaVectorSlicerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    Attribute[] attrs = new Attribute[]{
      NumericAttribute.defaultAttr().withName("f1"),
      NumericAttribute.defaultAttr().withName("f2"),
      NumericAttribute.defaultAttr().withName("f3")
    };
    AttributeGroup group = new AttributeGroup("userFeatures", attrs);

    JavaRDD<Row> jrdd = jsc.parallelize(Lists.newArrayList(
      RowFactory.create(Vectors.sparse(3, new int[]{0, 1}, new double[]{-2.0, 2.3})),
      RowFactory.create(Vectors.dense(-2.0, 2.3, 0.0))
    ));

    DataFrame dataset = jsql.createDataFrame(jrdd, (new StructType()).add(group.toStructField()));

    VectorSlicer vectorSlicer = new VectorSlicer()
      .setInputCol("userFeatures").setOutputCol("features");

    vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});
    // or slicer.setIndices(new int[]{1, 2}), or slicer.setNames(new String[]{"f2", "f3"})

    DataFrame output = vectorSlicer.transform(dataset);

    System.out.println(output.select("userFeatures", "features").first());
    // $example off$
    jsc.stop();
  }
}

