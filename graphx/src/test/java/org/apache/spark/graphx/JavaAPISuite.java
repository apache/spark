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

package org.apache.spark.graphx;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.api.java.JavaVertexRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaAPISuite implements Serializable {

    private transient JavaSparkContext ssc;

    @Before
    public void initialize() {
        this.ssc = new JavaSparkContext("local", "GraphX JavaAPISuite");
    }

    @After
    public void finalize() {
        ssc.stop();
        ssc = null;
    }

    @Test
    public void testCount() {
        List<Tuple2<Long, Tuple2<String, String>>> myList =
                new ArrayList<Tuple2<Long, Tuple2<String, String>>>();
        myList.add(new Tuple2(1L, new Tuple2("abc", "XYZ")));
        myList.add(new Tuple2(2L, new Tuple2("def", "SFN")));
        myList.add(new Tuple2(3L, new Tuple2("xyz", "XYZ")));
        JavaRDD<Tuple2<Long, Tuple2<String, String>>> javaRDD = ssc.parallelize(myList);
        JavaVertexRDD javaVertexRDD = new JavaVertexRDD(javaRDD.rdd());
        assertEquals(javaVertexRDD.count(), 3);
    }
}
