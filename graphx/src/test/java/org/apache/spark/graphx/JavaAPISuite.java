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


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.api.java.JavaEdgeRDD;
import org.apache.spark.graphx.api.java.JavaVertexRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaAPISuite implements Serializable {

    private transient JavaSparkContext ssc;
    private List<Tuple2<Object, VertexProperty<String, String>>> myList;
    private ClassTag<VertexProperty<String, String>> classTag;


    @Before
    public void before() {
        this.ssc = new JavaSparkContext("local", "GraphX JavaAPISuite");

        this.myList = new ArrayList<Tuple2<Object, VertexProperty<String, String>>>();
        this.myList.add(new Tuple2(1L, new VertexProperty("001", "kushal")));
        this.myList.add(new Tuple2(2L, new VertexProperty("002", "xia")));
        this.myList.add(new Tuple2(3L, new VertexProperty("003", "briton")));

        this.classTag = ClassTag$.MODULE$.apply(VertexProperty.class);
    }

    @After
    public void after() {
        ssc.stop();
        ssc = null;
    }

    private class VertexProperty<T1, T2> implements Serializable {
        T1 field1;
        T2 field2;

        VertexProperty(T1 field1, T2 field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        T1 getField1() { return field1; }
        T2 getField2() { return field2; }
        void setField1(T1 value) { this.field1 = value; }
        void setField2(T2 value) { this.field2 = value; }
    }

    @Test
    public void testVertexRDDCount() {

        JavaRDD<Tuple2<Object, VertexProperty<String, String>>>
                javaRDD = ssc.parallelize(this.myList);

        JavaVertexRDD<VertexProperty<String, String>> javaVertexRDD =
                JavaVertexRDD.apply(javaRDD, this.classTag);

        assertEquals(3L, javaVertexRDD.count().intValue());
    }

    @Test
    public void testEdgeRDDCount() {

        List<Edge<String>> edgeList = new ArrayList<Edge<String>>();
        edgeList.add(new Edge<String>(0, 1, "abcd"));
        edgeList.add(new Edge<String>(1, 2, "defg"));
        edgeList.add(new Edge<String>(2, 3, "hijk"));
        edgeList.add(new Edge<String>(1, 3, "lmno"));

        JavaRDD<Edge<String>> javaRDD = ssc.parallelize(edgeList);

        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);

        JavaEdgeRDD<String> javaEdgeRDD =
                JavaEdgeRDD.apply(javaRDD, classTag);

        assertEquals(javaEdgeRDD.count().longValue(), 4L);
    }
}
