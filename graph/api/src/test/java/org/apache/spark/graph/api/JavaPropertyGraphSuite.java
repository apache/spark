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

package org.apache.spark.graph.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.HashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class JavaPropertyGraphSuite implements Serializable {
    private transient TestSparkSession spark;
    private transient CypherSession cypherSession;
    private transient JavaSparkContext jsc;

    @Before
    public void setUp() {
        // Trigger static initializer of TestData
        spark = new TestSparkSession();

        jsc = new JavaSparkContext(spark.sparkContext());
        spark.loadTestData();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    private StructType createSchema(List<String> fieldNames, List<DataType> dataTypes) {
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            fields.add(DataTypes.createStructField(fieldNames.get(i), dataTypes.get(i), true));
        }
        return DataTypes.createStructType(fields);
    }

    @Test
    public void testCreateFromNodeFrame() {
        StructType personSchema = createSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(DataTypes.LongType, DataTypes.StringType));

        List<Row> personData = Arrays.asList(
                RowFactory.create(0L, "Alice"),
                RowFactory.create(1L, "Bob"));

        Dataset<Row> personDf = spark.createDataFrame(personData, personSchema);

        Set<String> labels = Sets.newHashSet("Person");

        NodeFrame nodeFrame = new NodeFrame(personDf, "id", JavaConverters.asScalaSet(labels).toSet(), new HashMap<>());

        Seq<NodeFrame> nodeFrames = JavaConverters.asScalaBuffer(Lists.newArrayList(nodeFrame)).toSeq();
        Seq<RelationshipFrame> relationshipFrames = JavaConverters.asScalaBuffer(new ArrayList<RelationshipFrame>()).toSeq();

        PropertyGraph graph = cypherSession.createGraph(nodeFrames, relationshipFrames);
        List<Row> result = graph.nodes().collectAsList();
        Assert.assertEquals(1, result.size());
    }
}
