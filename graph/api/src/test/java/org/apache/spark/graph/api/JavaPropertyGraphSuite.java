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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public abstract class JavaPropertyGraphSuite implements Serializable {
    private transient TestSparkSession spark;
    private transient CypherSession cypherSession;

    abstract CypherSession getCypherSession(SparkSession sparkSession);

    @Before
    public void setUp() {
        spark = new TestSparkSession();
        cypherSession = getCypherSession(spark);
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testCreateFromNodeFrame() {
        StructType personSchema = createSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(LongType, StringType));

        List<Row> personData = Arrays.asList(
                RowFactory.create(0L, "Alice"),
                RowFactory.create(1L, "Bob"));

        StructType knowsSchema = createSchema(
                Lists.newArrayList("id", "source", "target", "since"),
                Lists.newArrayList(LongType, LongType, LongType, IntegerType));

        List<Row> knowsData = Collections.singletonList(RowFactory.create(0L, 0L, 1L, 1984));

        Dataset<Row> personDf = spark.createDataFrame(personData, personSchema);
        NodeFrame personNodeFrame = cypherSession.buildNodeFrame(personDf)
            .idColumn("id")
            .labelSet(new String[]{"Person"})
            .properties(Collections.singletonMap("name", "name"))
            .build();

        Dataset<Row> knowsDf = spark.createDataFrame(knowsData, knowsSchema);
        RelationshipFrame knowsRelFrame = cypherSession.buildRelationshipFrame(knowsDf)
                .idColumn("id")
                .sourceIdColumn("source")
                .targetIdColumn("target")
                .relationshipType("KNOWS")
                .properties(Collections.singletonMap("since", "since"))
                .build();


        PropertyGraph graph = cypherSession.createGraph(
                new NodeFrame[]{personNodeFrame},
                new RelationshipFrame[]{knowsRelFrame});
        List<Row> result = graph.nodes().collectAsList();
        Assert.assertEquals(2, result.size());
    }

    private StructType createSchema(List<String> fieldNames, List<DataType> dataTypes) {
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            fields.add(createStructField(fieldNames.get(i), dataTypes.get(i), true));
        }
        return createStructType(fields);
    }
}
