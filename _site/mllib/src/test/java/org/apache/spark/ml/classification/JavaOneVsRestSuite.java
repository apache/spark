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

package org.apache.spark.ml.classification;

import java.io.Serializable;
import java.util.List;

import static scala.collection.JavaConversions.seqAsJavaList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static org.apache.spark.mllib.classification.LogisticRegressionSuite.generateMultinomialLogisticInput;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class JavaOneVsRestSuite implements Serializable {

    private transient JavaSparkContext jsc;
    private transient SQLContext jsql;
    private transient DataFrame dataset;
    private transient JavaRDD<LabeledPoint> datasetRDD;

    @Before
    public void setUp() {
        jsc = new JavaSparkContext("local", "JavaLOneVsRestSuite");
        jsql = new SQLContext(jsc);
        int nPoints = 3;

        // The following weights and xMean/xVariance are computed from iris dataset with lambda=0.2.
        // As a result, we are drawing samples from probability distribution of an actual model.
        double[] weights = {
                -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
                -0.16624, -0.84355, -0.048509, -0.301789, 4.170682 };

        double[] xMean = {5.843, 3.057, 3.758, 1.199};
        double[] xVariance = {0.6856, 0.1899, 3.116, 0.581};
        List<LabeledPoint> points = seqAsJavaList(generateMultinomialLogisticInput(
                weights, xMean, xVariance, true, nPoints, 42));
        datasetRDD = jsc.parallelize(points, 2);
        dataset = jsql.createDataFrame(datasetRDD, LabeledPoint.class);
    }

    @After
    public void tearDown() {
        jsc.stop();
        jsc = null;
    }

    @Test
    public void oneVsRestDefaultParams() {
        OneVsRest ova = new OneVsRest();
        ova.setClassifier(new LogisticRegression());
        Assert.assertEquals(ova.getLabelCol() , "label");
        Assert.assertEquals(ova.getPredictionCol() , "prediction");
        OneVsRestModel ovaModel = ova.fit(dataset);
        DataFrame predictions = ovaModel.transform(dataset).select("label", "prediction");
        predictions.collectAsList();
        Assert.assertEquals(ovaModel.getLabelCol(), "label");
        Assert.assertEquals(ovaModel.getPredictionCol() , "prediction");
    }
}
