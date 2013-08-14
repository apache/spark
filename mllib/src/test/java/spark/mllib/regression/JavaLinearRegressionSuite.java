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

package spark.mllib.regression;

import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;

public class JavaLinearRegressionSuite implements Serializable {
    private transient JavaSparkContext sc;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "JavaLinearRegressionSuite");
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
        System.clearProperty("spark.driver.port");
    }

    int validatePrediction(List<LabeledPoint> validationData, LinearRegressionModel model) {
        int numAccurate = 0;
        for (LabeledPoint point: validationData) {
            Double prediction = model.predict(point.features());
            // A prediction is off if the prediction is more than 0.5 away from expected value.
            if (Math.abs(prediction - point.label()) <= 0.5) {
                numAccurate++;
            }
        }
        return numAccurate;
    }

    @Test
    public void runLinearRegressionUsingConstructor() {
        int nPoints = 10000;
        double A = 2.0;
        double[] weights = {-1.5, 1.0e-2};

        JavaRDD<LabeledPoint> testRDD = sc.parallelize(LinearRegressionSuite.generateLinearRegressionInputAsList(A,
                weights, nPoints, 42), 2).cache();
        List<LabeledPoint> validationData =
                LinearRegressionSuite.generateLinearRegressionInputAsList(A, weights, nPoints, 17);

        LinearRegressionWithSGD svmSGDImpl = new LinearRegressionWithSGD();
        svmSGDImpl.optimizer().setStepSize(1.0)
                .setRegParam(0.01)
                .setNumIterations(20);
        LinearRegressionModel model = svmSGDImpl.run(testRDD.rdd());

        int numAccurate = validatePrediction(validationData, model);
        Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
    }

    @Test
    public void runLinearRegressionUsingStaticMethods() {
        int nPoints = 10000;
        double A = 2.0;
        double[] weights = {-1.5, 1.0e-2};

        JavaRDD<LabeledPoint> testRDD = sc.parallelize(LinearRegressionSuite.generateLinearRegressionInputAsList(A,
                weights, nPoints, 42), 2).cache();
        List<LabeledPoint> validationData =
                LinearRegressionSuite.generateLinearRegressionInputAsList(A, weights, nPoints, 17);

        LinearRegressionModel model = LinearRegressionWithSGD.train(testRDD.rdd(), 100, 1.0, 1.0);

        int numAccurate = validatePrediction(validationData, model);
        Assert.assertTrue(numAccurate > nPoints * 4.0 / 5.0);
    }

}
