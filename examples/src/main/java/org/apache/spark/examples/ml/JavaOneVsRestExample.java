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

import org.apache.commons.cli.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.util.MetadataUtils;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;

/**
 * An example runner for Multiclass to Binary Reduction with One Vs Rest.
 * The example uses Logistic Regression as the base classifier. All parameters in
 * the base classifier are set as default values.
 * Run with
 * <pre>
 * bin/run-example ml.JavaOneVsRestExample [options]
 * </pre>
 */
public class JavaOneVsRestExample {

    static class Params {
        LogisticRegression classifier;
        String input;
        String testInput;
        double fracTest;

        public Params(LogisticRegression classifier) {
            this.classifier = classifier;
        }
    }

    public static void main(String[] args) {
        Params params = parse(args);
        SparkConf conf = new SparkConf().setAppName("JavaOneVsRestExample");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext jsql = new SQLContext(jsc);
        OneVsRest ova = new OneVsRest();
        ova.setClassifier(params.classifier);

        String input = params.input;
        RDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), input);
        RDD<LabeledPoint> train = null;
        RDD<LabeledPoint> test = null;
        String testInput = params.testInput;

        // compute the train/test split: if testInput is not provided use part of input.
        double f = params.fracTest;
        if (testInput != null) {
            train = inputData;
            test = MLUtils.loadLibSVMFile(jsc.sc(), testInput);
        } else {
            RDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{1 - f, f}, 12345);
            train = tmp[0];
            test = tmp[1];
        }

        // train the multiclass model.
        DataFrame trainingDataframe = jsql.createDataFrame(train, LabeledPoint.class);
        OneVsRestModel ovaModel = ova.fit(trainingDataframe.cache());

        // score the model on test data.
        DataFrame testDataframe = jsql.createDataFrame(test, LabeledPoint.class);
        DataFrame predictions = ovaModel.transform(testDataframe)
                .select("prediction", "label");

        MulticlassMetrics metrics = new MulticlassMetrics(predictions);

        // output the confusion matrix.
        System.out.println(metrics.confusionMatrix().toString());

        StructField predictionColSchema = predictions.schema().apply("prediction");
        Integer numClasses = (Integer)MetadataUtils.getNumClasses(predictionColSchema).get();

        // compute the false positive rate per label
        StringBuilder results = new StringBuilder();
        results.append("label\tfpr\n");
        for (int label = 0; label < numClasses ; label++) {
            results.append(label);
            results.append("\t");
            results.append(metrics.falsePositiveRate(label));
            results.append("\n");
        }
    }

    private static Params parse(String[] args) {
        String input = args[0];
        String[] remainingArgs = null;
        if (args.length > 1) {
            remainingArgs = new String[args.length - 1];
        } else {
            remainingArgs = new String[0];
        }
        System.arraycopy(args, 1, remainingArgs, 0, remainingArgs.length);
        Options options = new Options();
        options.addOption("testInput", false, "input path to labeled examples");
        options.addOption("fracTest", false, "fraction of data to hold out for testing. " +
                "If given option testInput, this option is ignored. default: 0.2");
        options.addOption("maxIter", false, "maximum number of iterations. default:100");
        options.addOption("tol", false, "the convergence tolerance of iterations. default: 1E-6");
        options.addOption("regParam", false, "the regularization parameter");
        options.addOption("elasticNetParam", false, "the ElasticNet mixing parameter");
        CommandLineParser parser = new BasicParser();

        LogisticRegression classifier = new LogisticRegression();
        String testInput = null;
        double fracTest = 0.0;
        try {
            CommandLine cmd = parser.parse( options, remainingArgs);
            String value = null;
            if ((value = cmd.getOptionValue("maxIter")) != null) {
                classifier.setMaxIter(Integer.parseInt(value));
            }
            if ((value = cmd.getOptionValue("tol")) != null) {
                classifier.setTol(Double.parseDouble(value));
            }
            if ((value = cmd.getOptionValue("regParam")) != null) {
                classifier.setRegParam(Double.parseDouble(value));
            }
            if ((value = cmd.getOptionValue("elasticNetParam")) != null) {
                classifier.setElasticNetParam(Double.parseDouble(value));
            }
            if ((value = cmd.getOptionValue("testInput")) != null) {
                testInput = value;
            }
            if ((value = cmd.getOptionValue("fracTest")) != null) {
                fracTest = Double.parseDouble(value);
            }

        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "JavaOneVsRestExample", options );
            System.exit(-1);
        }
        Params params = new Params(classifier);
        params.input = input;
        params.testInput = testInput;
        params.fracTest = fracTest;
        return params;
    }
}
