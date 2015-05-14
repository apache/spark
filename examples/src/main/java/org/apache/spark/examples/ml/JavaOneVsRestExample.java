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
 * The example uses Logistic Regression as the base classifier. All parameters that
 * can be specified on the base classifier can be passed in to the runner options.
 * Run with
 * <pre>
 * bin/run-example ml.JavaOneVsRestExample [options]
 * </pre>
 */
public class JavaOneVsRestExample {

  private static class Params {
    String input;
    String testInput = null;
    Integer maxIter = 100;
    double tol = 1E-6;
    boolean fitIntercept = true;
    Double regParam = null;
    Double elasticNetParam = null;
    double fracTest = 0.2;
  }

  public static void main(String[] args) {
    // parse the arguments
    Params params = parse(args);
    SparkConf conf = new SparkConf().setAppName("JavaOneVsRestExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // configure the base classifier
    LogisticRegression classifier = new LogisticRegression()
      .setMaxIter(params.maxIter)
      .setTol(params.tol)
      .setFitIntercept(params.fitIntercept);

    if (params.regParam != null) {
      classifier.setRegParam(params.regParam);
    }
    if (params.elasticNetParam != null) {
      classifier.setElasticNetParam(params.elasticNetParam);
    }

    // instantiate the One Vs Rest Classifier
    OneVsRest ova = new OneVsRest();
    ova.setClassifier(classifier);

    String input = params.input;
    RDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), input);
    RDD<LabeledPoint> train;
    RDD<LabeledPoint> test;
    String testInput = params.testInput;

    // compute the train/test split: if testInput is not provided use part of input.
    if (testInput != null) {
      train = inputData;
      test = MLUtils.loadLibSVMFile(jsc.sc(), testInput);
    } else {
      double f = params.fracTest;
      RDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{1 - f, f}, 12345);
      train = tmp[0];
      test = tmp[1];
    }

    // train the multiclass model.
    DataFrame trainingDataframe = jsql.createDataFrame(train, LabeledPoint.class);
    OneVsRestModel ovaModel = ova.fit(trainingDataframe.cache());

    // score the model on test data.
    DataFrame testDataframe = jsql.createDataFrame(test, LabeledPoint.class);
    DataFrame predictions = ovaModel.transform(testDataframe.cache())
      .select("prediction", "label");

    MulticlassMetrics metrics = new MulticlassMetrics(predictions);

    // output the confusion matrix.
    System.out.println("ConfusionMatrix");
    System.out.println(metrics.confusionMatrix().toString());

    StructField predictionColSchema = predictions.schema().apply("prediction");
    Integer numClasses = (Integer) MetadataUtils.getNumClasses(predictionColSchema).get();

    // compute the false positive rate per label
    StringBuilder results = new StringBuilder();
    results.append("label\tfpr\n");
    for (int label = 0; label < numClasses; label++) {
      results.append(label);
      results.append("\t");
      results.append(metrics.falsePositiveRate((double) label));
      results.append("\n");
    }
    System.out.println(results);

    jsc.stop();
  }

  private static Params parse(String[] args) {
    String input = args[0];
    String[] remainingArgs;
    if (args.length > 1) {
        remainingArgs = new String[args.length - 1];
    } else {
      remainingArgs = new String[0];
    }
    System.arraycopy(args, 1, remainingArgs, 0, remainingArgs.length);

    Option testInput = OptionBuilder.withArgName( "testInput" )
            .hasArg()
            .withDescription("input path to labeled examples")
      .create("testInput");
    Option fracTest = OptionBuilder.withArgName( "testInput" )
            .hasArg()
            .withDescription("fraction of data to hold out for testing." +
                    " If given option testInput, this option is ignored. default: 0.2")
      .create("fracTest");
    Option maxIter = OptionBuilder.withArgName( "maxIter" )
            .hasArg()
            .withDescription("maximum number of iterations for Logistic Regression. default:100")
      .create("maxIter");
    Option tol = OptionBuilder.withArgName( "tol" )
            .hasArg()
            .withDescription("the convergence tolerance of iterations " +
                    "for Logistic Regression. default: 1E-6")
      .create("tol");
    Option fitIntercept = OptionBuilder.withArgName( "fitIntercept" )
            .hasArg()
            .withDescription("fit intercept for logistic regression. default true")
      .create("fitIntercept");
    Option regParam = OptionBuilder.withArgName( "regParam" )
            .hasArg()
            .withDescription("the regularization parameter for Logistic Regression.")
      .create("regParam");
    Option elasticNetParam = OptionBuilder.withArgName("elasticNetParam" )
            .hasArg()
            .withDescription("the ElasticNet mixing parameter for Logistic Regression.")
      .create("elasticNetParam");

    Options options = new Options()
      .addOption(testInput)
      .addOption(fracTest)
      .addOption(maxIter)
      .addOption(tol)
      .addOption(fitIntercept)
      .addOption(regParam)
      .addOption(elasticNetParam);

    CommandLineParser parser = new PosixParser();

    Params params = new Params();
    params.input = input;

    try {
      CommandLine cmd = parser.parse(options, remainingArgs);
      String value;
      if (cmd.hasOption("maxIter")) {
        value = cmd.getOptionValue("maxIter");
        params.maxIter = Integer.parseInt(value);
      }
      if (cmd.hasOption("tol")) {
        value = cmd.getOptionValue("tol");
        params.tol = Double.parseDouble(value);
      }
      if (cmd.hasOption("fitIntercept")) {
        value = cmd.getOptionValue("fitIntercept");
        params.fitIntercept = Boolean.parseBoolean(value);
      }
      if (cmd.hasOption("regParam")) {
        value = cmd.getOptionValue("regParam");
        params.regParam = Double.parseDouble(value);
      }
      if (cmd.hasOption("elasticNetParam")) {
        value = cmd.getOptionValue("elasticNetParam");
        params.elasticNetParam = Double.parseDouble(value);
      }
      if (cmd.hasOption("testInput")) {
        value = cmd.getOptionValue("testInput");
        params.testInput = value;
      }
      if (cmd.hasOption("fracTest")) {
        value = cmd.getOptionValue("fracTest");
        params.fracTest = Double.parseDouble(value);
      }

    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "JavaOneVsRestExample", options );
      System.exit(-1);
    }
    return params;
  }
}
