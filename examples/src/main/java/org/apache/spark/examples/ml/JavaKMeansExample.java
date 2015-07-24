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

import java.util.regex.Pattern;

import org.apache.commons.cli.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * An example demonstrating a k-means clustering.
 * Run with
 * {{{
 * bin/run-example ml.JavaSimpleParamsExample [options] -k <int> -input <file>
 * }}}
 */
public class JavaKMeansExample {

  private static class Params {
    String input;
    Integer k = 2;
    Integer maxIter = 20;
    Integer runs = 1;
    Double epsilon = 1E-4;
    Long seed = 1L;
    String initMode = KMeans.K_MEANS_PARALLEL();
    Integer initSteps = 5;
  }

  private static class ParsePoint implements Function<String, Row> {
    Pattern separater = Pattern.compile(" ");

    @Override
    public Row call(String line) {
      String[] tok = this.separater.split(line);
      double[] point = new double[tok.length];
      for (int i = 0; i < tok.length; ++i) {
        point[i] = Double.parseDouble(tok[i]);
      }
      Vector[] points = {Vectors.dense(point)};
      Row row = new GenericRow(points);
      return row;
    }
  }

  public static void main(String[] args) {
    // parse the arguments
    Params params = parse(args);
    SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    DataFrame dataset = loadData(jsc, params.input);

    org.apache.spark.ml.clustering.KMeans kmeans = new org.apache.spark.ml.clustering.KMeans()
      .setK(params.k)
      .setMaxIter(params.maxIter)
      .setRuns(params.runs)
      .setEpsilon(params.epsilon)
      .setSeed(params.seed)
      .setInitMode(params.initMode)
      .setInitSteps(params.initSteps);
    KMeansModel model = kmeans.fit(dataset);

    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center: centers) {
      System.out.println(center);
    }

    jsc.stop();
  }

  private static DataFrame loadData(JavaSparkContext jsc, String input) {
    SQLContext sqlContext = new SQLContext(jsc);

    JavaRDD<Row> points = jsc.textFile(input).map(new ParsePoint());
    StructField[] fields = new StructField[1];
    fields[0] = new StructField("features", new VectorUDT(), false, Metadata.empty());
    StructType schema = new StructType(fields);
    DataFrame dataset = sqlContext.createDataFrame(points, schema);
    return dataset;
  }

  private static Params parse(String[] args) {
    Options options = generateCommandlineOptions();
    CommandLineParser parser = new PosixParser();
    Params params = new Params();

    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("input")) {
        params.input = cmd.getOptionValue("input");
      }
      if (cmd.hasOption("k")) {
        String value = cmd.getOptionValue("k");
        params.k = Integer.parseInt(value);
      }
      if (cmd.hasOption("maxIter")) {
        String value = cmd.getOptionValue("maxIter");
        params.maxIter = Integer.parseInt(value);
      }
      if (cmd.hasOption("runs")) {
        String value = cmd.getOptionValue("runs");
        params.runs = Integer.parseInt(value);
      }
      if (cmd.hasOption("epsilon")) {
        String value = cmd.getOptionValue("epsilon");
        params.epsilon = Double.parseDouble(value);
      }
      if (cmd.hasOption("seed")) {
        String value = cmd.getOptionValue("seed");
        params.seed = Long.parseLong(value);
      }
      if (cmd.hasOption("initMode")) {
        params.initMode = cmd.getOptionValue("initMode");
      }
      if (cmd.hasOption("initSteps")) {
        String value = cmd.getOptionValue("initSteps");
        params.initSteps = Integer.parseInt(value);
      }
    } catch (ParseException e) {
      printHelpAndQuit(options);
    }
    return params;
  }

  @SuppressWarnings("static-access")
  private static Options generateCommandlineOptions() {
    Option input = OptionBuilder.withArgName("input")
      .hasArg()
      .isRequired()
      .withDescription("input path to labeled examples. This path must be specified")
      .create("input");
    Option k = OptionBuilder.withArgName("k")
      .hasArg()
      .isRequired()
      .withDescription("number of clusters created (>= 2). Default: ${defaults.k}")
      .create("k");
    Option maxIter = OptionBuilder.withArgName("maxIter")
      .hasArg()
      .withDescription("maximum number of iterations for Logistic Regression. default:100")
      .create("maxIter");
    Option runs = OptionBuilder.withArgName("runs")
      .hasArg()
      .withDescription("number of runs of the algorithm to execute in parallel, default: 1")
      .create("runs");
    Option epsilon = OptionBuilder.withArgName("epsilon")
      .hasArg()
      .withDescription("distance threshold within which we've consider centers to have converge" +
        ", default: 1E-4")
      .create("epsilon");
    Option seed = OptionBuilder.withArgName("seed")
      .hasArg()
      .withDescription("random seed, default: 1")
      .create("seed");
    Option initMode = OptionBuilder.withArgName("initMode")
      .hasArg()
      .withDescription("initialization algorithm (" + KMeans.RANDOM() + " or " +
        KMeans.K_MEANS_PARALLEL() + "default: " + KMeans.K_MEANS_PARALLEL())
      .create("initMode");
    Option initSteps = OptionBuilder.withArgName("initSteps")
      .hasArg()
      .withDescription("number of steps for k-means||, default: 5")
      .create("initSteps");

    Options options = new Options()
      .addOption(input)
      .addOption(k)
      .addOption(maxIter)
      .addOption(runs)
      .addOption(epsilon)
      .addOption(seed)
      .addOption(initMode)
      .addOption(initSteps);
    return options;
  }

  private static void printHelpAndQuit(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("JavaKMeansExample", options);
    System.exit(-1);
  }
}
