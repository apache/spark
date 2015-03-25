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

package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.DecisionTreeClassificationModel;
import org.apache.spark.mllib.classification.RandomForestClassificationModel;
import org.apache.spark.mllib.classification.RandomForestClassifier;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.DecisionTreeClassifier;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.util.HashMap;

public final class JavaNewDT {

  public static void main(String[] args) {
    String datapath = "data/mllib/sample_libsvm_data.txt";
    if (args.length == 1) {
      datapath = args[0];
    } else if (args.length > 1) {
      System.err.println("Usage: JavaDecisionTree <libsvm format data file>");
      System.exit(1);
    }
    SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

    /****************** EXAMPLE OF Decision Tree **************************/
    DecisionTreeClassifier dt = new DecisionTreeClassifier()
        .setImpurity("Gini")
        .setMaxBins(5);
    DecisionTreeClassificationModel dtModel1 = dt.run(data);
    java.util.Map<Integer, Integer> catMap = new HashMap<Integer, Integer>();
    DecisionTreeClassificationModel dtModel2 = dt.run(data, catMap);
    DecisionTreeClassificationModel dtModel3 = dt.run(data, catMap, 2);

    /****************** EXAMPLE OF Ensemble **************************/
    RandomForestClassifier rf = new RandomForestClassifier()
        .setImpurity("gini")
        .setCheckpointInterval(5)
        .setFeaturesPerNode("auto")
        .setImpurity("Gini")
        .setCheckpointInterval(5)
        .setFeaturesPerNode("onethird");
    RandomForestClassificationModel rfModel = rf.run(data);

    sc.stop();
  }
}
