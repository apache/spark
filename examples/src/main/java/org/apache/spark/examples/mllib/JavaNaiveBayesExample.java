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

// $example on$
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
// $example off$
import org.apache.spark.SparkConf;

public class JavaNaiveBayesExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaNaiveBayesExample");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    // $example on$
    String path = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> inputData = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
    JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
    JavaRDD<LabeledPoint> training = tmp[0]; // training set
    JavaRDD<LabeledPoint> test = tmp[1]; // test set
    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
    JavaPairRDD<Double, Double> predictionAndLabel =
      test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
        @Override
        public Tuple2<Double, Double> call(LabeledPoint p) {
          return new Tuple2<>(model.predict(p.features()), p.label());
        }
      });
    double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        return pl._1().equals(pl._2());
      }
    }).count() / (double) test.count();

    // Save and load model
    model.save(jsc.sc(), "target/tmp/myNaiveBayesModel");
    NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "target/tmp/myNaiveBayesModel");
    // $example off$

    jsc.stop();
  }
}
