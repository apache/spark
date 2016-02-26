package org.apache.spark.examples.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.MaxAbsScaler;
import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class JavaMaxAbsScalerExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaMaxAbsScalerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    DataFrame dataFrame = jsql.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
    MaxAbsScaler scaler = new MaxAbsScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures");

    // Compute summary statistics and generate MaxAbsScalerModel
    MaxAbsScalerModel scalerModel = scaler.fit(dataFrame);

    // rescale each feature to range [min, max].
    DataFrame scaledData = scalerModel.transform(dataFrame);
    scaledData.show();
    // $example off$
    jsc.stop();
  }

}
