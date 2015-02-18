package org.apache.spark.examples.mllib;

import scala.Tuple2;
import scala.Tuple3;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;

/**
 * Java example for graph clustering using power iteration clustering (PIC).
 */
public class JavaPowerIterationClusteringExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaPowerIterationClusteringExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    @SuppressWarnings("unchecked")
    JavaRDD<Tuple3<Long, Long, Double>> similarities = sc.parallelize(Lists.newArrayList(
      new Tuple3<Long, Long, Double>(0L, 1L, 0.9),
      new Tuple3<Long, Long, Double>(1L, 2L, 0.9),
      new Tuple3<Long, Long, Double>(2L, 3L, 0.9),
      new Tuple3<Long, Long, Double>(3L, 4L, 0.1),
      new Tuple3<Long, Long, Double>(4L, 5L, 0.9)));

    PowerIterationClustering pic = new PowerIterationClustering()
      .setK(2)
      .setMaxIterations(10);
    PowerIterationClusteringModel model = pic.run(similarities);

    for (Tuple2<Object, Object> assignment: model.assignments().toJavaRDD().collect()) {
      System.out.println(assignment._1() + " -> " + assignment._2());
    }

    sc.stop();
  }
}
