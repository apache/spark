package spark.examples;

import scala.util.Random;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaLR {

  static int N = 10000;  // Number of data points
  static int D = 10;   // Number of dimensions
  static double R = 0.7;  // Scaling factor
  static int ITERATIONS = 5;
  static Random rand = new Random(42);

  static class DataPoint implements Serializable {
    public DataPoint(double[] x, int y) {
      this.x = x;
      this.y = y;
    }
    double[] x;
    int y;
  }

  static DataPoint generatePoint(int i) {
    int y = (i % 2 == 0) ? -1 : 1;
    double[] x = new double[D];
    for (int j = 0; j < D; j++) {
      x[j] = rand.nextGaussian() + y * R;
    }
    return new DataPoint(x, y);
  }

  static List<DataPoint> generateData() {
    List<DataPoint> points = new ArrayList<DataPoint>(N);
    for (int i = 0; i < N; i++) {
      points.add(generatePoint(i));
    }
    return points;
  }

  static class VectorSum extends Function2<double[], double[], double[]> {

    @Override
    public double[] apply(double[] a, double[] b) {
      double[] result = new double[D];
      for (int j = 0; j < D; j++) {
        result[j] = a[j] + b[j];
      }
      return result;
    }
  }

  static class ComputeGradient extends Function<DataPoint, double[]> {

    double[] weights;

    public ComputeGradient(double[] weights) {
      this.weights = weights;
    }

    @Override
    public double[] apply(DataPoint p) {
      double[] gradient = new double[D];
      for (int i = 0; i < D; i++) {
        double dot = dot(weights, p.x);
        gradient[i] = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y * p.x[i];
      }
      return gradient;
    }
  }

  public static double dot(double[] a, double[] b) {
    double x = 0;
    for (int i = 0; i < D; i++) {
      x += a[i] * b[i];
    }
    return x;
  }

  public static void printWeights(double[] a) {
    System.out.println(Arrays.toString(a));
  }

  public static void main(String[] args) {

      if (args.length == 0) {
        System.err.println("Usage: JavaLR <host> [<slices>]");
        System.exit(1);
      }

      JavaSparkContext sc = new JavaSparkContext(args[0], "JavaLR");
      Integer numSlices =  (args.length > 1) ? Integer.parseInt(args[1]): 2;
      List<DataPoint> data = generateData();

      // Initialize w to a random value
      double[] w = new double[D];
      for (int i = 0; i < D; i++) {
        w[i] = 2 * rand.nextDouble() - 1;
      }

      System.out.print("Initial w: ");
      printWeights(w);

      for (int i = 1; i <= ITERATIONS; i++) {
        System.out.println("On iteration " + i);

        double[] gradient = sc.parallelize(data, numSlices).map(
          new ComputeGradient(w)
        ).reduce(new VectorSum());

        for (int j = 0; j < D; j++) {
          w[j] -= gradient[j];
        }

      }

      System.out.print("Final w: ");
      printWeights(w);
      System.exit(0);
    }
}
