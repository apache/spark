package spark.api.java;

import java.util.Arrays;
import java.util.List;

// See
// http://scala-programming-language.1934581.n4.nabble.com/Workaround-for-implementing-java-varargs-in-2-7-2-final-tp1944767p1944772.html
abstract class JavaSparkContextVarargsWorkaround {
  public <T> JavaRDD<T> union(JavaRDD<T> ... rdds) {
    return union(Arrays.asList(rdds));
  }

  public JavaDoubleRDD union(JavaDoubleRDD ... rdds) {
    return union(Arrays.asList(rdds));
  }

  public <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V> ... rdds) {
    return union(Arrays.asList(rdds));
  }

  abstract public <T> JavaRDD<T> union(List<JavaRDD<T>> rdds);
  abstract public JavaDoubleRDD union(List<JavaDoubleRDD> rdds);
  abstract public <K, V> JavaPairRDD<K, V> union(List<JavaPairRDD<K, V>> rdds);

}
