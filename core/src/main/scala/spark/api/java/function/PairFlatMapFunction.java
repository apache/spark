package spark.api.java.function;

import scala.Tuple2;
import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * A function that returns zero or more key-value pair records from each input record. The
 * key-value pairs are represented as scala.Tuple2 objects.
 */
// PairFlatMapFunction does not extend FlatMapFunction because flatMap is
// overloaded for both FlatMapFunction and PairFlatMapFunction.
public abstract class PairFlatMapFunction<T, K, V>
  extends WrappedFunction1<T, Iterable<Tuple2<K, V>>>
  implements Serializable {

  public abstract Iterable<Tuple2<K, V>> call(T t) throws Exception;

  public ClassManifest<K> keyType() {
    return (ClassManifest<K>) ClassManifest$.MODULE$.fromClass(Object.class);
  }

  public ClassManifest<V> valueType() {
    return (ClassManifest<V>) ClassManifest$.MODULE$.fromClass(Object.class);
  }
}
