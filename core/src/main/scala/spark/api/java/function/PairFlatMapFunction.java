package spark.api.java.function;

import scala.Tuple2;
import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

// PairFlatMapFunction does not extend FlatMapFunction because flatMap is
// overloaded for both FlatMapFunction and PairFlatMapFunction.
public abstract class PairFlatMapFunction<T, K, V> extends AbstractFunction1<T, Iterable<Tuple2<K,
  V>>> implements Serializable {

  public ClassManifest<K> keyType() {
    return (ClassManifest<K>) ClassManifest$.MODULE$.fromClass(Object.class);
  }

  public ClassManifest<V> valueType() {
    return (ClassManifest<V>) ClassManifest$.MODULE$.fromClass(Object.class);
  }

  public abstract Iterable<Tuple2<K, V>> apply(T t);

}
