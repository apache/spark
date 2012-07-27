package spark.api.java.function;

import scala.Tuple2;
import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

// PairFunction does not extend Function because some UDF functions, like map,
// are overloaded for both Function and PairFunction.
public abstract class PairFunction<T, K, V> extends AbstractFunction1<T, Tuple2<K,
  V>> implements Serializable {

  public ClassManifest<K> keyType() {
    return (ClassManifest<K>) ClassManifest$.MODULE$.fromClass(Object.class);
  }

  public ClassManifest<V> valueType() {
    return (ClassManifest<V>) ClassManifest$.MODULE$.fromClass(Object.class);
  }

  public abstract Tuple2<K, V> apply(T t);

}
