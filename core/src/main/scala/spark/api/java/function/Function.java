package spark.api.java.function;

import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;


/**
 * Base class for functions whose return types do not have special RDDs; DoubleFunction is
 * handled separately, to allow DoubleRDDs to be constructed when mapping RDDs to doubles.
 */
public abstract class Function<T, R> extends AbstractFunction1<T, R> implements Serializable {
  public abstract R apply(T t);

  public ClassManifest<R> returnType() {
    return (ClassManifest<R>) ClassManifest$.MODULE$.fromClass(Object.class);
  }
}

