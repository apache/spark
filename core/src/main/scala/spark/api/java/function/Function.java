package spark.api.java.function;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;


/**
 * Base class for functions whose return types do not create special RDDs. PairFunction and
 * DoubleFunction are handled separately, to allow PairRDDs and DoubleRDDs to be constructed
 * when mapping RDDs of other types.
 */
public abstract class Function<T, R> extends WrappedFunction1<T, R> implements Serializable {
  public abstract R call(T t) throws Exception;

  public ClassTag<R> returnType() {
    return (ClassTag<R>) ClassTag$.MODULE$.apply(Object.class);
  }
}

