package spark.api.java.function;


import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * A function that returns zero or more records of type Double from each input record.
 */
// DoubleFlatMapFunction does not extend FlatMapFunction because flatMap is
// overloaded for both FlatMapFunction and DoubleFlatMapFunction.
public abstract class DoubleFlatMapFunction<T> extends AbstractFunction1<T, Iterable<Double>>
  implements Serializable {

  public abstract Iterable<Double> call(T t);

  @Override
  public final Iterable<Double> apply(T t) { return call(t); }
}
