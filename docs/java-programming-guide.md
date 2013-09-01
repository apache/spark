---
layout: global
title: Java Programming Guide
---

The Spark Java API exposes all the Spark features available in the Scala version to Java.
To learn the basics of Spark, we recommend reading through the
[Scala programming guide](scala-programming-guide.html) first; it should be
easy to follow even if you don't know Scala.
This guide will show how to use the Spark features described there in Java.

The Spark Java API is defined in the
[`org.apache.spark.api.java`](api/core/index.html#org.apache.spark.api.java.package) package, and includes
a [`JavaSparkContext`](api/core/index.html#org.apache.spark.api.java.JavaSparkContext) for
initializing Spark and [`JavaRDD`](api/core/index.html#org.apache.spark.api.java.JavaRDD) classes,
which support the same methods as their Scala counterparts but take Java functions and return
Java data and collection types. The main differences have to do with passing functions to RDD
operations (e.g. map) and handling RDDs of different types, as discussed next.

# Key Differences in the Java API

There are a few key differences between the Java and Scala APIs:

* Java does not support anonymous or first-class functions, so functions must
  be implemented by extending the
  [`org.apache.spark.api.java.function.Function`](api/core/index.html#org.apache.spark.api.java.function.Function),
  [`Function2`](api/core/index.html#org.apache.spark.api.java.function.Function2), etc.
  classes.
* To maintain type safety, the Java API defines specialized Function and RDD
  classes for key-value pairs and doubles. For example, 
  [`JavaPairRDD`](api/core/index.html#org.apache.spark.api.java.JavaPairRDD)
  stores key-value pairs.
* RDD methods like `collect()` and `countByKey()` return Java collections types,
  such as `java.util.List` and `java.util.Map`.
* Key-value pairs, which are simply written as `(key, value)` in Scala, are represented
  by the `scala.Tuple2` class, and need to be created using `new Tuple2<K, V>(key, value)`.

## RDD Classes

Spark defines additional operations on RDDs of key-value pairs and doubles, such
as `reduceByKey`, `join`, and `stdev`.

In the Scala API, these methods are automatically added using Scala's
[implicit conversions](http://www.scala-lang.org/node/130) mechanism.

In the Java API, the extra methods are defined in the
[`JavaPairRDD`](api/core/index.html#org.apache.spark.api.java.JavaPairRDD)
and [`JavaDoubleRDD`](api/core/index.html#org.apache.spark.api.java.JavaDoubleRDD)
classes.  RDD methods like `map` are overloaded by specialized `PairFunction`
and `DoubleFunction` classes, allowing them to return RDDs of the appropriate
types.  Common methods like `filter` and `sample` are implemented by
each specialized RDD class, so filtering a `PairRDD` returns a new `PairRDD`,
etc (this acheives the "same-result-type" principle used by the [Scala collections
framework](http://docs.scala-lang.org/overviews/core/architecture-of-scala-collections.html)).

## Function Classes

The following table lists the function classes used by the Java API.  Each
class has a single abstract method, `call()`, that must be implemented.

<table class="table">
<tr><th>Class</th><th>Function Type</th></tr>

<tr><td>Function&lt;T, R&gt;</td><td>T =&gt; R </td></tr>
<tr><td>DoubleFunction&lt;T&gt;</td><td>T =&gt; Double </td></tr>
<tr><td>PairFunction&lt;T, K, V&gt;</td><td>T =&gt; Tuple2&lt;K, V&gt; </td></tr>

<tr><td>FlatMapFunction&lt;T, R&gt;</td><td>T =&gt; Iterable&lt;R&gt; </td></tr>
<tr><td>DoubleFlatMapFunction&lt;T&gt;</td><td>T =&gt; Iterable&lt;Double&gt; </td></tr>
<tr><td>PairFlatMapFunction&lt;T, K, V&gt;</td><td>T =&gt; Iterable&lt;Tuple2&lt;K, V&gt;&gt; </td></tr>

<tr><td>Function2&lt;T1, T2, R&gt;</td><td>T1, T2 =&gt; R (function of two arguments)</td></tr>
</table>

## Storage Levels

RDD [storage level](scala-programming-guide.html#rdd-persistence) constants, such as `MEMORY_AND_DISK`, are
declared in the [org.apache.spark.api.java.StorageLevels](api/core/index.html#org.apache.spark.api.java.StorageLevels) class. To
define your own storage level, you can use StorageLevels.create(...). 


# Other Features

The Java API supports other Spark features, including
[accumulators](scala-programming-guide.html#accumulators),
[broadcast variables](scala-programming-guide.html#broadcast-variables), and
[caching](scala-programming-guide.html#rdd-persistence).


# Example

As an example, we will implement word count using the Java API.

{% highlight java %}
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

JavaSparkContext sc = new JavaSparkContext(...);
JavaRDD<String> lines = ctx.textFile("hdfs://...");
JavaRDD<String> words = lines.flatMap(
  new FlatMapFunction<String, String>() {
    public Iterable<String> call(String s) {
      return Arrays.asList(s.split(" "));
    }
  }
);
{% endhighlight %}

The word count program starts by creating a `JavaSparkContext`, which accepts
the same parameters as its Scala counterpart.  `JavaSparkContext` supports the
same data loading methods as the regular `SparkContext`; here, `textFile`
loads lines from text files stored in HDFS.

To split the lines into words, we use `flatMap` to split each line on
whitespace.  `flatMap` is passed a `FlatMapFunction` that accepts a string and
returns an `java.lang.Iterable` of strings.

Here, the `FlatMapFunction` was created inline; another option is to subclass
`FlatMapFunction` and pass an instance to `flatMap`:

{% highlight java %}
class Split extends FlatMapFunction<String, String> {
  public Iterable<String> call(String s) {
    return Arrays.asList(s.split(" "));
  }
);
JavaRDD<String> words = lines.flatMap(new Split());
{% endhighlight %}

Continuing with the word count example, we map each word to a `(word, 1)` pair:

{% highlight java %}
import scala.Tuple2;
JavaPairRDD<String, Integer> ones = words.map(
  new PairFunction<String, String, Integer>() {
    public Tuple2<String, Integer> call(String s) {
      return new Tuple2(s, 1);
    }
  }
);
{% endhighlight %}

Note that `map` was passed a `PairFunction<String, String, Integer>` and
returned a `JavaPairRDD<String, Integer>`.

To finish the word count program, we will use `reduceByKey` to count the
occurrences of each word:

{% highlight java %}
JavaPairRDD<String, Integer> counts = ones.reduceByKey(
  new Function2<Integer, Integer, Integer>() {
    public Integer call(Integer i1, Integer i2) {
      return i1 + i2;
    }
  }
);
{% endhighlight %}

Here, `reduceByKey` is passed a `Function2`, which implements a function with
two arguments.  The resulting `JavaPairRDD` contains `(word, count)` pairs.

In this example, we explicitly showed each intermediate RDD.  It is also
possible to chain the RDD transformations, so the word count example could also
be written as:

{% highlight java %}
JavaPairRDD<String, Integer> counts = lines.flatMap(
    ...
  ).map(
    ...
  ).reduceByKey(
    ...
  );
{% endhighlight %}

There is no performance difference between these approaches; the choice is
just a matter of style.

# Javadoc

We currently provide documentation for the Java API as Scaladoc, in the
[`org.apache.spark.api.java` package](api/core/index.html#org.apache.spark.api.java.package), because
some of the classes are implemented in Scala. The main downside is that the types and function
definitions show Scala syntax (for example, `def reduce(func: Function2[T, T]): T` instead of
`T reduce(Function2<T, T> func)`). 
We hope to generate documentation with Java-style syntax in the future.


# Where to Go from Here

Spark includes several sample programs using the Java API in
[`examples/src/main/java`](https://github.com/apache/incubator-spark/tree/master/examples/src/main/java/org/apache/spark/examples).  You can run them by passing the class name to the
`run-example` script included in Spark; for example:

    ./run-example org.apache.spark.examples.JavaWordCount

Each example program prints usage help when run
without any arguments.
