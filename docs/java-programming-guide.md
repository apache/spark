---
layout: global
title: Java Programming Guide
---

The Spark Java API
([spark.api.java]({{HOME_PATH}}api/core/index.html#spark.api.java.package)) defines
[`JavaSparkContext`]({{HOME_PATH}}api/core/index.html#spark.api.java.JavaSparkContext) and
[`JavaRDD`]({{HOME_PATH}}api/core/index.html#spark.api.java.JavaRDD) clases,
which support
the same methods as their Scala counterparts but take Java functions and return
Java data and collection types.

Because Java API is similar to the Scala API, this programming guide only
covers Java-specific features;
the [Scala Programming Guide]({{HOME_PATH}}scala-programming-guide.html)
provides a more general introduction to Spark concepts and should be read
first.


# Key differences in the Java API
There are a few key differences between the Java and Scala APIs:

* Java does not support anonymous or first-class functions, so functions must
  be implemented by extending the
  [`spark.api.java.function.Function`]({{HOME_PATH}}api/core/index.html#spark.api.java.function.Function),
  [`Function2`]({{HOME_PATH}}api/core/index.html#spark.api.java.function.Function2), etc.
  classes.
* To maintain type safety, the Java API defines specialized Function and RDD
  classes for key-value pairs and doubles.
* RDD methods like `collect` and `countByKey` return Java collections types,
  such as `java.util.List` and `java.util.Map`.


## RDD Classes
Spark defines additional operations on RDDs of doubles and key-value pairs, such
as `stdev` and `join`.

In the Scala API, these methods are automatically added using Scala's
[implicit conversions](http://www.scala-lang.org/node/130) mechanism.

In the Java API, the extra methods are defined in
[`JavaDoubleRDD`]({{HOME_PATH}}api/core/index.html#spark.api.java.JavaDoubleRDD) and
[`JavaPairRDD`]({{HOME_PATH}}api/core/index.html#spark.api.java.JavaPairRDD)
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

<tr><td>Function&lt;T, R&gt;</td><td>T -&gt; R </td></tr>
<tr><td>DoubleFunction&lt;T&gt;</td><td>T -&gt; Double </td></tr>
<tr><td>PairFunction&lt;T, K, V&gt;</td><td>T -&gt; Tuple2&lt;K, V&gt; </td></tr>

<tr><td>FlatMapFunction&lt;T, R&gt;</td><td>T -&gt; Iterable&lt;R&gt; </td></tr>
<tr><td>DoubleFlatMapFunction&lt;T&gt;</td><td>T -&gt; Iterable&lt;Double&gt; </td></tr>
<tr><td>PairFlatMapFunction&lt;T, K, V&gt;</td><td>T -&gt; Iterable&lt;Tuple2&lt;K, V&gt;&gt; </td></tr>

<tr><td>Function2&lt;T1, T2, R&gt;</td><td>T1, T2 -&gt; R (function of two arguments)</td></tr>
</table>

# Other Features
The Java API supports other Spark features, including
[accumulators]({{HOME_PATH}}scala-programming-guide.html#accumulators),
[broadcast variables]({{HOME_PATH}}scala-programming-guide.html#broadcast_variables), and
[caching]({{HOME_PATH}}scala-programming-guide.html#caching).

# Example

As an example, we will implement word count using the Java API.

{% highlight java %}
import spark.api.java.*;
import spark.api.java.function.*;

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
a matter of style.


# Where to go from here
Spark includes several sample jobs using the Java API in
`examples/src/main/java`.  You can run them by passing the class name to the
`run` script included in Spark -- for example, `./run
spark.examples.JavaWordCount`.  Each example program prints usage help when run
without any arguments.
