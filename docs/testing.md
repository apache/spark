---
layout: global
title: Testing with Spark
description: How to write unit tests for Spark and for Spark-based applications
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

***************************************************************************************************

# Primary testing classes

1. `org.apache.spark.SharedSparkContext`
   Includes a `SparkContext` for use by the test suite
1. `org.apache.spark.SparkFunSuite`
   Standardizes reporting of test and test suite names
1. `org.apache.spark.sql.test.SharedSQLContext`
   Includes a `SQLContext` for use by the test suite
   Incorporates `SparkFunSuite`
1. `org.apache.spark.sql.test.SharedSparkSession`
   Includes a `SparkSession` for use by the test suite

***************************************************************************************************

# Unit testing Spark

All internal tests of Spark code should derive, directly or indirectly, from `SparkFunSuite`, so as to standardize the reporting of suite and test name logging.

Tests that require a `SparkContext` should derive from `SharedSparkContext` also.

`SharedSQLContext` already derives from `SparkFunSuite`, so may be extended directly by tests requiring a `SQLContext`.

`SharedSparkSession` does not derive from `SparkFunSuite`, so should not be extended directly for any internal Spark tests.

***************************************************************************************************

# Unit testing code that uses Spark using ScalaTest

External applications that use Spark may extend `SharedSparkContext` or `SharedSparkSession`.  These classes support various testing styles:

## FunSuite style
(% highlight scala %}
class MySparkTest extends FunSuite with SharedSparkContext {
  test("A parallelized RDD should be able to count its elements") {
    assert(4 === sc.parallelize(Seq(1, 2, 3, 4)).count)
  }
}
{% endhighlight %}

## FunSpec style
(% highlight scala %}
class MySparkTest extends FunSpec with SharedSparkContext {
  describe("A parallelized RDD") {
    it("should be able to count its elements") {
      assert(4 === sc.parallelize(Seq(1, 2, 3, 4)).count)
    }
  }
}
{% endhighlight %}

## FlatSpec style
(% highlight scala %}
class MySparkTest extends FlatSpec with SharedSparkContext {
  "A parallelized RDD" should "be able to count its elements" in {
    assert(4 === sc.parallelize(Seq(1, 2, 3, 4)).count)
  }
}
{% endhighlight %}

## WordSpec style
(% highlight scala %}
class MySparkTest extends WordSpec with SharedSparkContext {
  "A parallelized RDD" when {
    "created" should {
      "be able to count its elements" in {
        assert(4 === sc.parallelize(Seq(1, 2, 3, 4)).count)
      }
    }
  }
}
{% endhighlight %}

# Context and Session initialization

It should be noted that, in `SharedSparkContext`, the `SparkContext` (`sc`) isn't initialized until `beforeAll` is called.  When using several testing styles, such as `FunSpec`, it is not uncommon to initialize shared resources inside a describe block (or equivalent), but outside an `it` block - i.e., in the registration phase.  `beforeAll`, however, isn't called until after the registration phase, and just before the test phase.  Therefore, an `initializeContext` call is exposed so that users can make sure the context is initialized in these blocks.  For example:

{% highlight scala %}
class MySparkTest extend FunSpec with SharedSparkContext {
  describe("A parallelized RDD") {
    initializeContext()
	val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    it("should be able to count its elements") {
      assert(4 === rdd.count)
    }
  }
}
{% endhighlight scala %}

Similarly, in `SharedSparkSession`, there is an `initializeSession` call for the same purpose:

{% highlight scala %}
class MySparkTest extend FunSpec with SharedSparkSession {
  describe("A simple Dataset") {
    initializeContext()
	import testImplicits._

    val dataset = Seq(1, 2, 3, 4).toDS
    it("should be able to count its elements") {
      assert(4 === dataset.count)
    }
  }
}
{% endhighlight scala %}
