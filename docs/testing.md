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

1. org.apache.spark.SharedSparkContext
   Includes a SparkContext for use by the test suite
1. org.apache.spark.SparkFunSuite
   Standardizes reporting of test and test suite names
1. org.apache.spark.sql.test.SharedSQLContext
   Includes a SQLContext for use by the test suite
   Incorporates SparkFunSuite
1. org.apache.spark.sql.test.SharedSparkSession
   Includes a SparkSession for use by the test suite

***************************************************************************************************

# Testing Spark

All internal tests of Spark code should derive, directly or indirectly, from SparkFunSuite, so as to standardize the reporting of suite and test name logging.

Tests that require a SparkContext should derive from SharedSparkContext also.

SharedSQLContext already derives from SparkFunSuite, so may be extended directly by tests requiring a SQLContext.

SharedSparkSession does not derive from SparkFunSuite, so should not be extended directly for any internal spark tests.

***************************************************************************************************

# Testing code that uses Spark

External applications that use Spark may extend SharedSparkContext or SharedSparkSession.  It should be noted that, in SharedSparkSession, the SparkSession isn't initialized by default until beforeAll - which has not been called before the code that is inside outer test grouping blocks (like 'describe'), but outside actual test cases.  To use a SparkSession in these areas of code, one must call initializeSession first.