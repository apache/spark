---
layout: global
title: Tutorial - Running a Simple Spark Application
---

1. Create directory for spark demo:

        ~$ mkdir SparkTest

2. Copy the sbt files in ~/spark/sbt directory:

        ~/SparkTest$ cp -r ../spark/sbt .

3. Edit the ~/SparkTest/sbt/sbt file to look like this:

        #!/bin/bash
        java -Xmx800M -XX:MaxPermSize=150m -jar $(dirname $0)/sbt-launch-*.jar "$@"

4. To build a Spark application, you need Spark and its dependencies in a single Java archive (JAR) file. Create this JAR in Spark's main directory with sbt as:

        ~/spark$ sbt/sbt assembly

5. create a source file in ~/SparkTest/src/main/scala directory:

        ~/SparkTest/src/main/scala$ vi Test1.scala

6. Make the contain of the Test1.scala file like this:

        import spark.SparkContext
        import spark.SparkContext._
        object Test1 {
          def main(args: Array[String]) {
            val sc = new SparkContext("local", "SparkTest")
            println(sc.parallelize(1 to 10).reduce(_ + _))
            System.exit(0)
          }
        }

7. Run the Test1.scala file:

        ~/SparkTest$ sbt/sbt run
