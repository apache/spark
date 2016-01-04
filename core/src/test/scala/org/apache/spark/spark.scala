// Provide the LocalSparkContext and SharedSparkContext in org.apache.spark so the old tests work.
package org.apache
package object spark {
  val LocalSparkContext = org.apache.spark.util.testing.LocalSparkContext
  type LocalSparkContext = org.apache.spark.util.testing.LocalSparkContext
  type SharedSparkContext = org.apache.spark.util.testing.SharedSparkContext
  val SPARK_VERSION = "2.0.0-SNAPSHOT"
}
