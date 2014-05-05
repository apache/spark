package org.apache.spark.api.python

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext

private[spark] object PythonUtils {
  private val pathSeparator = System.getProperty("path.separator")

  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for(sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python").mkString(File.separator)
      pythonPath += Seq(sparkHome, "python", "lib", "py4j-0.8.1-src.zip").mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    pythonPath.mkString(pathSeparator)
  }

  /** Merge PYTHONPATHS with the appropriate separator. Ignores blank strings. */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(pathSeparator)
  }
}
