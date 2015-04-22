package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.annotation.RDDScoped

/**
 *
 */
private[spark] object RDDScope {

  /**
   *
   */
  val SCOPE_NESTING_DELIMITER = ";"
  val SCOPE_NAME_DELIMITER = "_"

  /**
   *
   */
  private val classesWithScopeMethods = Set(
    "org.apache.spark.SparkContext",
    "org.apache.spark.rdd.RDD",
    "org.apache.spark.rdd.PairRDDFunctions",
    "org.apache.spark.rdd.AsyncRDDActions"
  )

  /**
   *
   */
  private val scopeIdCounter = new AtomicInteger(0)


  /**
   *
   */
  private def makeScopeId(name: String): String = {
    name.replace(SCOPE_NESTING_DELIMITER, "-").replace(SCOPE_NAME_DELIMITER, "-") +
      SCOPE_NAME_DELIMITER + scopeIdCounter.getAndIncrement
  }

  /**
   *
   */
  private[spark] def getScope: Option[String] = {
    val rddScopeNames = Thread.currentThread.getStackTrace
      // Avoid reflecting on all classes in the stack trace
      .filter { ste => classesWithScopeMethods.contains(ste.getClassName) }
      // Return the corresponding method if it has the @RDDScoped annotation
      .flatMap { ste =>
    // Note that this is an approximation since we match the method only by name
    // Unfortunate we cannot be more precise because the stack trace does not
    // include parameter information
      Class.forName(ste.getClassName).getDeclaredMethods.find { m =>
        m.getName == ste.getMethodName &&
          m.getDeclaredAnnotations.exists { a =>
            a.annotationType() == classOf[RDDScoped]
          }
      }
    }
      // Use the method name as the scope name for now
      .map { m => m.getName }

    // It is common for such methods to internally invoke other methods with the same name
    // (e.g. union, reduceByKey). Here we remove adjacent duplicates such that the scope
    // chain does not capture this (e.g. a, a, b, c, b, c, c => a, b, c, b, c). This is
    // surprisingly difficult to express even in Scala.
    var prev: String = null
    val dedupedRddScopeNames = rddScopeNames.flatMap { n =>
      if (n != prev) {
        prev = n
        Some(n)
      } else {
        None
      }
    }

    // Chain scope IDs to denote hierarchy, with outermost scope first
    val rddScopeIds = dedupedRddScopeNames.map(makeScopeId)
    if (rddScopeIds.nonEmpty) {
      Some(rddScopeIds.reverse.mkString(SCOPE_NESTING_DELIMITER))
    } else {
      None
    }
  }

}
