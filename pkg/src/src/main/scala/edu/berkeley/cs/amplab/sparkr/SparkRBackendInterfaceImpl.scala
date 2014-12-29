package edu.berkeley.cs.amplab.sparkr

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import org.apache.spark.{SparkEnv, Partition, SparkException, TaskContext, SparkContext}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD, JavaPairRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class SparkRBackendInterfaceImpl extends SparkRBackendInterface {

  var sc: Option[SparkContext] = None
  val rddMap = new HashMap[Int, RDD[_]]

  override def createSparkContext(master: String, appName: String, sparkHome: String,
    jars: Array[String], sparkEnvirMap: Map[String, String],
    sparkExecutorEnvMap: Map[String, String]): String = {

    // TODO: create a version of createSparkContext that works with Map[String, String] 
    sc = Some(RRDD.createSparkContext(master, appName, sparkHome, jars,
      sparkEnvirMap.asInstanceOf[Map[Object, Object]],
      sparkExecutorEnvMap.asInstanceOf[Map[Object, Object]]))
    
    // TODO: can't get applicationId  in Spark 1.1 ? 
    sc.get.hashCode.toString
  }

  //
  //override def createRRDD(): Int = {
  //}
}
