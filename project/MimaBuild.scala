import com.typesafe.tools.mima.plugin.MimaKeys.{binaryIssueFilters, previousArtifact}
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings

object MimaBuild {

  val ignoredABIProblems = {
    import com.typesafe.tools.mima.core._
    import com.typesafe.tools.mima.core.ProblemFilters._
    /**
     * A: Detections are semi private or likely to become semi private at some point.
     */
    Seq(exclude[MissingClassProblem]("org.apache.spark.util.XORShiftRandom"),
      exclude[MissingClassProblem]("org.apache.spark.util.XORShiftRandom$"),
      exclude[MissingMethodProblem]("org.apache.spark.util.Utils.cloneWritables"),
      // Scheduler is not considered a public API.
      excludePackage("org.apache.spark.deploy"),
      // Was made private in 1.0
      excludePackage("org.apache.spark.util.collection.ExternalAppendOnlyMap#DiskMapIterator"),
      excludePackage("org.apache.spark.util.collection.ExternalAppendOnlyMap#ExternalIterator"),
      exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaPairRDD.cogroupResultToJava"),
      exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaPairRDD.groupByResultToJava"),
      exclude[IncompatibleMethTypeProblem]("org.apache.spark.scheduler.TaskSchedulerImpl.handleFailedTask"),
      exclude[MissingMethodProblem]("org.apache.spark.scheduler.TaskSchedulerImpl.taskSetTaskIds"),
      exclude[IncompatibleMethTypeProblem]("org.apache.spark.scheduler.TaskSetManager.handleFailedTask"),
      exclude[MissingMethodProblem]("org.apache.spark.scheduler.TaskSetManager.removeAllRunningTasks"),
      exclude[MissingMethodProblem]("org.apache.spark.scheduler.TaskSetManager.runningTasks_="),
      exclude[MissingMethodProblem]("org.apache.spark.scheduler.DAGScheduler.lastFetchFailureTime"),
      exclude[MissingMethodProblem]("org.apache.spark.scheduler.DAGScheduler.lastFetchFailureTime_="),
      exclude[MissingMethodProblem]("org.apache.spark.storage.BlockObjectWriter.bytesWritten")) ++
    /**
     * B: Detections are mostly false +ve.
     */
    Seq(exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.setGenerator"),
      exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.mapPartitions"),
      exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.mapPartitions"),
      exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.mapPartitions"),
      exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.foreachPartition"),
      exclude[MissingMethodProblem]("org.apache.spark.api.python.PythonRDD.writeToStream")) ++
    /**
     * Detections I am unsure about. Should be either moved to B (false +ve) or A.
     */
    Seq(exclude[MissingClassProblem]("org.apache.spark.mllib.recommendation.MFDataGenerator$"),
      exclude[MissingClassProblem]("org.apache.spark.rdd.ClassTags"),
      exclude[MissingClassProblem]("org.apache.spark.rdd.ClassTags$"),
      exclude[MissingMethodProblem]("org.apache.spark.util.collection.ExternalAppendOnlyMap.org$apache$spark$util$collection$ExternalAppendOnlyMap$$wrapForCompression$1"),
      exclude[MissingMethodProblem]("org.apache.spark.util.collection.ExternalAppendOnlyMap.org$apache$spark$util$collection$ExternalAppendOnlyMap$$sparkConf"),
      exclude[MissingClassProblem]("org.apache.spark.mllib.recommendation.MFDataGenerator"),
      exclude[MissingClassProblem]("org.apache.spark.mllib.optimization.SquaredGradient"),
      exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.regression.LinearRegressionWithSGD.gradient"),
      exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.regression.RidgeRegressionWithSGD.gradient"),
      exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.regression.LassoWithSGD.gradient")
    )
  }

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None,
    binaryIssueFilters ++= ignoredABIProblems
  )

}
