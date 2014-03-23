import com.typesafe.tools.mima.plugin.MimaKeys.{binaryIssueFilters, previousArtifact}
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import sbt._

object MimaBuild {

  def ignoredABIProblems(base: File) = {
    import com.typesafe.tools.mima.core._
    import com.typesafe.tools.mima.core.ProblemFilters._

    // Excludes placed here will be used for all Spark versions
    val defaultExcludes = Seq()

    // Read package-private excludes from file
    val excludeFilePath = (base.getAbsolutePath + "/.mima-excludes")
    val excludeFile = file(excludeFilePath) 
    val packagePrivateList: Seq[String] =
      if (!excludeFile.exists()) {
        Seq()
      } else {
        IO.read(excludeFile).split("\n")
      }

    def excludeClass(className: String) = {
      Seq(
        excludePackage(className), 
        ProblemFilters.exclude[MissingClassProblem](className),
        ProblemFilters.exclude[MissingTypesProblem](className),
        excludePackage(className + "$"), 
        ProblemFilters.exclude[MissingClassProblem](className + "$"),
        ProblemFilters.exclude[MissingTypesProblem](className + "$")
      )
    }
    def excludeSparkClass(className: String) = excludeClass("org.apache.spark." + className)

    val packagePrivateExcludes = packagePrivateList.flatMap(excludeClass)

    /* Excludes specific to a given version of Spark. When comparing the given version against
       its immediate predecessor, the excludes listed here will be applied. */
    val versionExcludes =
      SparkBuild.SPARK_VERSION match {
        case v if v.startsWith("1.0") =>
          Seq(excludePackage("org.apache.spark.api.java")) ++
           excludeSparkClass("rdd.ClassTags") ++
           excludeSparkClass("util.XORShiftRandom") ++
           excludeSparkClass("mllib.recommendation.MFDataGenerator") ++
           excludeSparkClass("mllib.optimization.SquaredGradient") ++
           excludeSparkClass("mllib.regression.RidgeRegressionWithSGD") ++
           excludeSparkClass("mllib.regression.LassoWithSGD") ++
           excludeSparkClass("mllib.regression.LinearRegressionWithSGD")
        case _ => Seq()
      }

    defaultExcludes ++ packagePrivateExcludes ++ versionExcludes
  }

  def mimaSettings(sparkHome: File) = mimaDefaultSettings ++ Seq(
    previousArtifact := None,
    binaryIssueFilters ++= ignoredABIProblems(sparkHome)
  )

}
