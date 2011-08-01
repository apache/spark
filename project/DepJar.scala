import sbt._
import Keys._
import java.io.PrintWriter
import scala.collection.mutable
import scala.io.Source
import Project.Initialize

/*
 * This is based on the AssemblyPlugin. For now it was easier to copy and modify than to wait for
 * the required changes needed for us to customise it so that it does what we want. We may revisit
 * this in the future.
 */
object DepJarPlugin extends Plugin {
  val DepJar = config("dep-jar") extend(Runtime)
  val depJar = TaskKey[File]("dep-jar", "Builds a single-file jar of all dependencies.")

  val jarName           = SettingKey[String]("jar-name")
  val outputPath        = SettingKey[File]("output-path")
  val excludedFiles     = SettingKey[Seq[File] => Seq[File]]("excluded-files")
  val conflictingFiles  = SettingKey[Seq[File] => Seq[File]]("conflicting-files")

  private def assemblyTask: Initialize[Task[File]] =
    (test, packageOptions, cacheDirectory, outputPath,
        fullClasspath, excludedFiles, conflictingFiles, streams) map {
      (test, options, cacheDir, jarPath, cp, exclude, conflicting, s) =>
        IO.withTemporaryDirectory { tempDir =>
          val srcs = assemblyPaths(tempDir, cp, exclude, conflicting, s.log)
          val config = new Package.Configuration(srcs, jarPath, options)
          Package(config, cacheDir, s.log)
          jarPath
        }
    }

  private def assemblyPackageOptionsTask: Initialize[Task[Seq[PackageOption]]] =
    (packageOptions in Compile, mainClass in DepJar) map { (os, mainClass) =>
      mainClass map { s =>
        os find { o => o.isInstanceOf[Package.MainClass] } map { _ => os
        } getOrElse { Package.MainClass(s) +: os }
      } getOrElse {os}
    }

  private def assemblyExcludedFiles(base: Seq[File]): Seq[File] = {
    ((base / "scala" ** "*") +++ // exclude scala library
      (base / "spark" ** "*") +++ // exclude Spark classes
      ((base / "META-INF" ** "*") --- // generally ignore the hell out of META-INF
        (base / "META-INF" / "services" ** "*") --- // include all service providers
        (base / "META-INF" / "maven" ** "*"))).get // include all Maven POMs and such
  }

  private def assemblyPaths(tempDir: File, classpath: Classpath,
      exclude: Seq[File] => Seq[File], conflicting: Seq[File] => Seq[File], log: Logger) = {
    import sbt.classpath.ClasspathUtilities

    val (libs, directories) = classpath.map(_.data).partition(ClasspathUtilities.isArchive)
    val services = mutable.Map[String, mutable.ArrayBuffer[String]]()
    for(jar <- libs) {
      val jarName = jar.asFile.getName
      log.info("Including %s".format(jarName))
      IO.unzip(jar, tempDir)
      IO.delete(conflicting(Seq(tempDir)))
      val servicesDir = tempDir / "META-INF" / "services"
      if (servicesDir.asFile.exists) {
       for (service <- (servicesDir ** "*").get) {
         val serviceFile = service.asFile
         if (serviceFile.exists && serviceFile.isFile) {
           val entries = services.getOrElseUpdate(serviceFile.getName, new mutable.ArrayBuffer[String]())
           for (provider <- Source.fromFile(serviceFile).getLines) {
             if (!entries.contains(provider)) {
               entries += provider
             }
           }
         }
       }
     }
    }

    for ((service, providers) <- services) {
      log.debug("Merging providers for %s".format(service))
      val serviceFile = (tempDir / "META-INF" / "services" / service).asFile
      val writer = new PrintWriter(serviceFile)
      for (provider <- providers.map { _.trim }.filter { !_.isEmpty }) {
        log.debug("-  %s".format(provider))
        writer.println(provider)
      }
      writer.close()
    }

    val base = tempDir +: directories
    val descendants = ((base ** (-DirectoryFilter)) --- exclude(base)).get
    descendants x relativeTo(base)
  }

  lazy val depJarSettings = inConfig(DepJar)(Seq(
    depJar <<= packageBin.identity,
    packageBin <<= assemblyTask,
    jarName <<= (name, version) { (name, version) => name + "-dep-" + version + ".jar" },
    outputPath <<= (target, jarName) { (t, s) => t / s },
    test <<= (test in Test).identity,
    mainClass <<= (mainClass in Runtime).identity,
    fullClasspath <<= (fullClasspath in Runtime).identity,
    packageOptions <<= assemblyPackageOptionsTask,
    excludedFiles := assemblyExcludedFiles _,
    conflictingFiles := assemblyExcludedFiles _
  )) ++
  Seq(
    depJar <<= (depJar in DepJar).identity
  )
}