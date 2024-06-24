/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.{File, IOException, PrintStream}
import java.net.URI
import java.text.ParseException
import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor.{Artifact, DefaultDependencyDescriptor, DefaultExcludeRule, DefaultModuleDescriptor, ExcludeRule}
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.{DownloadStatus, ResolveReport}
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{ChainResolver, FileSystemResolver, IBiblioResolver}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.ArrayImplicits._

/** Provides utility functions to be used inside SparkSubmit. */
private[spark] object MavenUtils extends Logging {
  val JAR_IVY_SETTING_PATH_KEY: String = "spark.jars.ivySettings"

  // Exposed for testing
  // var printStream = SparkSubmit.printStream

  // Exposed for testing.
  // These components are used to make the default exclusion rules for Spark dependencies.
  // We need to specify each component explicitly, otherwise we miss
  // spark-streaming utility components. Underscore is there to differentiate between
  // spark-streaming_2.1x and spark-streaming-kafka-0-10-assembly_2.1x
  val IVY_DEFAULT_EXCLUDES: Seq[String] = Seq(
    "catalyst_",
    "core_",
    "graphx_",
    "kvstore_",
    "launcher_",
    "mllib_",
    "mllib-local_",
    "network-common_",
    "network-shuffle_",
    "repl_",
    "sketch_",
    "sql_",
    "streaming_",
    "tags_",
    "unsafe_")

  /**
   * Represents a Maven Coordinate
   *
   * @param groupId
   *   the groupId of the coordinate
   * @param artifactId
   *   the artifactId of the coordinate
   * @param version
   *   the version of the coordinate
   */
  private[spark] case class MavenCoordinate(
      groupId: String,
      artifactId: String,
      version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
  }

  /**
   * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided in
   * the format `groupId:artifactId:version` or `groupId/artifactId:version`.
   *
   * @param coordinates
   *   Comma-delimited string of maven coordinates
   * @return
   *   Sequence of Maven coordinates
   */
  def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").map { p =>
      val splits = p.replace("/", ":").split(":")
      require(
        splits.length == 3,
        s"Provided Maven Coordinates must be in the form " +
          s"'groupId:artifactId:version'. The coordinate provided is: $p")
      require(
        splits(0) != null && splits(0).trim.nonEmpty,
        s"The groupId cannot be null or " +
          s"be whitespace. The groupId provided is: ${splits(0)}")
      require(
        splits(1) != null && splits(1).trim.nonEmpty,
        s"The artifactId cannot be null or " +
          s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(
        splits(2) != null && splits(2).trim.nonEmpty,
        s"The version cannot be null or " +
          s"be whitespace. The version provided is: ${splits(2)}")
      MavenCoordinate(splits(0), splits(1), splits(2))
    }.toImmutableArraySeq
  }

  /** Path of the local Maven cache. */
  private[util] def m2Path: File = {
    if (SparkEnvUtils.isTesting) {
      // test builds delete the maven cache, and this can cause flakiness
      new File("dummy", ".m2" + File.separator + "repository")
    } else {
      new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")
    }
  }

  /**
   * Create a ChainResolver used by Ivy to search for and resolve dependencies.
   *
   * @param defaultIvyUserDir
   *   The default user path for Ivy
   * @param useLocalM2AsCache
   *   Whether to use the local maven repo as a cache
   * @return
   *   A ChainResolver used by Ivy to search for and resolve dependencies.
   */
  private[util] def createRepoResolvers(
      defaultIvyUserDir: File,
      useLocalM2AsCache: Boolean = true): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("spark-list")

    if (useLocalM2AsCache) {
      val localM2 = new IBiblioResolver
      localM2.setM2compatible(true)
      localM2.setRoot(m2Path.toURI.toString)
      localM2.setUsepoms(true)
      localM2.setName("local-m2-cache")
      cr.add(localM2)
    }

    val localIvy = new FileSystemResolver
    val localIvyRoot = new File(defaultIvyUserDir, "local")
    localIvy.setLocal(true)
    localIvy.setRepository(new FileRepository(localIvyRoot))
    val ivyPattern = Seq(
      localIvyRoot.getAbsolutePath,
      "[organisation]",
      "[module]",
      "[revision]",
      "ivys",
      "ivy.xml").mkString(File.separator)
    localIvy.addIvyPattern(ivyPattern)
    val artifactPattern = Seq(
      localIvyRoot.getAbsolutePath,
      "[organisation]",
      "[module]",
      "[revision]",
      "[type]s",
      "[artifact](-[classifier]).[ext]").mkString(File.separator)
    localIvy.addArtifactPattern(artifactPattern)
    localIvy.setName("local-ivy-cache")
    cr.add(localIvy)

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    val defaultInternalRepo: Option[String] = sys.env.get("DEFAULT_ARTIFACT_REPOSITORY")
    br.setRoot(defaultInternalRepo.getOrElse("https://repo1.maven.org/maven2/"))
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot(
      sys.env.getOrElse("DEFAULT_ARTIFACT_REPOSITORY", "https://repos.spark-packages.org/"))
    sp.setName("spark-packages")
    cr.add(sp)
    cr
  }

  /**
   * Output a list of paths for the downloaded jars to be added to the classpath (will append to
   * jars in SparkSubmit).
   *
   * @param artifacts
   *   Sequence of dependencies that were resolved and retrieved
   * @param cacheDirectory
   *   Directory where jars are cached
   * @return
   *   List of paths for the dependencies
   */
  private[util] def resolveDependencyPaths(
      artifacts: Array[AnyRef],
      cacheDirectory: File): Seq[String] = {
    artifacts
      .map(_.asInstanceOf[Artifact])
      .filter { artifactInfo =>
        if (artifactInfo.getExt == "jar") {
          true
        } else {
          logInfo(s"Skipping non-jar dependency ${artifactInfo.getId}")
          false
        }
      }
      .map { artifactInfo =>
        val artifact = artifactInfo.getModuleRevisionId
        val extraAttrs = artifactInfo.getExtraAttributes
        val classifier = if (extraAttrs.containsKey("classifier")) {
          "-" + extraAttrs.get("classifier")
        } else {
          ""
        }
        cacheDirectory.getAbsolutePath + File.separator +
          s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}$classifier.jar"
      }.toImmutableArraySeq
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  private[util] def addDependenciesToIvy(
      md: DefaultModuleDescriptor,
      artifacts: Seq[MavenCoordinate],
      ivyConfName: String)(implicit printStream: PrintStream): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName + "(runtime)")
      // scalastyle:off println
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      // scalastyle:on println
      md.addDependency(dd)
    }
  }

  /** Add exclusion rules for dependencies already included in the spark-assembly */
  private def addExclusionRules(
      ivySettings: IvySettings,
      ivyConfName: String,
      md: DefaultModuleDescriptor): Unit = {
    // Add scala exclusion rule
    md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

    IVY_DEFAULT_EXCLUDES.foreach { comp =>
      md.addExcludeRule(
        createExclusion(s"org.apache.spark:spark-$comp*:*", ivySettings, ivyConfName))
    }
  }

  /**
   * Build Ivy Settings using options with default resolvers
   *
   * @param remoteRepos
   *   Comma-delimited string of remote repositories other than maven central
   * @param ivyPath
   *   The path to the local ivy repository
   * @param useLocalM2AsCache
   *   Whether or not use `local-m2 repo` as cache
   * @return
   *   An IvySettings object
   */
  def buildIvySettings(
      remoteRepos: Option[String],
      ivyPath: Option[String],
      useLocalM2AsCache: Boolean = true)(implicit printStream: PrintStream): IvySettings = {
    val ivySettings: IvySettings = new IvySettings
    processIvyPathArg(ivySettings, ivyPath)

    // create a pattern matcher
    ivySettings.addMatcher(new GlobPatternMatcher)
    // create the dependency resolvers
    val repoResolver = createRepoResolvers(ivySettings.getDefaultIvyUserDir, useLocalM2AsCache)
    ivySettings.addResolver(repoResolver)
    ivySettings.setDefaultResolver(repoResolver.getName)
    processRemoteRepoArg(ivySettings, remoteRepos)
    // (since 2.5) Setting the property ivy.maven.lookup.sources to false
    // disables the lookup of the sources artifact.
    // And setting the property ivy.maven.lookup.javadoc to false
    // disables the lookup of the javadoc artifact.
    ivySettings.setVariable("ivy.maven.lookup.sources", "false")
    ivySettings.setVariable("ivy.maven.lookup.javadoc", "false")
    ivySettings
  }

  /**
   * Load Ivy settings from a given filename, using supplied resolvers
   *
   * @param settingsFile
   *   Path to Ivy settings file
   * @param remoteRepos
   *   Comma-delimited string of remote repositories other than maven central
   * @param ivyPath
   *   The path to the local ivy repository
   * @return
   *   An IvySettings object
   */
  def loadIvySettings(settingsFile: String, remoteRepos: Option[String], ivyPath: Option[String])(
      implicit printStream: PrintStream): IvySettings = {
    val uri = new URI(settingsFile)
    val file = Option(uri.getScheme).getOrElse("file") match {
      case "file" => new File(uri.getPath)
      case scheme =>
        throw new IllegalArgumentException(
          s"Scheme $scheme not supported in " +
            JAR_IVY_SETTING_PATH_KEY)
    }
    require(file.exists(), s"Ivy settings file $file does not exist")
    require(file.isFile, s"Ivy settings file $file is not a normal file")
    val ivySettings: IvySettings = new IvySettings
    try {
      ivySettings.load(file)
      if (ivySettings.getDefaultIvyUserDir == null && ivySettings.getDefaultCache == null) {
        // To protect old Ivy-based systems like old Spark from Apache Ivy 2.5.2's incompatibility.
        // `processIvyPathArg` can overwrite these later.
        val alternateIvyDir = System.getProperty("ivy.home",
          System.getProperty("user.home") + File.separator + ".ivy2.5.2")
        ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
        ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
      }
    } catch {
      case e @ (_: IOException | _: ParseException) =>
        throw new SparkException(s"Failed when loading Ivy settings from $settingsFile", e)
    }
    processIvyPathArg(ivySettings, ivyPath)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /* Set ivy settings for location of cache, if option is supplied */
  private def processIvyPathArg(ivySettings: IvySettings, ivyPath: Option[String]): Unit = {
    val alternateIvyDir = ivyPath.filterNot(_.trim.isEmpty).getOrElse {
      // To protect old Ivy-based systems like old Spark from Apache Ivy 2.5.2's incompatibility.
      System.getProperty("ivy.home",
        System.getProperty("user.home") + File.separator + ".ivy2.5.2")
    }
    ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
    ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
  }

  /* Add any optional additional remote repositories */
  private def processRemoteRepoArg(ivySettings: IvySettings, remoteRepos: Option[String])(implicit
      printStream: PrintStream): Unit = {
    remoteRepos.filterNot(_.trim.isEmpty).map(_.split(",")).foreach { repositoryList =>
      val cr = new ChainResolver
      cr.setName("user-list")

      // add current default resolver, if any
      Option(ivySettings.getDefaultResolver).foreach(cr.add)

      // add additional repositories, last resolution in chain takes precedence
      repositoryList.zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        // scalastyle:off println
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
        // scalastyle:on println
      }

      ivySettings.addResolver(cr)
      ivySettings.setDefaultResolver(cr.getName)
    }
  }

  /** A nice function to use in tests as well. Values are dummy strings. */
  private[util] def getModuleDescriptor: DefaultModuleDescriptor =
    DefaultModuleDescriptor.newDefaultInstance(ModuleRevisionId
      // Include UUID in module name, so multiple clients resolving maven coordinate at the
      // same time do not modify the same resolution file concurrently.
      .newInstance("org.apache.spark", s"spark-submit-parent-${UUID.randomUUID.toString}", "1.0"))

  /**
   * Clear ivy resolution from current launch. The resolution file is usually at
   * ~/.ivy2/org.apache.spark-spark-submit-parent-$UUID-default.xml,
   * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.xml, and
   * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.properties. Since each launch
   * will have its own resolution files created, delete them after each resolution to prevent
   * accumulation of these files in the ivy cache dir.
   */
  private def clearIvyResolutionFiles(
      mdId: ModuleRevisionId,
      defaultCacheFile: File,
      ivyConfName: String): Unit = {
    val currentResolutionFiles = Seq(
      s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.properties")
    currentResolutionFiles.foreach { filename =>
      new File(defaultCacheFile, filename).delete()
    }
  }

  /**
   * Clear invalid cache files in ivy. The cache file is usually at
   * ~/.ivy2/cache/${groupId}/${artifactId}/ivy-${version}.xml,
   * ~/.ivy2/cache/${groupId}/${artifactId}/ivy-${version}.xml.original, and
   * ~/.ivy2/cache/${groupId}/${artifactId}/ivydata-${version}.properties.
   * Because when using `local-m2` repo as a cache, some invalid files were created.
   * If not deleted here, an error prompt similar to `unknown resolver local-m2-cache`
   * will be generated, making some confusion for users.
   */
  private def clearInvalidIvyCacheFiles(
      mdId: ModuleRevisionId,
      defaultCacheFile: File): Unit = {
    val cacheFiles = Seq(
      s"${mdId.getOrganisation}${File.separator}${mdId.getName}${File.separator}" +
        s"ivy-${mdId.getRevision}.xml",
      s"${mdId.getOrganisation}${File.separator}${mdId.getName}${File.separator}" +
        s"ivy-${mdId.getRevision}.xml.original",
      s"${mdId.getOrganisation}${File.separator}${mdId.getName}${File.separator}" +
        s"ivydata-${mdId.getRevision}.properties")
    cacheFiles.foreach { filename =>
      new File(defaultCacheFile, filename).delete()
    }
  }

  /**
   * Resolves any dependencies that were supplied through maven coordinates
   *
   * @param coordinates
   *   Comma-delimited string of maven coordinates
   * @param ivySettings
   *   An IvySettings containing resolvers to use
   * @param noCacheIvySettings
   *   An no-cache(local-m2-cache) IvySettings containing resolvers to use
   * @param transitive
   *   Whether resolving transitive dependencies, default is true
   * @param exclusions
   *   Exclusions to apply when resolving transitive dependencies
   * @return
   *   Seq of path to the jars of the given maven artifacts including their transitive
   *   dependencies
   */
  def resolveMavenCoordinates(
      coordinates: String,
      ivySettings: IvySettings,
      noCacheIvySettings: Option[IvySettings] = None,
      transitive: Boolean,
      exclusions: Seq[String] = Nil,
      isTest: Boolean = false)(implicit printStream: PrintStream): Seq[String] = {
    if (coordinates == null || coordinates.trim.isEmpty) {
      Nil
    } else {
      val sysOut = System.out
      // Default configuration name for ivy
      val ivyConfName = "default"
      var md: DefaultModuleDescriptor = null
      try {
        // To prevent ivy from logging to system out
        System.setOut(printStream)
        // A Module descriptor must be specified. Entries are dummy strings
        md = getModuleDescriptor
        md.setDefaultConf(ivyConfName)
        val artifacts = extractMavenCoordinates(coordinates)
        // Directories for caching downloads through ivy and storing the jars when maven coordinates
        // are supplied to spark-submit
        val packagesDirectory: File = new File(ivySettings.getDefaultIvyUserDir, "jars")
        // scalastyle:off println
        printStream.println(
          s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
        printStream.println(s"The jars for the packages stored in: $packagesDirectory")
        // scalastyle:on println

        val ivy = Ivy.newInstance(ivySettings)
        ivy.pushContext()

        // Set resolve options to download transitive dependencies as well
        val resolveOptions = new ResolveOptions
        resolveOptions.setTransitive(transitive)
        val retrieveOptions = new RetrieveOptions
        // Turn downloading and logging off for testing
        if (isTest) {
          resolveOptions.setDownload(false)
          resolveOptions.setLog(LogOptions.LOG_QUIET)
          retrieveOptions.setLog(LogOptions.LOG_QUIET)
        } else {
          resolveOptions.setDownload(true)
        }
        // retrieve all resolved dependencies
        retrieveOptions.setDestArtifactPattern(
          packagesDirectory.getAbsolutePath + File.separator +
            "[organization]_[artifact]-[revision](-[classifier]).[ext]")
        retrieveOptions.setConfs(Array(ivyConfName))

        // Add exclusion rules for Spark and Scala Library
        addExclusionRules(ivySettings, ivyConfName, md)
        // add all supplied maven artifacts as dependencies
        addDependenciesToIvy(md, artifacts, ivyConfName)
        exclusions.foreach { e =>
          md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
        }
        // resolve dependencies
        val rr: ResolveReport = ivy.resolve(md, resolveOptions)
        if (rr.hasError) {
          // SPARK-46302: When there are some corrupted jars in the local maven repo,
          // we try to continue without the cache
          val failedReports = rr.getArtifactsReports(DownloadStatus.FAILED, true)
          if (failedReports.nonEmpty && noCacheIvySettings.isDefined) {
            val failedArtifacts = failedReports.map(r => r.getArtifact)
            logInfo(s"Download failed: ${failedArtifacts.mkString("[", ", ", "]")}, " +
              s"attempt to retry while skipping local-m2-cache.")
            failedArtifacts.foreach(artifact => {
              clearInvalidIvyCacheFiles(artifact.getModuleRevisionId, ivySettings.getDefaultCache)
            })
            ivy.popContext()

            val noCacheIvy = Ivy.newInstance(noCacheIvySettings.get)
            noCacheIvy.pushContext()

            val noCacheRr = noCacheIvy.resolve(md, resolveOptions)
            if (noCacheRr.hasError) {
              throw new RuntimeException(noCacheRr.getAllProblemMessages.toString)
            }
            noCacheIvy.retrieve(noCacheRr.getModuleDescriptor.getModuleRevisionId, retrieveOptions)
            val dependencyPaths = resolveDependencyPaths(
              noCacheRr.getArtifacts.toArray, packagesDirectory)
            noCacheIvy.popContext()

            dependencyPaths
          } else {
            throw new RuntimeException(rr.getAllProblemMessages.toString)
          }
        } else {
          ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId, retrieveOptions)
          val dependencyPaths = resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
          ivy.popContext()

          dependencyPaths
        }
      } finally {
        System.setOut(sysOut)
        if (md != null) {
          clearIvyResolutionFiles(md.getModuleRevisionId, ivySettings.getDefaultCache, ivyConfName)
        }
      }
    }
  }

  private[util] def createExclusion(
      coords: String,
      ivySettings: IvySettings,
      ivyConfName: String): ExcludeRule = {
    val c = extractMavenCoordinates(coords).head
    val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
    val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
    rule.addConfiguration(ivyConfName)
    rule
  }

  private def isInvalidQueryString(tokens: Array[String]): Boolean = {
    tokens.length != 2 || StringUtils.isBlank(tokens(0)) || StringUtils.isBlank(tokens(1))
  }

  /**
   * Parse URI query string's parameter value of `transitive`, `exclude` and `repos`.
   * Other invalid parameters will be ignored.
   *
   * @param uri
   *   Ivy URI need to be downloaded.
   * @return
   *   Tuple value of parameter `transitive`, `exclude` and `repos` value.
   *
   *   1. transitive: whether to download dependency jar of Ivy URI, default value is true and
   *      this parameter value is case-insensitive. This mimics Hive's behaviour for parsing the
   *      transitive parameter. Invalid value will be treat as false. Example: Input:
   *      exclude=org.mortbay.jetty:jetty&transitive=true Output: true
   *
   *    2. exclude: comma separated exclusions to apply when resolving transitive dependencies,
   *      consists of `group:module` pairs separated by commas. Example: Input:
   *      excludeorg.mortbay.jetty:jetty,org.eclipse.jetty:jetty-http Output:
   *      [org.mortbay.jetty:jetty,org.eclipse.jetty:jetty-http]
   *
   *    3. repos: comma separated repositories to use when resolving dependencies.
   */
  def parseQueryParams(uri: URI): (Boolean, String, String) = {
    val uriQuery = uri.getQuery
    if (uriQuery == null) {
      (true, "", "")
    } else {
      val mapTokens = uriQuery.split("&").map(_.split("="))
      if (mapTokens.exists(MavenUtils.isInvalidQueryString)) {
        throw new IllegalArgumentException(
          s"Invalid query string in Ivy URI ${uri.toString}: $uriQuery")
      }
      val groupedParams = mapTokens.map(kv => (kv(0), kv(1))).groupBy(_._1)

      // Parse transitive parameters (e.g., transitive=true) in an Ivy URI, default value is true
      val transitiveParams = groupedParams.get("transitive")
      if (transitiveParams.map(_.length).getOrElse(0) > 1) {
        logWarning(
          "It's best to specify `transitive` parameter in ivy URI query only once." +
            " If there are multiple `transitive` parameter, we will select the last one")
      }
      val transitive =
        transitiveParams
          .flatMap(_.takeRight(1).map(_._2.equalsIgnoreCase("true")).headOption)
          .getOrElse(true)

      // Parse an excluded list (e.g., exclude=org.mortbay.jetty:jetty,org.eclipse.jetty:jetty-http)
      // in an Ivy URI. When download Ivy URI jar, Spark won't download transitive jar
      // in a excluded list.
      val exclusionList = groupedParams
        .get("exclude")
        .map { params =>
          params
            .map(_._2)
            .flatMap { excludeString =>
              val excludes = excludeString.split(",")
              if (excludes.map(_.split(":")).exists(MavenUtils.isInvalidQueryString)) {
                throw new IllegalArgumentException(
                  s"Invalid exclude string in Ivy URI ${uri.toString}:" +
                    " expected 'org:module,org:module,..', found " + excludeString)
              }
              excludes
            }
            .mkString(",")
        }
        .getOrElse("")

      val repos = groupedParams
        .get("repos")
        .map { params =>
          params
            .map(_._2)
            .flatMap(_.split(","))
            .mkString(",")
        }
        .getOrElse("")

      val validParams = Set("transitive", "exclude", "repos")
      val invalidParams = groupedParams.keys.filterNot(validParams.contains).toSeq
      if (invalidParams.nonEmpty) {
        logWarning(
          s"Invalid parameters `${invalidParams.sorted.mkString(",")}` found " +
            s"in Ivy URI query `$uriQuery`.")
      }

      (transitive, exclusionList, repos)
    }
  }
}
