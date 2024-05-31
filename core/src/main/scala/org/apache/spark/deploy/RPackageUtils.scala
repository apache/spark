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

package org.apache.spark.deploy

import java.io._
import java.util.jar.JarFile
import java.util.logging.Level
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.jdk.CollectionConverters._

import com.google.common.io.{ByteStreams, Files}

import org.apache.spark.api.r.RUtils
import org.apache.spark.internal.{LogEntry, Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.util.{RedirectThread, Utils}

private[deploy] object RPackageUtils extends Logging {

  /** The key in the MANIFEST.mf that we look for, in case a jar contains R code. */
  private final val hasRPackage = "Spark-HasRPackage"
  private final val hasRPackageMDC = MDC(CONFIG, hasRPackage)

  /** Base of the shell command used in order to install R packages. */
  private final val baseInstallCmd = Seq("R", "CMD", "INSTALL", "-l")

  /** R source code should exist under R/pkg in a jar. */
  private final val RJarEntries = "R/pkg"

  /** Documentation on how the R source file layout should be in the jar. */
  private[deploy] final val RJarDoc: MessageWithContext =
    log"""In order for Spark to build R packages that are parts of Spark Packages, there are a few
      |requirements. The R source code must be shipped in a jar, with additional Java/Scala
      |classes. The jar must be in the following format:
      |  1- The Manifest (META-INF/MANIFEST.mf) must contain the key-value: $hasRPackageMDC: true
      |  2- The standard R package layout must be preserved under R/pkg/ inside the jar. More
      |  information on the standard R package layout can be found in:
      |  http://cran.r-project.org/doc/contrib/Leisch-CreatingPackages.pdf
      |  An example layout is given below. After running `jar tf $$JAR_FILE | sort`:
      |
      |META-INF/MANIFEST.MF
      |R/
      |R/pkg/
      |R/pkg/DESCRIPTION
      |R/pkg/NAMESPACE
      |R/pkg/R/
      |R/pkg/R/myRcode.R
      |org/
      |org/apache/
      |...""".stripMargin

  /** Internal method for logging. We log to a printStream in tests, for debugging purposes. */
  private def print(
      msg: LogEntry,
      printStream: PrintStream,
      level: Level = Level.FINE,
      e: Throwable = null): Unit = {
    if (printStream != null) {
      // scalastyle:off println
      printStream.println(msg.message)
      // scalastyle:on println
      if (e != null) {
        e.printStackTrace(printStream)
      }
    } else {
      level match {
        case Level.INFO => logInfo(msg)
        case Level.WARNING => logWarning(msg)
        case Level.SEVERE => logError(msg, e)
        case _ => logDebug(msg)
      }
    }
  }

  /**
   * Checks the manifest of the Jar whether there is any R source code bundled with it.
   * Exposed for testing.
   */
  private[deploy] def checkManifestForR(jar: JarFile): Boolean = {
    if (jar.getManifest == null) {
      return false
    }
    val manifest = jar.getManifest.getMainAttributes
    manifest.getValue(hasRPackage) != null && manifest.getValue(hasRPackage).trim == "true"
  }

  /**
   * Runs the standard R package installation code to build the R package from source.
   * Multiple runs don't cause problems.
   */
  private def rPackageBuilder(
      dir: File,
      printStream: PrintStream,
      verbose: Boolean,
      libDir: String): Boolean = {
    // this code should be always running on the driver.
    val pathToPkg = Seq(dir, "R", "pkg").mkString(File.separator)
    val installCmd = baseInstallCmd ++ Seq(libDir, pathToPkg)
    if (verbose) {
      print(log"Building R package with the command: ${MDC(COMMAND, installCmd)}", printStream)
    }
    try {
      val builder = new ProcessBuilder(installCmd.asJava)
      builder.redirectErrorStream(true)

      // Put the SparkR package directory into R library search paths in case this R package
      // may depend on SparkR.
      val env = builder.environment()
      val rPackageDir = RUtils.sparkRPackagePath(isDriver = true)
      env.put("SPARKR_PACKAGE_DIR", rPackageDir.mkString(","))
      env.put("R_PROFILE_USER",
        Seq(rPackageDir(0), "SparkR", "profile", "general.R").mkString(File.separator))

      val process = builder.start()
      new RedirectThread(process.getInputStream, printStream, "redirect R packaging").start()
      process.waitFor() == 0
    } catch {
      case e: Throwable =>
        print(log"Failed to build R package.", printStream, Level.SEVERE, e)
        false
    }
  }

  /**
   * Extracts the files under /R in the jar to a temporary directory for building.
   */
  private def extractRFolder(jar: JarFile, printStream: PrintStream, verbose: Boolean): File = {
    val tempDir = Utils.createTempDir(null)
    val jarEntries = jar.entries()
    while (jarEntries.hasMoreElements) {
      val entry = jarEntries.nextElement()
      val entryRIndex = entry.getName.indexOf(RJarEntries)
      if (entryRIndex > -1) {
        val entryPath = entry.getName.substring(entryRIndex)
        if (entry.isDirectory) {
          val dir = new File(tempDir, entryPath)
          if (verbose) {
            print(log"Creating directory: ${MDC(PATH, dir)}", printStream)
          }
          dir.mkdirs
        } else {
          val inStream = jar.getInputStream(entry)
          val outPath = new File(tempDir, entryPath)
          Files.createParentDirs(outPath)
          val outStream = new FileOutputStream(outPath)
          if (verbose) {
            print(log"Extracting ${MDC(JAR_ENTRY, entry)} to ${MDC(PATH, outPath)}", printStream)
          }
          Utils.copyStream(inStream, outStream, closeStreams = true)
        }
      }
    }
    tempDir
  }

  /**
   * Extracts the files under /R in the jar to a temporary directory for building.
   */
  private[deploy] def checkAndBuildRPackage(
      jars: String,
      printStream: PrintStream = null,
      verbose: Boolean = false): Unit = {
    jars.split(",").foreach { jarPath =>
      val file = new File(Utils.resolveURI(jarPath))
      if (file.exists()) {
        val jar = new JarFile(file)
        Utils.tryWithSafeFinally {
          if (checkManifestForR(jar)) {
            print(log"${MDC(PATH, file)} contains R source code. Now installing package.",
              printStream, Level.INFO)
            val rSource = extractRFolder(jar, printStream, verbose)
            if (RUtils.rPackages.isEmpty) {
              RUtils.rPackages = Some(Utils.createTempDir().getAbsolutePath)
            }
            try {
              if (!rPackageBuilder(rSource, printStream, verbose, RUtils.rPackages.get)) {
                print(log"ERROR: Failed to build R package in ${MDC(PATH, file)}.", printStream)
                print(RJarDoc, printStream)
              }
            } finally {
              // clean up
              if (!rSource.delete()) {
                logWarning(log"Error deleting ${MDC(PATH, rSource.getPath())}")
              }
            }
          } else {
            if (verbose) {
              print(log"${MDC(PATH, file)} doesn't contain R source code, skipping...", printStream)
            }
          }
        } {
          jar.close()
        }
      } else {
        print(log"WARN: ${MDC(PATH, file)} resolved as dependency, but not found.",
          printStream, Level.WARNING)
      }
    }
  }

  private def listFilesRecursively(dir: File, excludePatterns: Seq[String]): Set[File] = {
    if (!dir.exists()) {
      Set.empty[File]
    } else {
      if (dir.isDirectory) {
        val subDir = dir.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = {
            !excludePatterns.map(name.contains).reduce(_ || _) // exclude files with given pattern
          }
        })
        subDir.flatMap(listFilesRecursively(_, excludePatterns)).toSet
      } else {
        Set(dir)
      }
    }
  }

  /** Zips all the R libraries built for distribution to the cluster. */
  private[deploy] def zipRLibraries(dir: File, name: String): File = {
    val filesToBundle = listFilesRecursively(dir, Seq(".zip"))
    // create a zip file from scratch, do not append to existing file.
    val zipFile = new File(dir, name)
    if (!zipFile.delete()) {
      logWarning(log"Error deleting ${MDC(PATH, zipFile.getPath())}")
    }
    val zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile, false))
    try {
      filesToBundle.foreach { file =>
        // Get the relative paths for proper naming in the ZIP file. Note that
        // we convert dir to URI to force / and then remove trailing / that show up for
        // directories because the separator should always be / for according to ZIP
        // specification and therefore `relPath` here should be, for example,
        // "/packageTest/def.R" or "/test.R".
        val relPath = file.toURI.toString.replaceFirst(dir.toURI.toString.stripSuffix("/"), "")
        val fis = new FileInputStream(file)
        val zipEntry = new ZipEntry(relPath)
        zipOutputStream.putNextEntry(zipEntry)
        ByteStreams.copy(fis, zipOutputStream)
        zipOutputStream.closeEntry()
        fis.close()
      }
    } finally {
      zipOutputStream.close()
    }
    zipFile
  }
}
