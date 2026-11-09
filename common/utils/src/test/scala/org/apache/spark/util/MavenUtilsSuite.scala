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

import java.io.{File, IOException, OutputStream, PrintStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket, SocketTimeoutException, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.FileTime
import java.util.concurrent.{CancellationException, CountDownLatch, Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.sun.net.httpserver.HttpServer
import org.apache.ivy.core.IvyContext
import org.apache.ivy.core.module.descriptor.{DependencyDescriptor, MDArtifact}
import org.apache.ivy.core.resolve.{ResolveData, ResolvedModuleRevision}
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.resolver.{AbstractResolver, ChainResolver, FileSystemResolver, IBiblioResolver}
import org.apache.ivy.util.url.{URLHandlerDispatcher, URLHandlerRegistry}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.util.MavenUtils.MavenCoordinate

class MavenUtilsSuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterEach {

  private val resolverTimeoutMs = 30 * 1000
  private var tempIvyPath: String = _

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  implicit private val printStream: PrintStream = new BufferPrintStream

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    val lineBuffer = ArrayBuffer[String]()
    // scalastyle:off println
    override def println(line: String): Unit = {
      lineBuffer += line
    }
    // scalastyle:on println
  }

  private def buildTestIvySettings(
      repo: File,
      ivyPath: String,
      resolutionStarted: Option[AtomicBoolean] = None): IvySettings = {
    val settings = new IvySettings
    MavenUtils.processIvyPathArg(settings, Some(ivyPath))
    val resolver = new IBiblioResolver {
      override def getDependency(
          descriptor: DependencyDescriptor,
          data: ResolveData): ResolvedModuleRevision = {
        resolutionStarted.foreach(_.set(true))
        super.getDependency(descriptor, data)
      }
    }
    resolver.setM2compatible(true)
    resolver.setUsepoms(true)
    resolver.setRoot(repo.toURI.toString)
    resolver.setName("test-repo")
    settings.addResolver(resolver)
    settings.setDefaultResolver(resolver.getName)
    settings
  }

  private def shutdownExecutor(executor: ExecutorService): Unit = {
    executor.shutdownNow()
    assert(executor.awaitTermination(10, TimeUnit.SECONDS),
      "Ivy resolution executor did not terminate")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempIvyPath = SparkFileUtils.createTempDir(namePrefix = "ivy").getAbsolutePath()
  }

  override def afterEach(): Unit = {
    SparkFileUtils.deleteRecursively(new File(tempIvyPath))
    super.afterEach()
  }

  test("incorrect maven coordinate throws error") {
    val coordinates = Seq("a:b: ", " :a:b", "a: :b", "a:b:", ":a:b", "a::b", "::", "a:b", "a")
    for (coordinate <- coordinates) {
      intercept[IllegalArgumentException] {
        MavenUtils.extractMavenCoordinates(coordinate)
      }
    }
  }

  test("create repo resolvers") {
    val settings = new IvySettings
    val res1 = MavenUtils.createRepoResolvers(settings.getDefaultIvyUserDir)
    // should have central and spark-packages by default
    assert(res1.getResolvers.size() === 4)
    assert(res1.getResolvers.get(0).asInstanceOf[IBiblioResolver].getName === "local-m2-cache")
    assert(res1.getResolvers.get(1).asInstanceOf[FileSystemResolver].getName === "local-ivy-cache")
    assert(res1.getResolvers.get(2).asInstanceOf[IBiblioResolver].getName === "central")
    assert(res1.getResolvers.get(3).asInstanceOf[IBiblioResolver].getName === "spark-packages")
  }

  test("create additional resolvers") {
    val repos = "a/1,b/2,c/3"
    val settings = MavenUtils.buildIvySettings(Option(repos), Some(tempIvyPath))
    val resolver = settings.getDefaultResolver.asInstanceOf[ChainResolver]
    assert(resolver.getResolvers.size() === 4)
    val expected = repos.split(",").map(r => s"$r/")
    resolver.getResolvers.toArray.map(_.asInstanceOf[AbstractResolver]).zipWithIndex.foreach {
      case (r, i) =>
        if (1 < i && i < 3) {
          assert(r.getName === s"repo-$i")
          assert(r.asInstanceOf[IBiblioResolver].getRoot === expected(i - 1))
        }
    }
  }

  test("runtime resolver configures network timeouts for default and custom Ivy settings") {
    val settingsFile = Paths.get(tempIvyPath, "timeout-ivysettings.xml")
    Files.writeString(
      settingsFile,
      s"""<ivysettings>
         |  <settings defaultResolver="custom"/>
         |  <resolvers>
         |    <ibiblio name="custom" m2compatible="true"
         |      root="${Paths.get(tempIvyPath).toUri}"/>
         |  </resolvers>
         |</ivysettings>""".stripMargin)
    val settingsToTest = Seq(
      MavenUtils.buildIvySettings(None, Some(tempIvyPath)),
      MavenUtils.loadIvySettings(settingsFile.toString, None, Some(tempIvyPath)))

    settingsToTest.foreach { settings =>
      MavenUtils.setResolverTimeouts(settings, connectTimeoutMs = 1234, readTimeoutMs = 5678)

      val resolvers = settings.getResolvers.asScala.collect {
        case resolver: AbstractResolver => resolver
      }
      assert(resolvers.nonEmpty)
      assert(resolvers.forall(_.getTimeoutConstraint.getConnectionTimeout == 1234))
      assert(resolvers.forall(_.getTimeoutConstraint.getReadTimeout == 5678))
    }
  }

  test("add dependencies works correctly") {
    val md = MavenUtils.getModuleDescriptor
    val artifacts = MavenUtils.extractMavenCoordinates("com.databricks:spark-csv_2.12:0.1," +
      "com.databricks:spark-avro_2.12:0.1")

    MavenUtils.addDependenciesToIvy(md, artifacts, "default")
    assert(md.getDependencies.length === 2)
  }

  test("excludes works correctly") {
    val md = MavenUtils.getModuleDescriptor
    val excludes = Seq("a:b", "c:d")
    excludes.foreach { e =>
      md.addExcludeRule(MavenUtils.createExclusion(e + ":*", new IvySettings, "default"))
    }
    val rules = md.getAllExcludeRules
    assert(rules.length === 2)
    val rule1 = rules(0).getId.getModuleId
    assert(rule1.getOrganisation === "a")
    assert(rule1.getName === "b")
    val rule2 = rules(1).getId.getModuleId
    assert(rule2.getOrganisation === "c")
    assert(rule2.getName === "d")
    intercept[IllegalArgumentException] {
      MavenUtils.createExclusion("e:f:g:h", new IvySettings, "default")
    }
  }

  test("ivy path works correctly") {
    val md = MavenUtils.getModuleDescriptor
    val artifacts = for (i <- 0 until 3) yield new MDArtifact(md, s"jar-$i", "jar", "jar")
    val jPaths = MavenUtils.resolveDependencyPaths(artifacts.toArray, new File(tempIvyPath))
    assert(jPaths.count(_.startsWith(tempIvyPath)) >= 3)
    val main = MavenCoordinate("my.awesome.lib", "mylib", "0.1")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      // end to end
      val jarPath = MavenUtils.resolveMavenCoordinates(
        main.toString,
        MavenUtils.buildIvySettings(Option(repo), Some(tempIvyPath)),
        transitive = true,
        isTest = true)
      assert(jarPath.forall(_.indexOf(tempIvyPath) >= 0), "should use non-default ivy path")
    }
  }

  test("runtime dependency resolver loads custom Ivy settings") {
    val main = MavenCoordinate("my.runtime.lib", "mylib", "0.1")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val settings = Paths.get(tempIvyPath, "ivysettings.xml")
      Files.writeString(
        settings,
        s"""<ivysettings>
           |  <settings defaultResolver="runtime"/>
           |  <resolvers>
           |    <ibiblio name="runtime" m2compatible="true" root="$repo"/>
           |  </resolvers>
           |</ivysettings>""".stripMargin)
      val resolver = new RuntimeDependencyResolver(
        ivySettingsPath = Some(settings.toString),
        ivyPath = Some(tempIvyPath))
      val previousUrlHandler = URLHandlerRegistry.getDefault

      val resolved = resolver.resolve(
        URI.create(s"ivy://${main.toString}"),
        resolverTimeoutMs,
        resolverTimeoutMs)

      assert(resolved.exists(_.getFileName.toString.contains("my.runtime.lib_mylib-0.1")))
      assert(resolved.forall(_.startsWith(Paths.get(tempIvyPath))))
      assert(URLHandlerRegistry.getDefault eq previousUrlHandler)
    }
  }

  test("runtime dependency resolver applies the repository policy") {
    val resolver = new RuntimeDependencyResolver(None, Some(tempIvyPath))
    val error = intercept[IllegalArgumentException] {
      resolver.resolve(
        URI.create("ivy://my.runtime.lib:mylib:0.1?repos=https://example.com/repository"),
        resolverTimeoutMs,
        resolverTimeoutMs,
        RuntimeDependencyResolver.RejectRequestedRepositories)
    }

    assert(error.getMessage.contains("do not allow repositories"))
  }

  test("cancellable Maven resolution unwraps worker failures") {
    val missingRepo = new File(tempIvyPath, "missing-repo")
    assert(missingRepo.mkdirs())

    val error = intercept[RuntimeException] {
      MavenUtils.resolveMavenCoordinatesWithCancellation(
        "my.runtime.missing:mylib:0.1",
        buildTestIvySettings(missingRepo, Paths.get(tempIvyPath, "missing-cache").toString),
        noCacheIvySettings = None,
        transitive = false,
        exclusions = Nil,
        isTest = true,
        isCancelled = () => false)
    }
    assert(error.getMessage.contains("unresolved dependency"))
    assert(error.getMessage.contains("my.runtime.missing#mylib;0.1"))
  }

  test("runtime dependency resolver honors cancellation before resolution") {
    val resolver = new RuntimeDependencyResolver(None, Some(tempIvyPath))

    intercept[CancellationException] {
      resolver.resolve(
        URI.create("ivy://my.runtime.lib:mylib:0.1"),
        resolverTimeoutMs,
        resolverTimeoutMs,
        isCancelled = () => true)
    }
  }

  test("runtime dependency resolver honors cancellation while waiting for Ivy") {
    val main = MavenCoordinate("my.runtime.waiting", "mylib", "0.1")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val executor = Executors.newFixedThreadPool(2)
      implicit val executionContext: ExecutionContext =
        ExecutionContext.fromExecutorService(executor)
      val releaseFirst = new CountDownLatch(1)
      val cancelled = new AtomicBoolean(false)
      SparkErrorUtils.tryWithSafeFinally {
        val firstEnteredIvy = new CountDownLatch(1)
        val blockingPrintStream = new PrintStream(noOpOutputStream) {
          private val blocked = new AtomicBoolean(false)
          // scalastyle:off println
          override def println(line: String): Unit = {
            if (blocked.compareAndSet(false, true)) {
              firstEnteredIvy.countDown()
              releaseFirst.await()
            }
          }
          // scalastyle:on println
        }
        val firstSettings = MavenUtils.buildIvySettings(
          Some(repo),
          Some(Paths.get(tempIvyPath, "first").toString))
        val first = Future {
          MavenUtils.resolveMavenCoordinates(
            main.toString,
            firstSettings,
            transitive = true,
            isTest = true)(blockingPrintStream)
        }

        assert(firstEnteredIvy.await(5, TimeUnit.SECONDS))
        val waitingForIvy = new CountDownLatch(1)
        val resolver = new RuntimeDependencyResolver(
          ivySettingsPath = None,
          ivyPath = Some(Paths.get(tempIvyPath, "second").toString))
        val second = Future {
          resolver.resolve(
            URI.create(s"ivy://${main.toString}"),
            resolverTimeoutMs,
            resolverTimeoutMs,
            isCancelled = () => {
              val workerWaiting = Thread.getAllStackTraces.asScala.exists { case (worker, trace) =>
                worker.getName == "spark-ivy-resolution" &&
                  worker.getState == Thread.State.TIMED_WAITING &&
                  trace.exists { frame =>
                    frame.getClassName == MavenUtils.getClass.getName &&
                      frame.getMethodName == "acquireIvyLock"
                  }
              }
              if (workerWaiting) {
                waitingForIvy.countDown()
              }
              cancelled.get()
            })
        }

        try {
          assert(waitingForIvy.await(5, TimeUnit.SECONDS))
          cancelled.set(true)
          intercept[CancellationException] {
            SparkThreadUtils.awaitResultNoSparkExceptionConversion(second, 5.seconds)
          }
        } finally {
          releaseFirst.countDown()
        }
        SparkThreadUtils.awaitResultNoSparkExceptionConversion(first, 5.seconds)
      } {
        cancelled.set(true)
        releaseFirst.countDown()
        shutdownExecutor(executor)
      }
    }
  }

  test("runtime dependency resolver cancels stalled reads using the read timeout") {
    val executor = Executors.newFixedThreadPool(2)
    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(executor)
    val server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
    server.setSoTimeout(100)
    val requestAccepted = new CountDownLatch(1)
    val releaseRequest = new CountDownLatch(1)
    val cancelled = new AtomicBoolean(false)
    val serverFuture = Future {
      val sockets = ArrayBuffer.empty[Socket]
      try {
        while (releaseRequest.getCount > 0) {
          try {
            sockets += server.accept()
            requestAccepted.countDown()
          } catch {
            case _: SocketTimeoutException =>
          }
        }
      } finally {
        sockets.foreach(_.close())
      }
    }
    SparkErrorUtils.tryWithSafeFinally {
      val settings = Paths.get(tempIvyPath, "cancellable-ivysettings.xml")
      Files.writeString(
        settings,
        s"""<ivysettings>
           |  <settings defaultResolver="blocked"/>
           |  <resolvers>
           |    <ibiblio name="blocked" m2compatible="true"
           |      root="http://127.0.0.1:${server.getLocalPort}"/>
           |  </resolvers>
           |</ivysettings>""".stripMargin)
      val resolver = new RuntimeDependencyResolver(
        ivySettingsPath = Some(settings.toString),
        ivyPath = Some(Paths.get(tempIvyPath, "cancelled").toString))
      val previousUrlHandler = URLHandlerRegistry.getDefault
      val resolution = Future {
        resolver.resolve(
          URI.create("ivy://my.runtime.active:mylib:0.1"),
          resolverTimeoutMs,
          readTimeoutMs = 250,
          isCancelled = () => cancelled.get())
      }

      assert(requestAccepted.await(5, TimeUnit.SECONDS))
      assert(URLHandlerRegistry.getDefault eq previousUrlHandler)
      cancelled.set(true)
      intercept[CancellationException] {
        SparkThreadUtils.awaitResultNoSparkExceptionConversion(resolution, 10.seconds)
      }
      assert(URLHandlerRegistry.getDefault eq previousUrlHandler)

      val next = MavenCoordinate("my.runtime.after.cancel", "mylib", "0.1")
      IvyTestUtils.withRepository(next, None, None) { repo =>
        val resolved = MavenUtils.resolveMavenCoordinates(
          next.toString,
          MavenUtils.buildIvySettings(Some(repo), Some(tempIvyPath)),
          transitive = true,
          isTest = true)
        assert(resolved.nonEmpty)
      }
    } {
      cancelled.set(true)
      releaseRequest.countDown()
      SparkErrorUtils.tryWithSafeFinally {
        SparkThreadUtils.awaitResultNoSparkExceptionConversion(serverFuture, 5.seconds)
      } {
        SparkErrorUtils.tryWithSafeFinally {
          server.close()
        } {
          shutdownExecutor(executor)
        }
      }
    }
  }

  test("runtime resolver preserves the HTTP handler when cancelling a progressing download") {
    val executor = Executors.newSingleThreadExecutor()
    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(executor)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val downloadStarted = new CountDownLatch(1)
    val releaseDownload = new CountDownLatch(1)
    val cancelled = new AtomicBoolean(false)
    val downloadSize = 1024 * 1024
    val bytesSent = new AtomicInteger(0)
    val previousUrlHandler = URLHandlerRegistry.getDefault
    val customUrlHandler = new URLHandlerDispatcher
    customUrlHandler.setDefault(previousUrlHandler)
    URLHandlerRegistry.setDefault(customUrlHandler)
    SparkErrorUtils.tryWithSafeFinally {
      server.createContext("/", exchange => {
        try {
          if (!exchange.getRequestURI.getPath.endsWith(".jar")) {
            exchange.sendResponseHeaders(404, -1)
          } else if (exchange.getRequestMethod == "HEAD") {
            exchange.getResponseHeaders.set("Content-Length", downloadSize.toString)
            exchange.sendResponseHeaders(200, -1)
          } else {
            exchange.sendResponseHeaders(200, downloadSize)
            val output = exchange.getResponseBody
            val chunk = new Array[Byte](1024)
            while (bytesSent.get() < downloadSize && releaseDownload.getCount > 0) {
              output.write(chunk)
              output.flush()
              bytesSent.addAndGet(chunk.length)
              downloadStarted.countDown()
              releaseDownload.await(25, TimeUnit.MILLISECONDS)
            }
          }
        } catch {
          case _: IOException =>
        } finally {
          exchange.close()
        }
      })
      server.start()
      val settings = Paths.get(tempIvyPath, "streaming-ivysettings.xml")
      Files.writeString(
        settings,
        s"""<ivysettings>
           |  <settings defaultResolver="streaming"/>
           |  <resolvers>
           |    <ibiblio name="streaming" m2compatible="true" usepoms="false"
           |      root="http://127.0.0.1:${server.getAddress.getPort}/"/>
           |  </resolvers>
           |</ivysettings>""".stripMargin)
      val resolver = new RuntimeDependencyResolver(
        ivySettingsPath = Some(settings.toString),
        ivyPath = Some(Paths.get(tempIvyPath, "streaming").toString))
      val resolution = Future {
        resolver.resolve(
          URI.create("ivy://my.runtime.streaming:mylib:0.1?transitive=false"),
          resolverTimeoutMs,
          resolverTimeoutMs,
          isCancelled = () => cancelled.get())
      }

      assert(downloadStarted.await(5, TimeUnit.SECONDS))
      assert(URLHandlerRegistry.getDefault eq customUrlHandler)
      cancelled.set(true)
      intercept[CancellationException] {
        SparkThreadUtils.awaitResultNoSparkExceptionConversion(resolution, 5.seconds)
      }
      assert(bytesSent.get() < downloadSize)
      assert(URLHandlerRegistry.getDefault eq customUrlHandler)
    } {
      cancelled.set(true)
      releaseDownload.countDown()
      SparkErrorUtils.tryWithSafeFinally {
        SparkErrorUtils.tryWithSafeFinally {
          server.stop(0)
        } {
          shutdownExecutor(executor)
        }
      } {
        URLHandlerRegistry.setDefault(previousUrlHandler)
      }
    }
  }

  test("runtime dependency resolver waits for an uncooperative Ivy worker") {
    val executor = Executors.newFixedThreadPool(2)
    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(executor)
    val resolverEntered = new CountDownLatch(1)
    val releaseResolver = new CountDownLatch(1)
    val resolverCompleted = new AtomicBoolean(false)
    val cancelled = new AtomicBoolean(false)
    SparkErrorUtils.tryWithSafeFinally {
      val blockingResolver = new FileSystemResolver {
        override def getDependency(
            _descriptor: DependencyDescriptor,
            _data: ResolveData): ResolvedModuleRevision = {
          resolverEntered.countDown()
          var released = false
          while (!released) {
            try {
              released = releaseResolver.await(100, TimeUnit.MILLISECONDS)
            } catch {
              case _: InterruptedException =>
            }
          }
          resolverCompleted.set(true)
          null
        }
      }
      blockingResolver.setName("blocking")
      val settings = MavenUtils.buildIvySettings(
        remoteRepos = None,
        ivyPath = Some(Paths.get(tempIvyPath, "uncooperative").toString))
      settings.addResolver(blockingResolver)
      settings.setDefaultResolver(blockingResolver.getName)

      val resolution = Future {
        MavenUtils.resolveMavenCoordinatesWithCancellation(
          "my.runtime.uncooperative:mylib:0.1",
          settings,
          noCacheIvySettings = None,
          transitive = true,
          exclusions = Nil,
          isTest = true,
          isCancelled = () => cancelled.get())
      }

      assert(resolverEntered.await(5, TimeUnit.SECONDS))
      cancelled.set(true)
      val delayedRelease = Future {
        try {
          Thread.sleep(3000)
        } finally {
          releaseResolver.countDown()
        }
      }

      intercept[CancellationException] {
        SparkThreadUtils.awaitResultNoSparkExceptionConversion(resolution, 10.seconds)
      }
      assert(resolverCompleted.get())
      SparkThreadUtils.awaitResultNoSparkExceptionConversion(delayedRelease, 5.seconds)
    } {
      cancelled.set(true)
      releaseResolver.countDown()
      shutdownExecutor(executor)
    }
  }

  for {
    cancellationTrigger <- Seq("caller interrupt", "interrupt flag", "cancelled predicate")
    interruptDuringJoin <- Seq(false, true)
  } {
    test("cancellable Maven resolution restores caller interruption after joining the worker " +
        s"(cancellationTrigger=$cancellationTrigger, " +
        s"interruptDuringJoin=$interruptDuringJoin)") {
      val resolverEntered = new CountDownLatch(1)
      val resolverInterrupted = new CountDownLatch(1)
      val releaseResolver = new CountDownLatch(1)
      val callerWaiting = new CountDownLatch(1)
      val releaseCaller = new CountDownLatch(1)
      val workerThread = new AtomicReference[Thread]()
      val callerFailure = new AtomicReference[Throwable]()
      val callerInterrupted = new AtomicBoolean(false)
      val blockingResolver = new FileSystemResolver {
        override def getDependency(
            _descriptor: DependencyDescriptor,
            _data: ResolveData): ResolvedModuleRevision = {
          workerThread.set(Thread.currentThread())
          resolverEntered.countDown()
          var released = false
          while (!released) {
            try {
              released = releaseResolver.await(100, TimeUnit.MILLISECONDS)
            } catch {
              case _: InterruptedException => resolverInterrupted.countDown()
            }
          }
          null
        }
      }
      blockingResolver.setName("blocking")
      val settings = MavenUtils.buildIvySettings(
        remoteRepos = None,
        ivyPath = Some(Paths.get(tempIvyPath, "caller-interrupted").toString))
      settings.addResolver(blockingResolver)
      settings.setDefaultResolver(blockingResolver.getName)
      val caller = new Thread(() => {
        try {
          MavenUtils.resolveMavenCoordinatesWithCancellation(
            "my.runtime.caller.interrupted:mylib:0.1",
            settings,
            noCacheIvySettings = None,
            transitive = false,
            exclusions = Nil,
            isTest = true,
            isCancelled = () => {
              if (resolverEntered.getCount == 0) {
                callerWaiting.countDown()
                releaseCaller.await()
                if (cancellationTrigger == "interrupt flag") {
                  Thread.currentThread().interrupt()
                }
                cancellationTrigger == "cancelled predicate"
              } else {
                false
              }
            })
        } catch {
          case error: Throwable => callerFailure.set(error)
        } finally {
          callerInterrupted.set(Thread.currentThread().isInterrupted)
        }
      }, "ivy-interrupted-caller")

      SparkErrorUtils.tryWithSafeFinally {
        caller.start()
        assert(resolverEntered.await(5, TimeUnit.SECONDS))
        assert(callerWaiting.await(5, TimeUnit.SECONDS))
        if (cancellationTrigger == "caller interrupt") {
          caller.interrupt()
        } else {
          releaseCaller.countDown()
        }
        assert(resolverInterrupted.await(5, TimeUnit.SECONDS))
        if (interruptDuringJoin) {
          caller.interrupt()
        }
        caller.join(TimeUnit.SECONDS.toMillis(1))
        assert(caller.isAlive, "Caller returned before the Ivy worker was released")
        releaseResolver.countDown()
        caller.join(TimeUnit.SECONDS.toMillis(5))

        assert(!caller.isAlive)
        assert(callerFailure.get().isInstanceOf[CancellationException])
        val shouldBeInterrupted =
          cancellationTrigger != "cancelled predicate" || interruptDuringJoin
        assert(callerInterrupted.get() == shouldBeInterrupted)
        assert(!workerThread.get().isAlive)
      } {
        releaseCaller.countDown()
        releaseResolver.countDown()
        caller.interrupt()
        SparkErrorUtils.tryWithSafeFinally {
          caller.join(TimeUnit.SECONDS.toMillis(10))
          assert(!caller.isAlive, "Ivy resolution caller did not terminate")
        } {
          Option(workerThread.get()).foreach { worker =>
            worker.join(TimeUnit.SECONDS.toMillis(10))
            assert(!worker.isAlive, "Ivy resolution worker did not terminate")
          }
        }
      }
    }
  }

  test("runtime dependency resolver preserves the local Ivy repository") {
    val main = MavenCoordinate("my.runtime.local", "mylib", "0.1")
    val localIvyHome = Paths.get(tempIvyPath, "local-ivy")
    val runtimeIvyPath = Paths.get(tempIvyPath, "runtime-ivy")
    IvyTestUtils.withRepository(
      main,
      dependencies = None,
      rootDir = Some(localIvyHome.resolve("local").toFile),
      useIvyLayout = true) { _ =>
      val resolver = new RuntimeDependencyResolver(
        ivySettingsPath = None,
        ivyPath = Some(runtimeIvyPath.toString),
        localIvyPath = Some(localIvyHome.toString))

      val resolved = resolver.resolve(
        URI.create(s"ivy://${main.toString}"),
        resolverTimeoutMs,
        resolverTimeoutMs)

      assert(resolved.exists(_.getFileName.toString.contains("my.runtime.local_mylib-0.1")))
      assert(resolved.forall(_.startsWith(runtimeIvyPath)))
    }
  }

  test("runtime dependency resolver replaces truncated retrieved jars") {
    val main = MavenCoordinate("my.runtime.retrieve", "mylib", "0.1")
    val localIvyHome = Paths.get(tempIvyPath, "local-ivy")
    val runtimeIvyPath = Paths.get(tempIvyPath, "runtime-ivy")
    IvyTestUtils.withRepository(
      main,
      dependencies = None,
      rootDir = Some(localIvyHome.resolve("local").toFile),
      useIvyLayout = true) { _ =>
      val resolver = new RuntimeDependencyResolver(
        ivySettingsPath = None,
        ivyPath = Some(runtimeIvyPath.toString),
        localIvyPath = Some(localIvyHome.toString))
      def resolve(): Seq[Path] = {
        resolver.resolve(
          URI.create(s"ivy://${main.toString}?transitive=false"),
          resolverTimeoutMs,
          resolverTimeoutMs)
      }

      val resolved = resolve()
      assert(resolved.size == 1)
      val jar = resolved.head
      val completeBytes = Files.readAllBytes(jar)
      val lastModified = Files.getLastModifiedTime(jar)
      Files.write(jar, completeBytes.take(completeBytes.length / 2))
      val newerTime = FileTime.fromMillis(lastModified.toMillis + TimeUnit.MINUTES.toMillis(1))
      Files.setLastModifiedTime(jar, newerTime)
      assert(Files.size(jar) < completeBytes.length)
      assert(Files.getLastModifiedTime(jar).toMillis > lastModified.toMillis)

      assert(resolve() == resolved)
      assert(Files.readAllBytes(jar).sameElements(completeBytes))
      assert(resolve() == resolved)
      assert(Files.readAllBytes(jar).sameElements(completeBytes))
    }
  }

  test("search for artifact at local repositories") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    // Local M2 repository
    IvyTestUtils.withRepository(main, Some(dep), Some(MavenUtils.m2Path)) { repo =>
      val jarPath = MavenUtils.resolveMavenCoordinates(
        main.toString,
        MavenUtils.buildIvySettings(None, Some(tempIvyPath)),
        transitive = true,
        isTest = true)
      assert(jarPath.exists(_.indexOf("mylib") >= 0), "should find artifact")
      assert(jarPath.exists(_.indexOf("mydep") >= 0), "should find dependency")
    }
    // Local Ivy Repository
    val settings = new IvySettings
    val ivyLocal = new File(settings.getDefaultIvyUserDir, "local" + File.separator)
    IvyTestUtils.withRepository(main, Some(dep), Some(ivyLocal), useIvyLayout = true) { repo =>
      val jarPath = MavenUtils.resolveMavenCoordinates(
        main.toString,
        MavenUtils.buildIvySettings(None, Some(tempIvyPath)),
        transitive = true,
        isTest = true)
      assert(jarPath.exists(_.indexOf("mylib") >= 0), "should find artifact")
      assert(jarPath.exists(_.indexOf("mydep") >= 0), "should find dependency")
    }
    // Local ivy repository with modified home
    val dummyIvyLocal = new File(tempIvyPath, "local" + File.separator)
    settings.setDefaultIvyUserDir(new File(tempIvyPath))
    IvyTestUtils.withRepository(main, Some(dep), Some(dummyIvyLocal), useIvyLayout = true,
      ivySettings = settings) { repo =>
      val jarPath = MavenUtils.resolveMavenCoordinates(
        main.toString,
        MavenUtils.buildIvySettings(None, Some(tempIvyPath)),
        transitive = true,
        isTest = true)
      assert(jarPath.exists(_.indexOf("mylib") >= 0), "should find artifact")
      assert(jarPath.forall(_.indexOf(tempIvyPath) >= 0), "should be in new ivy path")
      assert(jarPath.exists(_.indexOf("mydep") >= 0), "should find dependency")
    }
  }

  test("primary Ivy context is restored after a resolution failure") {
    val missingRepo = new File(tempIvyPath, "missing-repo")
    assert(missingRepo.mkdirs())
    val initialContext = IvyContext.getContext

    intercept[RuntimeException] {
      MavenUtils.resolveMavenCoordinates(
        "a:b:c",
        buildTestIvySettings(
          missingRepo,
          Paths.get(tempIvyPath, "missing-cache").toString),
        transitive = true,
        isTest = true)
    }
    assert(IvyContext.getContext eq initialContext)

    val valid = MavenCoordinate("my.context.after.failure", "mylib", "0.1")
    val validRepo = IvyTestUtils.createLocalRepositoryForTests(
      valid,
      dependencies = None,
      rootDir = Some(new File(tempIvyPath, "valid-repo")))
    val resolved = MavenUtils.resolveMavenCoordinates(
      valid.toString,
      buildTestIvySettings(validRepo, Paths.get(tempIvyPath, "valid-cache").toString),
      transitive = true,
      isTest = true)
    assert(resolved.nonEmpty)
    assert(IvyContext.getContext eq initialContext)
  }

  test("no-cache Ivy context is restored after a resolution failure") {
    val artifact = MavenCoordinate("my.context.no.cache", "mylib", "0.1")
    val primaryRepo = IvyTestUtils.createLocalRepositoryForTests(
      artifact,
      dependencies = None,
      rootDir = Some(new File(tempIvyPath, "primary-repo")))
    val jar = new File(
      IvyTestUtils.pathFromCoordinate(artifact, primaryRepo, "jar", useIvyLayout = false),
      IvyTestUtils.artifactName(artifact, useIvyLayout = false))
    assert(jar.delete())

    val noCacheRepo = new File(tempIvyPath, "no-cache-repo")
    assert(noCacheRepo.mkdirs())
    val fallbackStarted = new AtomicBoolean(false)
    val initialContext = IvyContext.getContext
    intercept[RuntimeException] {
      MavenUtils.resolveMavenCoordinates(
        artifact.toString,
        buildTestIvySettings(
          primaryRepo,
          Paths.get(tempIvyPath, "primary-cache").toString),
        noCacheIvySettings = Some(buildTestIvySettings(
          noCacheRepo,
          Paths.get(tempIvyPath, "no-cache").toString,
          resolutionStarted = Some(fallbackStarted))),
        transitive = true)
    }
    assert(fallbackStarted.get())
    assert(IvyContext.getContext eq initialContext)
  }

  test("neglects Spark and Spark's dependencies") {
    val coordinates = MavenUtils.IVY_DEFAULT_EXCLUDES
      .map(comp => s"org.apache.spark:spark-${comp}2.12:2.4.0")
      .mkString(",") + ",org.apache.spark:spark-core_fake:1.2.0"

    val path = MavenUtils.resolveMavenCoordinates(
      coordinates,
      MavenUtils.buildIvySettings(None, Some(tempIvyPath)),
      transitive = true,
      isTest = true)
    assert(path.isEmpty, "should return empty path")
    val main = MavenCoordinate("org.apache.spark", "spark-streaming-kafka-assembly_2.12", "1.2.0")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val files = MavenUtils.resolveMavenCoordinates(
        coordinates + "," + main.toString,
        MavenUtils.buildIvySettings(Some(repo), Some(tempIvyPath)),
        transitive = true,
        isTest = true)
      assert(files.forall(_.indexOf(main.artifactId) >= 0), "Did not return artifact")
    }
  }

  test("exclude dependencies end to end") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    IvyTestUtils.withRepository(main, Some(dep), None) { repo =>
      val files = MavenUtils.resolveMavenCoordinates(
        main.toString,
        MavenUtils.buildIvySettings(Some(repo), Some(tempIvyPath)),
        exclusions = Seq("my.great.dep:mydep"),
        transitive = true,
        isTest = true)
      assert(files.forall(_.indexOf(main.artifactId) >= 0), "Did not return artifact")
      assert(files.forall(_.indexOf("my.great.dep") < 0), "Returned excluded artifact")
    }
  }

  test("load ivy settings file") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.5"
    val dummyIvyLocal = new File(tempIvyPath, "local" + File.separator)
    val settingsText =
      s"""
         |<ivysettings>
         |  <caches defaultCacheDir="$tempIvyPath/cache"/>
         |  <settings defaultResolver="local-ivy-settings-file-test"/>
         |  <resolvers>
         |    <filesystem name="local-ivy-settings-file-test">
         |      <ivy pattern=
         |        "$dummyIvyLocal/[organisation]/[module]/[revision]/[type]s/[artifact].[ext]"/>
         |      <artifact pattern=
         |        "$dummyIvyLocal/[organisation]/[module]/[revision]/[type]s/[artifact].[ext]"/>
         |    </filesystem>
         |  </resolvers>
         |</ivysettings>
         |""".stripMargin

    val settingsFile = Paths.get(tempIvyPath, "ivysettings.xml")
    Files.write(settingsFile, settingsText.getBytes(StandardCharsets.UTF_8))
    val settings = MavenUtils.loadIvySettings(settingsFile.toString, None, Some(tempIvyPath))

    val testUtilSettings = new IvySettings
    testUtilSettings.setDefaultIvyUserDir(new File(tempIvyPath))
    IvyTestUtils.withRepository(main, Some(dep), Some(dummyIvyLocal), useIvyLayout = true,
      ivySettings = testUtilSettings) { repo =>
      val jarPath = MavenUtils.resolveMavenCoordinates(main.toString, settings,
        transitive = true, isTest = true)
      assert(jarPath.exists(_.indexOf("mylib") >= 0), "should find artifact")
      assert(jarPath.forall(_.indexOf(tempIvyPath) >= 0), "should be in new ivy path")
      assert(jarPath.exists(_.indexOf("mydep") >= 0), "should find dependency")
    }
  }

  test("SPARK-10878: test resolution files cleaned after resolving artifact") {
    val main = new MavenCoordinate("my.great.lib", "mylib", "0.1")

    IvyTestUtils.withRepository(main, None, None) { repo =>
      val ivySettings = MavenUtils.buildIvySettings(Some(repo), Some(tempIvyPath))
      val jarPath = MavenUtils.resolveMavenCoordinates(
        main.toString,
        ivySettings,
        transitive = true,
        isTest = true)
      val r = """.*org.apache.spark-spark-submit-parent-.*""".r
      assert(!ivySettings.getDefaultCache.listFiles.map(_.getName)
        .exists(r.findFirstIn(_).isDefined), "resolution files should be cleaned")
    }
  }

  test("SPARK-34624: should ignore non-jar dependencies") {
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = "my.great.dep:mydep:0.1"

    IvyTestUtils.withRepository(main, Some(dep), None) { repo =>
      // IvyTestUtils.withRepository does not have an easy way for creating non-jar dependencies
      // So we let it create the jar dependency in `mylib-0.1.pom`, and then modify the pom
      // to change the type of the transitive to `pom`
      val mainPom = Paths.get(URI.create(repo)).resolve("my/great/lib/mylib/0.1/mylib-0.1.pom")
      val lines = Files.lines(mainPom).iterator.asScala
        .map(l => if (l.trim == "<artifactId>mydep</artifactId>") s"$l<type>pom</type>" else l)
        .toList
      Files.write(mainPom, lines.asJava)

      val ivySettings = MavenUtils.buildIvySettings(Some(repo), Some(tempIvyPath))
      val jarPath = MavenUtils.resolveMavenCoordinates(
        main.toString,
        ivySettings,
        transitive = true,
        isTest = true)
      assert(!jarPath.exists(_.indexOf("mydep") >= 0), "should not find pom dependency." +
        s" Resolved jars are: $jarPath")
    }
  }
}
