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

package org.apache.spark.deploy.security

import java.io.{ByteArrayOutputStream, ObjectInputFilter, ObjectInputStream, ObjectOutputStream}
import java.net.URI
import java.nio.file.Paths
import java.time.Instant
import java.util.concurrent.{RejectedExecutionException, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.security._
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Manager for OIDC-based credential propagation on the driver.
 *
 * This class is a sibling of [[HadoopDelegationTokenManager]] and handles the OIDC credential
 * propagation path independently. Both managers run on independent threads and can both be
 * active simultaneously (e.g., a cluster accessing HDFS via Kerberos and S3 via OIDC).
 *
 * Responsibilities:
 *   1. Reads the current identity token via a [[TokenIngestor]]
 *   2. Calls [[CredentialProvider#resolve]] for each configured scheme
 *   3. Serializes the resulting [[UserCredentials]] and invokes the propagation callback
 *   4. Schedules renewal based on `min(identity token expiry, service credential expiry) -
 *      safetyMargin`
 *   5. Retries with exponential backoff on failure
 *
 * Intended to be started from `CoarseGrainedSchedulerBackend.start()` when
 * `spark.security.oidc.enabled=true`, independently of
 * `UserGroupInformation.isSecurityEnabled()`.
 *
 * Lifecycle: call `start()` exactly once, then `stop()` to shut down.
 * Calling `start()` after `stop()` is not supported.
 */
private[spark] class UserCredentialManager(
    sparkConf: SparkConf,
    tokenIngestor: TokenIngestor,
    onCredentialsUpdate: (Long, Array[Byte]) => Unit) extends Logging {

  private val safetyMargin = sparkConf.get(SECURITY_OIDC_RENEWAL_SAFETY_MARGIN)
  private val minInterval = sparkConf.get(SECURITY_OIDC_RENEWAL_MIN_INTERVAL)

  // Monotonically increasing version counter for credential updates.
  // Incremented on each successful credential acquisition (initial + renewals).
  // Used by executors to guard against stale TaskDescription credentials overwriting
  // fresher credentials delivered via RPC broadcast.
  private val credentialVersion = new AtomicLong(0)

  // Counter for exponential backoff calculation.
  // Only accessed from the single-thread renewal executor.
  private var consecutiveFailures: Int = 0
  private val maxBackoffMs: Long = UserCredentialManager.MAX_BACKOFF_MS

  private var renewalExecutor: ScheduledExecutorService = _

  // Snapshot of credential-related configuration at construction time.
  private val credentialConfMap: java.util.Map[String, String] = sparkConf.getAll
    .filter(_._1.startsWith("spark.security.oidc."))
    .toMap.asJava

  /**
   * Start the credential manager. Acquires initial credentials and schedules renewal.
   *
   * The initial credential acquisition is fail-fast: if the token cannot be loaded or
   * no credentials can be resolved, an exception is thrown. Subsequent renewal failures
   * are handled with exponential backoff.
   *
   * @return The version and serialized initial [[UserCredentials]].
   * @throws IllegalStateException if the initial credential acquisition fails.
   */
  def start(): (Long, Array[Byte]) = {
    require(renewalExecutor == null, "start() must not be called more than once")

    // Initial acquisition is fail-fast (no retry/backoff).
    // This ensures the application fails to start if credentials cannot be obtained,
    // rather than running with null credentials.
    val userContext = tokenIngestor.load()
    if (!userContext.isPresent) {
      throw new IllegalStateException(
        "Failed to start UserCredentialManager: identity token file is missing or malformed. " +
          s"Check ${SECURITY_OIDC_IDENTITY_TOKEN_FILE.key} configuration.")
    }

    val ctx = userContext.get()
    logInfo(log"Initial identity token loaded for principal " +
      log"${MDC(LogKeys.PRINCIPAL, ctx.getPrincipal)} " +
      log"(issuer: ${MDC(LogKeys.URI, ctx.getIssuer)})")

    val (credentials, earliestExpiry) = resolveCredentials(ctx)
    val serialized = UserCredentialManager.serializeUserCredentials(credentials)
    val version = credentialVersion.incrementAndGet()

    // Propagate initial credentials
    onCredentialsUpdate(version, serialized)

    // Create the renewal executor only after successful initial acquisition.
    // This avoids leaking a daemon thread if the fail-fast path throws, and
    // keeps the require() guard valid for retry scenarios.
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("user-credential-renewal")

    // Schedule first renewal
    val renewalDelay = computeRenewalDelay(ctx, earliestExpiry)
    scheduleRenewal(renewalDelay)

    logInfo(log"Credential acquisition successful. Next renewal in " +
      log"${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(renewalDelay))}.")
    (version, serialized)
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
    }
    // Close all initialized credential providers to release resources (e.g., HTTP clients).
    // CredentialProviderLoader.closeAll() operates on global static state, which is safe
    // because Spark enforces a single SparkContext (and thus a single UserCredentialManager)
    // per JVM.
    try {
      CredentialProviderLoader.closeAll()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        logWarning(log"Interrupted while closing credential providers during shutdown.", e)
      case NonFatal(e) =>
        logWarning(log"Error closing credential providers during shutdown.", e)
    }
  }

  /**
   * Scheduled renewal task: load identity token, resolve credentials, propagate,
   * and schedule the next renewal. Failures are retried with exponential backoff.
   */
  private def renewCredentialsTask(): Unit = {
    try {
      val userContext = tokenIngestor.load()
      if (!userContext.isPresent) {
        throw new IllegalStateException(
          "TokenIngestor returned empty - identity token file may be missing or malformed")
      }

      val ctx = userContext.get()
      logInfo(log"Loaded identity token for principal " +
        log"${MDC(LogKeys.PRINCIPAL, ctx.getPrincipal)} " +
        log"(issuer: ${MDC(LogKeys.URI, ctx.getIssuer)})")

      val (credentials, earliestExpiry) = resolveCredentials(ctx)
      val serialized = UserCredentialManager.serializeUserCredentials(credentials)
      val version = credentialVersion.incrementAndGet()

      // Propagate credentials to executors. Errors here are logged separately
      // so that credential-fetch success is not conflated with distribution failure.
      try {
        onCredentialsUpdate(version, serialized)
      } catch {
        case e: Exception =>
          logWarning(log"Credentials were resolved successfully but failed to propagate " +
            log"to executors. Executors will receive updated credentials on next renewal.", e)
      }

      // Reset backoff on successful credential resolution. This is intentionally done
      // even if onCredentialsUpdate failed above: the backoff counter tracks credential
      // *resolution* failures (STS/provider issues), not propagation failures.
      // Propagation failures are transient and will be retried on next scheduled renewal.
      consecutiveFailures = 0

      // Schedule next renewal
      val renewalDelay = computeRenewalDelay(ctx, earliestExpiry)
      scheduleRenewal(renewalDelay)

      logInfo(log"Credential renewal successful. Next renewal in " +
        log"${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(renewalDelay))}.")
    } catch {
      case _: InterruptedException =>
        // Shutting down, ignore
      case NonFatal(e) =>
        consecutiveFailures += 1
        val failures = consecutiveFailures
        val delay = computeBackoffDelay()
        logWarning(log"Failed to renew user credentials (attempt " +
          log"${MDC(LogKeys.NUM_RETRY, failures)}), " +
          log"will retry in ${MDC(LogKeys.TIME_UNITS, UIUtils.formatDuration(delay))}.", e)
        scheduleRenewal(delay)
      case t: Throwable =>
        // A fatal error would otherwise be swallowed by the executor, silently stopping
        // all future renewals. Log it before propagating.
        logError(log"Fatal error during credential renewal; no further renewals will be " +
          log"scheduled.", t)
        throw t
    }
  }

  /**
   * Resolve credentials for all schemes that have registered providers.
   *
   * @return Tuple of (UserCredentials, earliest expiry across all service credentials)
   */
  private def resolveCredentials(
      ctx: UserContext): (UserCredentials, Option[Instant]) = {
    val schemes = discoverSchemes()

    val credentialMap = new mutable.HashMap[String, ServiceCredential]()
    var earliestExpiry: Option[Instant] = None

    for (scheme <- schemes) {
      try {
        val providerOpt = CredentialProviderLoader.providerFor(scheme, credentialConfMap)
        if (providerOpt.isPresent) {
          val provider = providerOpt.get()
          // Use a synthetic target URI with just the scheme for initial resolution.
          // The full URI is passed in future versions when path-specific resolution is needed.
          // The "synthetic" authority signals this is not a real endpoint but a placeholder
          // for scheme-based provider selection.
          val target = new URI(scheme, UserCredentialManager.SYNTHETIC_TARGET_AUTHORITY,
            "/", null, null)
          val credential = provider.resolve(ctx, target)
          if (credential == null) {
            logWarning(log"Provider for scheme ${MDC(LogKeys.URI, scheme)} " +
              log"returned null; skipping.")
          } else {
            credentialMap.put(scheme, credential)

            val expiry = credential.getExpiresAt
            if (expiry != null) {
              earliestExpiry = earliestExpiry match {
                case Some(existing) if existing.isBefore(expiry) => Some(existing)
                case _ => Some(expiry)
              }
            }
          }
        } else {
          logWarning(log"No credential provider found for scheme " +
            log"${MDC(LogKeys.URI, scheme)}. Skipping.")
        }
      } catch {
        case e: Exception =>
          logWarning(log"Failed to resolve credentials for scheme " +
            log"${MDC(LogKeys.URI, scheme)}. Skipping this provider.", e)
      }
    }

    if (credentialMap.isEmpty) {
      throw new IllegalStateException(
        "No credential providers resolved any credentials. " +
          "Check that providers are on the classpath and configured correctly.")
    }

    (new UserCredentials(credentialMap.asJava), earliestExpiry)
  }

  /**
   * Discover all schemes that have at least one registered provider.
   *
   * Schemes are determined by explicit configuration keys of the form
   * `spark.security.oidc.provider.<scheme>`. If no explicit configuration
   * exists, the method queries `CredentialProviderLoader` to discover all providers
   * registered via ServiceLoader and collects their supported schemes.
   *
   * Note: When using auto-discovery (no explicit config), multiple providers may
   * support the same scheme. In that case, `CredentialProviderLoader.providerFor`
   * will throw an `IllegalArgumentException` for that scheme. The caller
   * (`resolveCredentials`) handles this gracefully via per-provider exception catching,
   * logging a warning and continuing with remaining schemes.
   */
  private def discoverSchemes(): Set[String] = {
    // Extract explicitly configured scheme names.
    // Keys are of the form spark.security.oidc.provider.<scheme> or
    // spark.security.oidc.provider.<scheme>.<subkey>. We extract only the first
    // segment after the prefix to get the scheme name.
    val providerPrefix = "spark.security.oidc.provider."
    val explicitSchemes = credentialConfMap.asScala
      .filter { case (k, _) => k.startsWith(providerPrefix) }
      .map { case (k, _) => k.stripPrefix(providerPrefix).split('.').head }
      .toSet

    if (explicitSchemes.nonEmpty) {
      explicitSchemes
    } else {
      // No explicit scheme configuration. Discover all schemes that have a provider
      // available on the classpath by probing CredentialProviderLoader.
      // This covers both built-in providers (e.g., connector/credential-aws for "s3a")
      // and third-party providers registered via ServiceLoader.
      CredentialProviderLoader.discoverAllSchemes().asScala.toSet
    }
  }

  /**
   * Compute the delay until next renewal.
   * Uses min(identity token expiry, service credential expiry) - safetyMargin,
   * bounded below by minInterval.
   */
  private[security] def computeRenewalDelay(
      ctx: UserContext,
      earliestCredentialExpiry: Option[Instant]): Long = {
    val now = System.currentTimeMillis()

    // Consider identity token expiry
    val tokenExpiry: Option[Long] = Option(ctx.getExpiresAt).map(_.toEpochMilli)

    // Consider earliest service credential expiry
    val credExpiry: Option[Long] = earliestCredentialExpiry.map(_.toEpochMilli)

    // Take the minimum of both
    val effectiveExpiry: Option[Long] = (tokenExpiry, credExpiry) match {
      case (Some(t), Some(c)) => Some(math.min(t, c))
      case (Some(t), None) => Some(t)
      case (None, Some(c)) => Some(c)
      case (None, None) => None
    }

    effectiveExpiry match {
      case Some(expiry) =>
        math.max(expiry - now - safetyMargin, minInterval)
      case None =>
        // No expiry information available from either the identity token or service
        // credentials. Use half of the default suggested TTL (15 min / 2 = 7.5 min,
        // rounded down) as a conservative polling interval. This ensures credentials
        // are refreshed even when providers don't report expiration times.
        UserCredentialManager.DEFAULT_RENEWAL_INTERVAL_NO_EXPIRY_MS
    }
  }

  /**
   * Compute backoff delay using exponential backoff with jitter.
   * Bounded by minInterval (floor) and maxBackoffMs (ceiling).
   */
  private[security] def computeBackoffDelay(): Long = {
    // Guard against negative or zero shift amounts. consecutiveFailures should always
    // be >= 1 when this is called (incremented before calling), but we protect against
    // edge cases defensively.
    val shiftAmount = math.max(0, math.min(consecutiveFailures - 1, 6))
    val baseDelay = minInterval * (1L << shiftAmount)
    val cappedDelay = math.min(baseDelay, maxBackoffMs)
    // Add 10% jitter to avoid thundering herd on recovery
    val jitter = (cappedDelay * 0.1 * math.random()).toLong
    math.max(cappedDelay + jitter, minInterval)
  }

  private def scheduleRenewal(delay: Long): Unit = {
    try {
      val renewalTask = new Runnable {
        override def run(): Unit = {
          renewCredentialsTask()
        }
      }
      renewalExecutor.schedule(renewalTask, delay, TimeUnit.MILLISECONDS)
    } catch {
      case _: RejectedExecutionException =>
        // Executor has been shut down (e.g., stop() called concurrently). This is expected
        // during application shutdown -- no further renewals will be scheduled.
        logDebug(log"Renewal scheduling rejected - executor is shut down.")
    }
  }

}

private[spark] object UserCredentialManager {

  /**
   * Synthetic authority used in target URIs for scheme-based provider resolution.
   * Providers should not rely on this value; it signals that no specific endpoint
   * is targeted and only the URI scheme is meaningful.
   */
  private val SYNTHETIC_TARGET_AUTHORITY = "synthetic"

  /**
   * Maximum backoff delay between credential renewal retry attempts.
   * Caps the exponential backoff to prevent excessively long gaps between retries.
   */
  private val MAX_BACKOFF_MS: Long = TimeUnit.MINUTES.toMillis(10)

  /**
   * Default renewal interval when neither the identity token nor service credentials
   * report an expiration time. Set to 7 minutes (half of the default 15-minute
   * suggested TTL from CredentialProvider.suggestedTtl()).
   */
  private val DEFAULT_RENEWAL_INTERVAL_NO_EXPIRY_MS: Long = TimeUnit.MINUTES.toMillis(7)

  /**
   * ObjectInputFilter pattern restricting deserialization to only the classes needed
   * for UserCredentials. This prevents deserialization gadget chain attacks while
   * allowing Java's built-in collection internal classes and arrays that HashMap uses.
   */
  private val DESERIALIZATION_FILTER: String =
    "org.apache.spark.security.**;" +
    "java.util.**;" +
    "java.time.**;" +
    "java.lang.**;" +
    "maxdepth=10;" +
    "maxarray=1000;" +
    "maxrefs=1000;" +
    "!*"

  /**
   * Create a UserCredentialManager if OIDC credential propagation is enabled.
   *
   * @param sparkConf The Spark configuration
   * @param onCredentialsUpdate Callback to propagate credentials to executors
   * @return Some(manager) if enabled, None otherwise
   */
  def create(
      sparkConf: SparkConf,
      onCredentialsUpdate: (Long, Array[Byte]) => Unit): Option[UserCredentialManager] = {
    if (!sparkConf.get(SECURITY_OIDC_ENABLED)) {
      None
    } else {
      val tokenFile = sparkConf.get(SECURITY_OIDC_IDENTITY_TOKEN_FILE).getOrElse {
        throw new IllegalArgumentException(
          s"${SECURITY_OIDC_IDENTITY_TOKEN_FILE.key} must be set when " +
            s"${SECURITY_OIDC_ENABLED.key} is true")
      }

      val tokenIngestor = new FileTokenIngestor(Paths.get(tokenFile))
      Some(new UserCredentialManager(sparkConf, tokenIngestor, onCredentialsUpdate))
    }
  }

  /**
   * Serialize [[UserCredentials]] to a byte array using Java serialization.
   */
  private[security] def serializeUserCredentials(credentials: UserCredentials): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    Utils.tryWithResource(new ObjectOutputStream(bos)) { oos =>
      oos.writeObject(credentials)
      oos.flush()
    }
    bos.toByteArray
  }

  /**
   * Deserialize [[UserCredentials]] from a byte array.
   *
   * Uses an [[ObjectInputFilter]] to restrict deserialized classes to only those
   * required for [[UserCredentials]], preventing deserialization attacks.
   */
  def deserializeUserCredentials(bytes: Array[Byte]): UserCredentials = {
    val bis = new java.io.ByteArrayInputStream(bytes)
    Utils.tryWithResource(new ObjectInputStream(bis)) { ois =>
      ois.setObjectInputFilter(
        ObjectInputFilter.Config.createFilter(DESERIALIZATION_FILTER))
      ois.readObject().asInstanceOf[UserCredentials]
    }
  }
}
