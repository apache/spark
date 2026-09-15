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
    onCredentialsUpdate: (Long, Array[Byte]) => Unit,
    credentialProviderLoader: CredentialProviderLoader)
  extends Logging {

  // Auxiliary constructor used only by tests, which construct the manager directly with a
  // default CredentialProviderLoader. Production code goes through
  // UserCredentialManager.create(...) (which passes the loader from the selection phase),
  // so it does not use this constructor.
  def this(
      sparkConf: SparkConf,
      tokenIngestor: TokenIngestor,
      onCredentialsUpdate: (Long, Array[Byte]) => Unit) = {
    this(sparkConf, tokenIngestor, onCredentialsUpdate, new CredentialProviderLoader())
  }

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

    val (credentials, earliestExpiry) = resolveCredentials(ctx, applyProperties = true)
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

    // Provider-declared additional Spark properties (CredentialProvider
    // .additionalSparkProperties()) are primarily applied earlier, during the selection phase
    // (UserCredentialManager.applyProviderProperties), which runs before SparkContext
    // materializes the driver's Hadoop Configuration -- that is what makes them effective on
    // the driver. As a fallback, resolveCredentials(applyProperties = true) above also applies
    // them here for any provider that actually resolved a credential (idempotent under
    // !sparkConf.contains(key)), covering the case where selection could not apply them.
    // Renewals do not re-apply them (they are static wiring, not rotating credentials).

    (version, serialized)
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
      try {
        if (!renewalExecutor.awaitTermination(
            UserCredentialManager.RENEWAL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          logWarning(log"Timed out waiting for credential renewal to stop.")
        }
      } catch {
        case e: InterruptedException =>
          logWarning(log"Interrupted while waiting for credential renewal to stop.", e)
          Thread.currentThread().interrupt()
      }
    }
    // Note: the CredentialProviderLoader is NOT closed here. It is owned and closed by
    // SparkContext (which created it in the selection phase and shares this same instance),
    // so that there is a single owner of closeAll(). Closing it here as well would risk a
    // renewal task (if still shutting down) racing against an already-closed loader.
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

      val (credentials, earliestExpiry) = resolveCredentials(ctx, applyProperties = false)
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
   * @param applyProperties If true (the initial acquisition from [[start]]), re-apply each
   *   resolved provider's [[CredentialProvider#additionalSparkProperties]] into `sparkConf`,
   *   guarded by `!sparkConf.contains(key)` so user-set values and values already applied by the
   *   selection phase are preserved. This is a fallback for the case where the selection phase
   *   could not apply them (e.g. it failed for that scheme): a provider that actually resolves a
   *   credential here is definitely "used", so its wiring should be in place. It is idempotent
   *   with the selection phase. Renewal passes false: the properties are static wiring and are
   *   not re-applied on renewals.
   * @return Tuple of (UserCredentials, earliest expiry across all service credentials)
   */
  private def resolveCredentials(
      ctx: UserContext,
      applyProperties: Boolean): (UserCredentials, Option[Instant]) = {
    val schemes = discoverSchemes()

    val credentialMap = new mutable.HashMap[String, ServiceCredential]()
    var earliestExpiry: Option[Instant] = None

    for (scheme <- schemes) {
      try {
        val providerOpt = credentialProviderLoader.providerFor(scheme, credentialConfMap)
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

            // Fallback wiring for a provider that actually resolved a credential. Idempotent
            // with the selection phase (guarded by !contains); only on initial acquisition.
            // Isolated in its own catch so that a throwing additionalSparkProperties() does not
            // discard the credential we just resolved (which is already in credentialMap with
            // its expiry accounted for above) or get misreported as a resolution failure.
            if (applyProperties) {
              try {
                applyResolvedProviderProperties(scheme, provider)
              } catch {
                case NonFatal(e) =>
                  logWarning(log"Resolved a credential for scheme ${MDC(LogKeys.URI, scheme)} " +
                    log"but failed to apply its declared Spark properties; the credential is " +
                    log"still used, but its provider wiring may be missing.", e)
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
   * Apply a resolved provider's declared Spark properties into `sparkConf`, only for keys not
   * already set (by the user or by the earlier selection phase). Note: on the driver these no
   * longer reach the already-materialized Hadoop `Configuration`; the selection phase is what
   * makes them effective on the driver. This is a best-effort fallback so that a provider that
   * resolves here still contributes its executor-facing wiring even if selection could not.
   */
  private def applyResolvedProviderProperties(
      scheme: String, provider: CredentialProvider): Unit = {
    UserCredentialManager.applyDeclaredProperties(
      sparkConf, provider, scheme, "after successful resolution")
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
    // Explicitly configured scheme names, from keys of the form
    // spark.security.oidc.provider.<scheme>(.<subkey>). Shared with the selection phase.
    val explicitSchemes = UserCredentialManager.explicitSchemesFrom(credentialConfMap)

    if (explicitSchemes.nonEmpty) {
      explicitSchemes
    } else {
      // No explicit scheme configuration. Discover all schemes that have a provider
      // available on the classpath by probing CredentialProviderLoader.
      // This covers both built-in providers (e.g., connector/credential-aws for "s3a")
      // and third-party providers registered via ServiceLoader.
      credentialProviderLoader.discoverAllSchemes().asScala.toSet
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

private[spark] object UserCredentialManager extends Logging {

  private val RENEWAL_SHUTDOWN_TIMEOUT_SECONDS = 10L

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
   * @param loader The [[CredentialProviderLoader]] from the selection phase
   *               ([[applyProviderProperties]]), passed as an `Option`. When OIDC is enabled it
   *               must be `Some` (the selection phase, which runs earlier and is skipped only
   *               when OIDC is disabled or in local mode, produced it); reusing that same
   *               instance ensures providers are discovered and initialized exactly once, so the
   *               resolution phase reuses the already-selected providers. It is an error for the
   *               loader to be `None` while OIDC is enabled and a resolution phase is expected.
   * @return Some(manager) if enabled, None otherwise
   */
  def create(
      sparkConf: SparkConf,
      onCredentialsUpdate: (Long, Array[Byte]) => Unit,
      loader: Option[CredentialProviderLoader]): Option[UserCredentialManager] = {
    if (!sparkConf.get(SECURITY_OIDC_ENABLED)) {
      None
    } else {
      // Enforce the invariant explicitly rather than silently allocating a fresh loader (which
      // SparkContext would not own and therefore never close, leaking provider resources).
      val selectionLoader = loader.getOrElse {
        throw new IllegalStateException(
          "OIDC credential propagation is enabled but no CredentialProviderLoader was produced " +
            "by the selection phase. This indicates the selection phase " +
            "(UserCredentialManager.applyProviderProperties) did not run before the resolution " +
            "phase, which should not happen outside local mode.")
      }
      val tokenFile = sparkConf.get(SECURITY_OIDC_IDENTITY_TOKEN_FILE).getOrElse {
        throw new IllegalArgumentException(
          s"${SECURITY_OIDC_IDENTITY_TOKEN_FILE.key} must be set when " +
            s"${SECURITY_OIDC_ENABLED.key} is true")
      }

      val tokenIngestor = new FileTokenIngestor(Paths.get(tokenFile))
      Some(new UserCredentialManager(
        sparkConf, tokenIngestor, onCredentialsUpdate, selectionLoader))
    }
  }

  /**
   * Selection phase of OIDC credential propagation, run early during `SparkContext`
   * initialization (before the driver's Hadoop `Configuration` is materialized).
   *
   * When OIDC credential propagation is enabled, this discovers the `CredentialProvider` for
   * each unambiguously-resolvable scheme (without initializing it) and applies the provider's
   * [[CredentialProvider#additionalSparkProperties]] declarations into `sparkConf` (only for
   * keys the user has not already set). This is the driver-side counterpart to the executor
   * path (where these properties travel via `SparkAppConfig` before the executor's environment
   * is built): applying them into `sparkConf` here -- before `SparkContext` materializes the
   * driver's Hadoop `Configuration` and other config-derived components -- lets driver-side
   * access (e.g. output-path existence checks, the commit protocol) use the propagated
   * credentials rather than falling back to the default credential chain.
   *
   * This phase performs NO credential resolution and NO network I/O: it only reads the static
   * `additionalSparkProperties()` declaration, obtained via
   * [[CredentialProviderLoader#selectProviderForProperties]] which selects the provider
   * WITHOUT calling `init()`. Actual provider initialization and credential acquisition (and
   * renewal) happen later in [[start]] on the scheduler backend. This separation of provider
   * SELECTION from credential RESOLUTION is intentional.
   *
   * The phase is skipped entirely when `isLocal` is true: `LocalSchedulerBackend` does not start
   * a [[UserCredentialManager]], so no resolution phase follows and no credentials are ever
   * populated. Wiring a provider class into the driver's Hadoop `Configuration` in that case
   * would make driver-side access fail (the provider would find no credentials) instead of
   * falling back to the default chain. (Running credential resolution in local mode -- for
   * parity with `HadoopDelegationTokenManager`, which does run in `LocalSchedulerBackend` -- is
   * left to a follow-up.)
   *
   * Scheme selection is limited to schemes for which a provider is UNAMBIGUOUSLY selected:
   * either an explicitly-configured scheme (`spark.security.oidc.provider.<scheme>`) or a
   * scheme with exactly one candidate provider on the classpath. Schemes with multiple
   * candidates and no explicit configuration are skipped here (there is no basis to choose
   * which provider's properties to apply); they are left to [[start]], where `providerFor`
   * raises a clear error prompting explicit configuration.
   *
   * @param sparkConf The Spark configuration to apply properties into. Not modified when OIDC
   *                  credential propagation is disabled or when `isLocal` is true.
   * @param isLocal Whether the application runs in local mode (no scheduler backend that starts
   *                a resolution phase).
   * @return `Some(loader)` with the [[CredentialProviderLoader]] used, to be passed to
   *         [[create]] so the resolution phase reuses the same loader; `None` when OIDC is
   *         disabled or when `isLocal` is true (no loader is allocated in those cases).
   */
  def applyProviderProperties(
      sparkConf: SparkConf,
      isLocal: Boolean): Option[CredentialProviderLoader] = {
    if (!sparkConf.get(SECURITY_OIDC_ENABLED) || isLocal) {
      return None
    }

    val loader = new CredentialProviderLoader()
    val confMap = sparkConf.getAll
      .filter { case (k, _) => k.startsWith("spark.security.oidc.") }
      .toMap.asJava

    // Determine candidate schemes: explicitly-configured schemes take precedence; otherwise
    // fall back to all schemes discoverable on the classpath (single-candidate schemes are the
    // intended zero-config case).
    val explicitSchemes = explicitSchemesFrom(confMap)
    val schemes =
      if (explicitSchemes.nonEmpty) explicitSchemes
      else loader.discoverAllSchemes().asScala.toSet

    for (scheme <- schemes) {
      try {
        val providerOpt = loader.selectProviderForProperties(scheme, confMap)
        if (providerOpt.isPresent) {
          val provider = providerOpt.get()
          val props = provider.additionalSparkProperties()
          if (props != null && !props.isEmpty) {
            applyDeclaredProperties(sparkConf, provider, scheme, "selection phase")
            // The wiring is in place, but the credentials it points at are not populated until
            // the resolution phase runs at scheduler-backend start. Any driver-side access to
            // this scheme that happens earlier in SparkContext construction (e.g. fetching
            // spark.jars / spark.files on this scheme) will fail to resolve credentials until
            // then. Log this so the eventual failure (including a ClassNotFoundException on
            // executors when the provider class is not on their classpath) is easy to trace.
            logInfo(log"Driver-side access to scheme ${MDC(LogKeys.URI, scheme)} will use OIDC " +
              log"credentials once they are acquired at scheduler start; access before that " +
              log"point (e.g. spark.jars/spark.files on this scheme) cannot be authenticated yet.")
          }
        }
      } catch {
        case NonFatal(e) =>
          // Ambiguous selection (multiple candidates, no explicit config) or a misbehaving
          // provider. We cannot apply this provider's properties, and -- unlike before -- the
          // resolution phase does not re-derive them from selection, so warn rather than hide
          // this at DEBUG. The resolution phase (start()) still applies properties for any
          // provider whose resolve() succeeds (idempotent under !contains), so a transient
          // failure here does not necessarily mean the wiring is lost for the whole application;
          // an ambiguous-scheme error, however, will resurface there as a clear failure.
          logWarning(log"Could not apply provider-declared properties for scheme " +
            log"${MDC(LogKeys.URI, scheme)} during the selection phase; if this scheme is " +
            log"actually used, credential resolution will report the underlying error.", e)
      }
    }
    Some(loader)
  }

  /**
   * Apply a provider's declared additional Spark properties
   * ([[CredentialProvider#additionalSparkProperties]]) into `sparkConf`, only for keys the user
   * (or an earlier phase) has not already set. Shared by the selection phase
   * ([[applyProviderProperties]]) and the initial-acquisition fallback in [[start]]
   * ([[applyResolvedProviderProperties]]) so the two apply properties identically. The `reason`
   * is included in the per-key INFO log to distinguish the two call sites.
   */
  private def applyDeclaredProperties(
      sparkConf: SparkConf,
      provider: CredentialProvider,
      scheme: String,
      reason: String): Unit = {
    val props = provider.additionalSparkProperties()
    if (props != null && !props.isEmpty) {
      props.forEach { (key, value) =>
        if (!sparkConf.contains(key)) {
          sparkConf.set(key, value)
          logInfo(log"Set ${MDC(LogKeys.CONFIG, key)} from " +
            log"${MDC(LogKeys.CLASS_NAME, provider.getClass.getName)} " +
            log"(scheme: ${MDC(LogKeys.URI, scheme)}, ${MDC(LogKeys.REASON, reason)}).")
        }
      }
    }
  }

  /**
   * Extracts explicitly-configured scheme names from OIDC configuration, i.e. the {@code
   * <scheme>} segment of keys of the form {@code spark.security.oidc.provider.<scheme>} (or
   * {@code ...<scheme>.<subkey>}). Shared by the selection phase and the instance-level
   * [[discoverSchemes]] so the two stay in sync.
   */
  private def explicitSchemesFrom(confMap: java.util.Map[String, String]): Set[String] = {
    val providerPrefix = "spark.security.oidc.provider."
    confMap.asScala
      .filter { case (k, _) => k.startsWith(providerPrefix) }
      .map { case (k, _) => k.stripPrefix(providerPrefix).split('.').head }
      .toSet
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
