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

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.{AmazonClientException, AmazonServiceException, ClientConfiguration, Protocol}
import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, InstanceProfileCredentialsProvider, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing, S3ObjectSummary}

import com.google.common.base.{Preconditions, Strings}
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.AbstractSequentialIterator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobPattern, Path, PathFilter}
import org.apache.hadoop.fs.s3.S3Credentials
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapred.{FileInputFormat, FileSplit, InputSplit, JobConf}

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Contains util methods to interact with S3 from Spark.
 */
@DeveloperApi
object SparkS3Util extends Logging {
  val sparkConf = SparkEnv.get.conf
  val conf: Configuration = SparkHadoopUtil.get.newConfiguration(sparkConf)

  private val s3ClientCache: Cache[String, AmazonS3Client] = CacheBuilder
    .newBuilder
    .concurrencyLevel(Runtime.getRuntime.availableProcessors)
    .build[String, AmazonS3Client]

  // Flag to enable S3 bulk listing. It is true by default.
  private val S3_BULK_LISTING_ENABLED: String = "spark.s3.bulk.listing.enabled"

  // Properties for AmazonS3Client. Default values should just work most of time.
  private val S3_CONNECT_TIMEOUT: String = "spark.s3.connect.timeout"
  private val S3_MAX_CONNECTIONS: String = "spark.s3.max.connections"
  private val S3_MAX_ERROR_RETRIES: String = "spark.s3.max.error.retries"
  private val S3_SOCKET_TIMEOUT: String = "spark.s3.socket.timeout"
  private val S3_SSL_ENABLED: String = "spark.s3.ssl.enabled"
  private val S3_USE_INSTANCE_CREDENTIALS: String = "spark.s3.use.instance.credentials"

  // Ignore hidden files whose name starts with "_" and ".", or ends with "$folder$".
  private val hiddenFileFilter = new PathFilter() {
    override def accept(p: Path): Boolean = {
      val name: String = p.getName()
      !name.startsWith("_") && !name.startsWith(".") && !name.endsWith("$folder$")
    }
  }

  /**
   * Initialize AmazonS3Client per bucket. Since access permissions might be different from bucket
   * to bucket, it is necessary to initialize AmazonS3Client on a per bucket basis.
   */
  private def getS3Client(bucket: String): AmazonS3Client = {
    val sslEnabled: Boolean = sparkConf.getBoolean(S3_SSL_ENABLED, true)
    val maxErrorRetries: Int = sparkConf.getInt(S3_MAX_ERROR_RETRIES, 10)
    val connectTimeout: Int = sparkConf.getInt(S3_CONNECT_TIMEOUT, 5000)
    val socketTimeout: Int = sparkConf.getInt(S3_SOCKET_TIMEOUT, 5000)
    val maxConnections: Int = sparkConf.getInt(S3_MAX_CONNECTIONS, 5)
    val useInstanceCredentials: Boolean = sparkConf.getBoolean(S3_USE_INSTANCE_CREDENTIALS, false)

    val clientConf: ClientConfiguration = new ClientConfiguration
    clientConf.setMaxErrorRetry(maxErrorRetries)
    clientConf.setProtocol(if (sslEnabled) Protocol.HTTPS else Protocol.HTTP)
    clientConf.setConnectionTimeout(connectTimeout)
    clientConf.setSocketTimeout(socketTimeout)
    clientConf.setMaxConnections(maxConnections)

    // There are different ways of obtaining S3 bucket access. Try them in the following order:
    //   1) Check if user specified an IAM role. If so, use it.
    //   2) Check if instance is associated with an IAM role. If so, use it.
    //   3) Check if default IAM role is set in Hadoop conf. If so, use it.
    //   4) If no IAM role is found, search for an AWS key pair.
    // If no credentials are found, throw an exception.
    val s3RoleArn = Option(conf.get("aws.iam.role.arn"))
    val s3RoleArnDefault = Option(conf.get("aws.iam.role.arn.default"))
    val credentialsProvider: AWSCredentialsProvider =
      s3RoleArn match {
        case Some(role) =>
          logDebug("Use user-specified IAM role: " + role)
          new STSAssumeRoleSessionCredentialsProvider(
            role, "RoleSessionName-" + Utils.random.nextInt)
        case None if useInstanceCredentials =>
          logDebug("Use IAM role associated with the instance")
          new InstanceProfileCredentialsProvider
        case _ =>
          s3RoleArnDefault match {
            case Some(role) =>
              logDebug("Use default IAM role configured in Hadoop config: " + role)
              new STSAssumeRoleSessionCredentialsProvider(
                role, "RoleSessionName-" + Utils.random.nextInt)
            case _ =>
              try {
                logDebug("Use AWS key pair")
                val credentials: S3Credentials = new S3Credentials
                credentials.initialize(URI.create(bucket), conf)
                new StaticCredentialsProvider(
                  new BasicAWSCredentials(credentials.getAccessKey, credentials.getSecretAccessKey))
              } catch {
                case e: Exception => throw new RuntimeException("S3 credentials not configured", e)
              }
        }
      }

    val providerName: String = credentialsProvider.getClass.getSimpleName
    val s3ClientKey: String =
      if (s3RoleArn == null) providerName
      else providerName + "_" + s3RoleArn

    Option(s3ClientCache.getIfPresent(s3ClientKey)).getOrElse {
      val newClient = new AmazonS3Client(credentialsProvider, clientConf)
      s3ClientCache.put(s3ClientKey, newClient)
      newClient
    }
  }

  /**
   * Helper function to extract S3 object key name from the given path.
   */
  private def keyFromPath(path: Path): String = {
    Preconditions.checkArgument(path.isAbsolute, "Path is not absolute: %s", path)
    var key: String = Strings.nullToEmpty(path.toUri.getPath)
    if (key.startsWith("/")) {
      key = key.substring(1)
    }
    if (key.endsWith("/")) {
      key = key.substring(0, key.length - 1)
    }
    key
  }

  /**
   * Helper function to convert S3ObjectSummary into FileStatus.
   */
  private def statusFromObjects(
      bucket: String,
      objects: util.List[S3ObjectSummary]): Iterator[FileStatus] = {
    val blockSize: Long = getS3BlockSize()
    val list: ArrayBuffer[FileStatus] = ArrayBuffer()
    for (obj: S3ObjectSummary <- objects.asScala) {
      if (!obj.getKey.endsWith("/")) {
        val path = new Path(bucket + "/" + obj.getKey)
        if (hiddenFileFilter.accept(path)) {
          val status: FileStatus = new FileStatus(
            obj.getSize,
            false,
            1,
            blockSize,
            obj.getLastModified.getTime,
            path)
          list += status
        }
      }
    }
    list.iterator
  }

  /**
   * For the given path, list S3 objects and return an iterator of returned FileStatuses.
   */
  @throws(classOf[AmazonClientException])
  @throws(classOf[AmazonServiceException])
  private def listPrefix(s3: AmazonS3Client, path: Path): Iterator[FileStatus] = {
    val uri: URI = path.toUri
    val key: String = keyFromPath(path)
    val request: ListObjectsRequest = new ListObjectsRequest()
      .withBucketName(uri.getAuthority)
      .withPrefix(key)

    val listings = new AbstractSequentialIterator[ObjectListing](s3.listObjects(request)) {
      protected def computeNext(previous: ObjectListing): ObjectListing = {
        if (!previous.isTruncated) {
          return null
        }
        s3.listNextBatchOfObjects(previous)
      }
    }.asScala

    val bucket: String = uri.getScheme + "://" + uri.getAuthority
    listings
      .map(listing => statusFromObjects(bucket, listing.getObjectSummaries))
      .reduceLeft(_ ++ _)
  }

  /**
   * For the given list of paths, sequentially list S3 objects per path and combine returned
   * FileStatuses into a single array.
   */
  private def listStatus(s3: AmazonS3Client, paths: List[Path]): Array[FileStatus] = {
    val list: ArrayBuffer[FileStatus] = ArrayBuffer()
    paths.foreach { path =>
      val iterator = listPrefix(s3, path)
      while (iterator.hasNext) {
        list += iterator.next
      }
    }
    list.toArray
  }

  /**
   * Find S3 block size from Hadoop conf. Try both s3 and s3n names.
   */
  private def getS3BlockSize(): Long = {
    val minS3BlockSize = 10485760; // 10mb
    val defaultS3BlockSize = 67108864; // 64mb
    val value = Option(conf.get("fs.s3.block.size"))
      .getOrElse(conf.get("fs.s3n.block.size", defaultS3BlockSize.toString)).toLong
    if (value < minS3BlockSize) {
      logWarning("S3 block size is set too small: " + value + ". Overriding it to 10mb.");
      minS3BlockSize
    } else {
      value
    }
  }

  /**
   * Find min split size from Hadoop conf. Try both Hadoop 1 and 2 names.
   */
  private def getMinSplitSize(): Long = {
    val value = Option(conf.get("mapred.min.split.size"))
      .getOrElse(conf.get("mapreduce.input.fileinputformat.split.minsize", "134217728"))
    value.toLong
  }

  /**
   * Return whether the given path is an S3 path or not.
   */
  private def isS3Path(path: Path): Boolean = {
    Option(path.toUri.getScheme).exists(_.toUpperCase.startsWith("S3"))
  }

  /**
   * Return whether S3 bulk listing can be enabled or not.
   */
  def s3BulkListingEnabled(jobConf: JobConf): Boolean = {
    val enabledByUser = sparkConf.getBoolean(S3_BULK_LISTING_ENABLED, false)
    val inputPaths = FileInputFormat.getInputPaths(jobConf)
    val noWildcard = inputPaths.forall(path => !new GlobPattern(path.toString).hasWildcard)
    val s3Paths = inputPaths.forall(SparkS3Util.isS3Path(_))
    enabledByUser && inputPaths.nonEmpty && s3Paths && noWildcard
  }

  /**
   * Return whether the given file is splittable or not.
   * Exposed for testing.
   */
  def isSplitable(jobConf: JobConf, file: Path): Boolean = {
    val compressionCodecs = new CompressionCodecFactory(jobConf)
    val codec = compressionCodecs.getCodec(file)
    if (codec == null) {
      true
    } else {
      codec.isInstanceOf[SplittableCompressionCodec]
    }
  }

  /**
   * Compute input splits for the given files. Borrowed code from `FileInputFormat.getSplits`.
   * Exposed for testing.
   */
  def computeSplits(
    jobConf: JobConf,
    files: Array[FileStatus],
    minSplits: Int): Array[InputSplit] = {
    val totalSize: Long = files.map(_.getLen).foldLeft(0L)((sum, len) => sum + len)
    val goalSize: Long = totalSize / (if (minSplits == 0) 1 else minSplits)
    val minSize: Long = getMinSplitSize()
    val splits: ArrayBuffer[InputSplit] = ArrayBuffer[InputSplit]()
    // Since S3 objects are remote, use a zero-length array to fake data local hosts.
    val fakeHosts: Array[String] = Array()

    for (file <- files) {
      val path: Path = file.getPath
      val length: Long = file.getLen
      if (length > 0 && isSplitable(jobConf, path)) {
        val blockSize: Long = file.getBlockSize
        val splitSize: Long = Math.max(minSize, Math.min(goalSize, blockSize))
        var bytesRemaining: Long = length
        while (bytesRemaining.toDouble / splitSize > 1.1) {
          splits += new FileSplit(path, length - bytesRemaining, splitSize, fakeHosts)
          bytesRemaining -= splitSize
        }
        if (bytesRemaining != 0) {
          splits += new FileSplit(path, length - bytesRemaining, bytesRemaining, fakeHosts)
        }
      } else {
        splits += new FileSplit(path, 0, length, fakeHosts)
      }
    }

    logDebug("Total size of input splits is " + totalSize)
    logDebug("Num of input splits is " + splits.size)

    splits.toArray
  }

  /**
   * This is based on `FileInputFormat.getSplits` method. Two key differences are:
   *   1) Use `AmazonS3Client.listObjects` instead of `FileSystem.listStatus`.
   *   2) Bypass data locality hints since they're irrelevant to S3 objects.
   */
  def getSplits(jobConf: JobConf, minSplits: Int): Array[InputSplit] = {
    val inputPaths: Array[Path] = FileInputFormat.getInputPaths(jobConf)
    val files: Array[FileStatus] = inputPaths.toList
      .groupBy[String] { path =>
        val uri = path.toUri
        uri.getScheme + "://" + uri.getAuthority
      }
      .map { case (bucket, paths) => (bucket, listStatus(getS3Client(bucket), paths)) }
      .values.reduceLeft(_ ++ _)
    computeSplits(jobConf, files, minSplits)
  }
}
