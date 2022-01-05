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

package org.apache.spark.starshuffle;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.network.util.LimitedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class read/write shuffle file on external storage like S3.
 */
public class StarS3ShuffleFileManager implements StarShuffleFileManager {
    private static final Logger logger = LoggerFactory.getLogger(StarS3ShuffleFileManager.class);

    // TODO make following values configurable
    public final static int S3_PUT_TIMEOUT_MILLISEC = 180 * 1000;

    // Following constants are copied from:
    // https://github.com/apache/hadoop/blob/6c6d1b64d4a7cd5288fcded78043acaf23228f96/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/Constants.java
    public static final long DEFAULT_MULTIPART_SIZE = 67108864; // 64M
    public static final long DEFAULT_MIN_MULTIPART_THRESHOLD = 134217728; // 128M
    public static final String MAX_THREADS = "fs.s3a.threads.max";
    public static final int DEFAULT_MAX_THREADS = 10;
    public static final String KEEPALIVE_TIME = "fs.s3a.threads.keepalivetime";
    public static final int DEFAULT_KEEPALIVE_TIME = 60;

    public static final String AWS_REGION = "fs.s3a.endpoint.region";
    public static final String DEFAULT_AWS_REGION = Regions.US_WEST_2.getName();

    private static TransferManager transferManager;
    private static final Object transferManagerLock = new Object();

    private final String awsRegion;
    private final int maxThreads;
    private final long keepAliveTime;

    public StarS3ShuffleFileManager(SparkConf conf) {
        Configuration hadoopConf = SparkHadoopUtil.get().newConfiguration(conf);

        awsRegion = hadoopConf.get(AWS_REGION, DEFAULT_AWS_REGION);

        int threads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
        if (threads < 2) {
            logger.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
            threads = 2;
        }
        maxThreads = threads;

        keepAliveTime = conf.getLong(KEEPALIVE_TIME, DEFAULT_KEEPALIVE_TIME);
    }

    @Override
    public String createFile(String root) {
        if (!root.endsWith("/")) {
            root = root + "/";
        }
        String fileName = String.format("shuffle-%s.data", UUID.randomUUID());
        return root + fileName;
    }

    @Override
    public void write(InputStream data, long size, String file) {
        logger.info("Writing to shuffle file: {}", file);
        writeS3(data, size, file);
    }

    @Override
    public InputStream read(String file, long offset, long size) {
        logger.info("Opening shuffle file: {}, offset: {}, size: {}", file, offset, size);
        return readS3(file, offset, size);
    }

    private void writeS3(InputStream inputStream, long size, String s3Url) {
        logger.info("Uploading shuffle file to s3: {}, size: {}", s3Url, size);

        S3BucketAndKey bucketAndKey = S3BucketAndKey.getFromUrl(s3Url);
        String bucket = bucketAndKey.getBucket();
        String key = bucketAndKey.getKey();

        TransferManager transferManager = getTransferManager();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/octet-stream");
        metadata.setContentLength(size);

        PutObjectRequest request = new PutObjectRequest(bucket,
                key,
                inputStream,
                metadata);

        AtomicLong totalTransferredBytes = new AtomicLong(0);

        request.setGeneralProgressListener(new ProgressListener() {
            private long lastLogTime = 0;

            @Override
            public void progressChanged(ProgressEvent progressEvent) {
                long count = progressEvent.getBytesTransferred();
                long total = totalTransferredBytes.addAndGet(count);
                long currentTime = System.currentTimeMillis();
                long logInterval = 10000;
                if (currentTime - lastLogTime >= logInterval) {
                    logger.info("S3 upload progress: {}, recent transferred {} bytes, total transferred {}", key, count, total);
                    lastLogTime = currentTime;
                }
            }
        });

        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/best-practices.html
        request.getRequestClientOptions().setReadLimit((int) DEFAULT_MULTIPART_SIZE + 1);
        request.setSdkRequestTimeout(S3_PUT_TIMEOUT_MILLISEC);
        request.setSdkClientExecutionTimeout(S3_PUT_TIMEOUT_MILLISEC);
        try {
            long startTime = System.currentTimeMillis();
            transferManager.upload(request).waitForCompletion();
            long duration = System.currentTimeMillis() - startTime;
            double mbs = 0;
            if (duration != 0) {
                mbs = ((double) size) / (1000 * 1000) / ((double) duration / 1000);
            }
            logger.info("S3 upload finished: {}, file size: {} bytes, total transferred: {}, throughput: {} mbs",
                    s3Url, size, totalTransferredBytes.get(), mbs);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to upload to s3: " + key, e);
        }
    }

    private InputStream readS3(String s3Url, long offset, long size) {
        logger.info("Downloading shuffle file from s3: {}, size: {}", s3Url, size);

        S3BucketAndKey bucketAndKey = S3BucketAndKey.getFromUrl(s3Url);

        File downloadTempFile;
        try {
            downloadTempFile = File.createTempFile("shuffle-download", ".data");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp file for downloading shuffle file");
        }

        TransferManager transferManager = getTransferManager();

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketAndKey.getBucket(), bucketAndKey.getKey())
                .withRange(offset, offset + size);

        AtomicLong totalTransferredBytes = new AtomicLong(0);

        getObjectRequest.setGeneralProgressListener(new ProgressListener() {
            private long lastLogTime = 0;

            @Override
            public void progressChanged(ProgressEvent progressEvent) {
                long count = progressEvent.getBytesTransferred();
                long total = totalTransferredBytes.addAndGet(count);
                long currentTime = System.currentTimeMillis();
                long logInterval = 10000;
                if (currentTime - lastLogTime >= logInterval) {
                    logger.info("S3 download progress: {}, recent transferred {} bytes, total transferred {}", s3Url, count, total);
                    lastLogTime = currentTime;
                }
            }
        });

        try {
            long startTime = System.currentTimeMillis();
            transferManager.download(getObjectRequest, downloadTempFile).waitForCompletion();
            long duration = System.currentTimeMillis() - startTime;
            double mbs = 0;
            if (duration != 0) {
                mbs = ((double) size) / (1000 * 1000) / ((double) duration / 1000);
            }
            logger.info("S3 download finished: {}, file size: {} bytes, total transferred: {}, throughput: {} mbs",
                    s3Url, size, totalTransferredBytes.get(), mbs);
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format(
                    "Failed to download shuffle file %s", s3Url));
        } finally {
            // TODO
            transferManager.shutdownNow();
        }

        // TODO delete downloadTempFile

        try {
            return new LimitedInputStream(new FileInputStream(downloadTempFile), size);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format(
                    "Failed to open downloaded shuffle file %s (from %s)", downloadTempFile, s3Url));
        }
    }

    private TransferManager getTransferManager() {
        synchronized (transferManagerLock) {
            if (transferManager != null) {
                return transferManager;
            }
            transferManager = createTransferManager(awsRegion, maxThreads, keepAliveTime);
            return transferManager;
        }
    }

    private static TransferManager createTransferManager(String region, int maxThreads, long keepAliveTime) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(S3_PUT_TIMEOUT_MILLISEC);
        clientConfiguration.setRequestTimeout(S3_PUT_TIMEOUT_MILLISEC);
        clientConfiguration.setSocketTimeout(S3_PUT_TIMEOUT_MILLISEC);
        clientConfiguration.setClientExecutionTimeout(S3_PUT_TIMEOUT_MILLISEC);

        ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("s3-shuffle-transfer-manager-worker-" + this.threadCount++);
                return thread;
            }
        };
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                maxThreads, Integer.MAX_VALUE,
                keepAliveTime, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory);
        ExecutorFactory executorFactory = new ExecutorFactory() {
            @Override
            public ExecutorService newExecutor() {
                return threadPoolExecutor;
            }
        };

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withClientConfiguration(clientConfiguration)
                .build();

        return TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .withMinimumUploadPartSize(DEFAULT_MULTIPART_SIZE)
                .withMultipartUploadThreshold(DEFAULT_MIN_MULTIPART_THRESHOLD)
                .withMultipartCopyPartSize(DEFAULT_MULTIPART_SIZE)
                .withMultipartCopyThreshold(DEFAULT_MIN_MULTIPART_THRESHOLD)
                .withExecutorFactory(executorFactory)
                .build();
    }

    public static void shutdownTransferManager() {
        synchronized (transferManagerLock) {
            if (transferManager == null) {
                return;
            }
            transferManager.shutdownNow(true);
            transferManager = null;
        }
    }

    public static class S3BucketAndKey {
        private String bucket;
        private String key;

        public static S3BucketAndKey getFromUrl(String s3Url) {
            URI url = URI.create(s3Url);
            String bucket = url.getHost();
            String key = url.getPath();
            if (key.startsWith("/")) {
                key = key.substring(1);
            }
            if (key.isEmpty()) {
                throw new RuntimeException(String.format(
                        "Could not get object key in s3 url: %s", s3Url));
            }
            return new S3BucketAndKey(bucket, key);
        }

        public S3BucketAndKey(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }

        public String getBucket() {
            return bucket;
        }

        public String getKey() {
            return key;
        }
    }
}
