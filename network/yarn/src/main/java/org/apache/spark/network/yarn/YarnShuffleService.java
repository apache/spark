
package org.apache.spark.network.yarn;

import java.lang.Override;
import java.nio.ByteBuffer;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.SystemPropertyConfigProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * External shuffle service used by Spark on Yarn.
 */
public class YarnShuffleService extends AuxiliaryService {
    private final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);
    private static final JobTokenSecretManager secretManager = new JobTokenSecretManager();

    private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
    private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

    public YarnShuffleService() {
        super("spark_shuffle");
        logger.info("Initializing Yarn shuffle service for Spark");
    }

    /**
     * Start the shuffle server with the given configuration.
     */
    @Override
    protected void serviceInit(Configuration conf) {
        try {
            int port = conf.getInt(
                SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
            TransportConf transportConf = new TransportConf(new SystemPropertyConfigProvider());
            RpcHandler rpcHandler = new ExternalShuffleBlockHandler();
            TransportContext transportContext = new TransportContext(transportConf, rpcHandler);
            transportContext.createServer(port);
        } catch (Exception e) {
            logger.error("Exception in starting Yarn shuffle service for Spark!", e);
        }
    }

    @Override
    public void initializeApplication(ApplicationInitializationContext context) {
        ApplicationId appId = context.getApplicationId();
        logger.debug("Initializing application " + appId + "!");
    }

    @Override
    public void stopApplication(ApplicationTerminationContext context) {
        ApplicationId appId = context.getApplicationId();
        logger.debug("Stopping application " + appId + "!");
    }

    @Override
    public ByteBuffer getMetaData() {
        logger.debug("Getting meta data");
        return ByteBuffer.allocate(0);
    }

    @Override
    public void initializeContainer(ContainerInitializationContext context) {
        ContainerId containerId = context.getContainerId();
        logger.debug("Initializing container " + containerId + "!");
    }

    @Override
    public void stopContainer(ContainerTerminationContext context) {
        ContainerId containerId = context.getContainerId();
        logger.debug("Stopping container " + containerId + "!");
    }
}
