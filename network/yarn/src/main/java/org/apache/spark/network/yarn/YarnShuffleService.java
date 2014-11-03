
package org.apache.spark.network.yarn;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;

/**
 * External shuffle service used by Spark on Yarn.
 */
public class YarnShuffleService extends AuxiliaryService {

    private static final JobTokenSecretManager secretManager = new JobTokenSecretManager();

    public YarnShuffleService() {
        super("sparkshuffleservice");
        log("--- [ Welcome to YarnShuffleService v0.1 ] ---");
    }

    @Override
    public void initializeApplication(ApplicationInitializationContext context) {
        ApplicationId appId = context.getApplicationId();
        log("Initializing application " + appId + "!");
    }

    @Override
    public void stopApplication(ApplicationTerminationContext context) {
        ApplicationId appId = context.getApplicationId();
        log("Stopping application " + appId + "!");
    }

    @Override
    public ByteBuffer getMetaData() {
        log("Getting meta data");
        return ByteBuffer.wrap("".getBytes());
    }

    @Override
    public void initializeContainer(ContainerInitializationContext context) {
        ContainerId containerId = context.getContainerId();
        log("Initializing container " + containerId + "!");
    }

    @Override
    public void stopContainer(ContainerTerminationContext context) {
        ContainerId containerId = context.getContainerId();
        log("Stopping container " + containerId + "!");
    }

    private void log(String msg) {
        Timestamp timestamp = new Timestamp(new Date().getTime());
        System.out.println("* org.apache.spark.YarnShuffleService " + timestamp + ": " + msg);
    }
}
